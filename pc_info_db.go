package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	gosocketio "github.com/googollee/go-socket.io"
	//	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/proto"

	//	"github.com/mtfelian/golang-socketio/transport"
	//	pcounter "github.com/synerex/proto_pcounter"

	protoPC "github.com/nagata-yoshiteru/proto_pcounter"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"github.com/sirupsen/logrus"
)

// VIsualizer

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	assetDir        = flag.String("assetdir", "../client/build", "set Web client dir")
	port            = flag.Int("port", 10081, "HarmoVis Ext Provider Listening Port")
	mu              = new(sync.Mutex)
	version         = "0.02"
	assetsDir       http.FileSystem
	ioserv          *gosocketio.Server
	sxServerAddress string
	pcMu            *sync.Mutex = nil
	pcLoop          *bool       = nil
	db              *sql.DB
	db_host         = os.Getenv("MYSQL_HOST")
	db_name         = os.Getenv("MYSQL_DATABASE")
	db_user         = os.Getenv("MYSQL_USER")
	db_pswd         = os.Getenv("MYSQL_PASSWORD")
)

type PCJ struct {
	SID  uint8
	Data []*PCDJ
}

type PCDJ struct {
	Time   int64
	Height uint32
	Dir    string
}

func init() {
	// connect
	addr := fmt.Sprintf("%s:%s@(%s:3306)/%s", db_user, db_pswd, db_host, db_name)
	print("connecting to " + addr + "\n")
	var err error
	db, err = sql.Open("mysql", addr)
	if err != nil {
		print("connection error: ")
		print(err)
		log.Fatal("\n")
	}

	// ping
	err = db.Ping()
	if err != nil {
		print("ping error: ")
		print(err)
		log.Fatal("\n")
	}

	// create users table
	_, err = db.Exec(`create table if not exists users(id BIGINT unsigned NOT NULL AUTO_INCREMENT, sub CHAR(36) NOT NULL, username VARCHAR(64) NOT NULL, email VARCHAR(256) NOT NULL, primary key(id))`)
	if err != nil {
		print("create users table error: ")
		print(err)
		log.Fatal("\n")
	}

	// create data_sources table
	_, err = db.Exec(`create table if not exists data_sources(id BIGINT unsigned NOT NULL AUTO_INCREMENT, user_id BIGINT unsigned NOT NULL, type INT NOT NULL, latitude DOUBLE, longitude DOUBLE, radius DOUBLE, opt VARCHAR(1024), foreign key fk_user_id (user_id) references users(id), primary key(id))`)
	if err != nil {
		print("create data_sources table error: ")
		print(err)
		log.Fatal("\n")
	}

	// create perms table
	_, err = db.Exec(`create table if not exists perms(id BIGINT unsigned NOT NULL AUTO_INCREMENT, user_id BIGINT unsigned NOT NULL, data_source_id BIGINT unsigned NOT NULL, granularity_time INT, granularity_mesh INT, opt VARCHAR(1024), foreign key fk_user_id (user_id) references users(id), foreign key fk_data_source_id (data_source_id) references data_sources(id), primary key(id))`)
	if err != nil {
		print("create perms table error: ")
		print(err)
		log.Fatal("\n")
	}
}

func supplyPCINFDBCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	mu.Lock()
	if sp.SupplyName == "PCounterMulti" {
		log.Printf("Broadcast PCS (%4d bytes)", len(sp.Cdata.Entity))
		ioserv.BroadcastToRoom("/", "#", "pcs", sp.Cdata.Entity)
	} else if sp.SupplyName == "PCounter" {
		pc := &protoPC.PCounter{}
		err := proto.Unmarshal(sp.Cdata.Entity, pc)
		if err == nil {
			sliceHostname := strings.Split(pc.Hostname, "-vc3d-")
			sid, _ := strconv.Atoi(sliceHostname[0])
			pcj := &PCJ{
				SID: uint8(sid),
			}
			for _, v := range pc.Data {
				if v.Typ == "counter" && v.Id == "1" {
					pcj.Data = append(pcj.Data, &PCDJ{
						Time:   v.Ts.AsTime().UnixNano() / 1000000,
						Height: v.Height,
						Dir:    v.Dir,
					})
				}
			}
			if len(pcj.Data) > 0 {
				jsonBytes, _ := json.Marshal(pcj)
				//log.Printf("Broadcast PC (%4d bytes) %+v", len(sp.Cdata.Entity), pcj)
				ioserv.BroadcastToRoom("/", "#", "pc", jsonBytes)
			} else {
				// log.Printf("Broadcast PC (%4d bytes) [Skipped] %+v", len(sp.Cdata.Entity), pcj)
			}
		} else {
			log.Printf("Unmarshaling err PC: %v", err)
		}
	} else {
		log.Printf("Broadcast Unknown (%4d bytes)", len(sp.Cdata.Entity))
		ioserv.BroadcastToRoom("/", "#", "unk_pc", sp.Cdata.Entity)
	}
	mu.Unlock()
}

func monitorStatus() {
	for {
		sxutil.SetNodeStatus(int32(runtime.NumGoroutine()), "PCINFDB")
		time.Sleep(time.Second * 3)
	}
}

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.InfoLevel)

	channelTypes := []uint32{pbase.PEOPLE_COUNTER_SVC}
	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "PCINFDB", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}
	log.Printf("Connecting SynerexServer at [%s]\n", sxServerAddress)

	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress) // if there is server address change, we should do it!

	argJSON := fmt.Sprintf("{Client:PCountInfoDB}")

	pcClient := sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, argJSON)
	pcMu, pcLoop = sxutil.SimpleSubscribeSupply(pcClient, supplyPCINFDBCallback)

	wg.Add(1)

	log.Printf("Starting PCounter Information DB Provider %s  on port %d", version, *port)

	wg.Wait()

}
