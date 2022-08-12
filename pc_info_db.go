package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	protoPF "github.com/UCLabNU/proto_pflow"
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
	pfMu            *sync.Mutex = nil
	pfLoop          *bool       = nil
	db              *sql.DB
	db_host         = os.Getenv("MYSQL_HOST")
	db_name         = os.Getenv("MYSQL_DATABASE")
	db_user         = os.Getenv("MYSQL_USER")
	db_pswd         = os.Getenv("MYSQL_PASSWORD")
)

type PFJ struct {
	ID         uint32
	FromSID    uint32
	FromTime   int64
	FromHeight uint32
	ToSID      uint32
	ToTime     int64
	ToHeight   uint32
}

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
		print("\n")
	}

	// ping
	err = db.Ping()
	if err != nil {
		print("ping error: ")
		print(err)
		print("\n")
	}

	// create table
	_, err = db.Exec(`create table if not exists pf(id BIGINT unsigned not null auto_increment, src_time DATETIME(3) not null, src_sid INT unsigned not null, dst_time DATETIME(3) not null, dst_sid INT unsigned not null, primary key(id))`)
	// select hex(mac) from log;
	// insert into pf (mac) values (x'000CF15698AD');
	if err != nil {
		print("exec error: ")
		print(err)
		print("\n")
	}
}

func parseDate(urlArr []string, start_idx int) (string, string) {
	sYear, err := strconv.Atoi(urlArr[start_idx])
	if err != nil {
		return "err", "wrong start year"
	}
	sMonth, _ := strconv.Atoi(urlArr[start_idx+1])
	if err != nil {
		return "err", "wrong start month"
	}
	sDay, _ := strconv.Atoi(urlArr[start_idx+2])
	if err != nil {
		return "err", "wrong start day"
	}
	eYear, err := strconv.Atoi(urlArr[start_idx+3])
	if err != nil {
		return "err", "wrong end year"
	}
	eMonth, _ := strconv.Atoi(urlArr[start_idx+4])
	if err != nil {
		return "err", "wrong end month"
	}
	eDay, _ := strconv.Atoi(urlArr[start_idx+5])
	if err != nil {
		return "err", "wrong end day"
	}
	stime := fmt.Sprintf("%04d-%02d-%02d 00:00:00.000", sYear, sMonth, sDay)
	etime := fmt.Sprintf("%04d-%02d-%02d 23:59:59.999", eYear, eMonth, eDay)
	return stime, etime
}

// apiHandler for PCounter/PFlow/WT Data
func apiHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}

	urlPath := r.URL.Path
	urlArr := strings.Split(urlPath, "/")

	switch urlArr[2] {
	case "pc":
		if len(urlArr) < 10 {
			io.WriteString(w, "PC: too few arguments")
			return
		}
		sid, err := strconv.Atoi(urlArr[3])
		if err != nil {
			io.WriteString(w, "PC: wrong src sid")
			return
		}
		stime, etime := parseDate(urlArr, 4)
		if stime == "err" {
			io.WriteString(w, fmt.Sprintf("PC: %s", etime))
		}
		fmt.Println(time.Now())
		rows, err := db.Query(`select id, time, dir, height from pc where sid = ? and time BETWEEN ? and ?`, sid, stime, etime)
		if err != nil {
			print(err)
			io.WriteString(w, "PC: query error")
		}
		fmt.Println(time.Now())
		var str_build strings.Builder
		print(rows)
		for rows.Next() {
			var id int64
			var time, dir string
			var height int
			err := rows.Scan(&id, &time, &dir, &height)
			if err != nil {
				print(err)
				io.WriteString(w, "PC: scan error")
			}
			str_build.WriteString(fmt.Sprintf("%d,%s,%s,%d\n", id, time, dir, height))
			//print(fmt.Sprintf("%d: %s %s %d\n", id, time, dir, height))
		}
		fmt.Println(time.Now())
		output := str_build.String()
		io.WriteString(w, output)
		fmt.Println(time.Now())
		return
	case "pca":
		if len(urlArr) < 9 {
			io.WriteString(w, "PCA: too few arguments")
			return
		}
		stime, etime := parseDate(urlArr, 3)
		if stime == "err" {
			io.WriteString(w, fmt.Sprintf("PCA: %s", etime))
		}
		fmt.Println(time.Now())
		rows, err := db.Query(`select id, sid, time, dir, height from pc where time BETWEEN ? and ?`, stime, etime)
		if err != nil {
			print(err)
			io.WriteString(w, "PCA: query error")
		}
		fmt.Println(time.Now())
		var str_build strings.Builder
		for rows.Next() {
			var id, sid int64
			var time, dir string
			var height int
			err := rows.Scan(&id, &sid, &time, &dir, &height)
			if err != nil {
				print(err)
				io.WriteString(w, "PCA: scan error")
			}
			str_build.WriteString(fmt.Sprintf("%d,%d,%s,%s,%d\n", id, sid, time, dir, height))
			// print(fmt.Sprintf("%d[%d]: %s %s %d\n", id, sid, time, dir, height))
		}
		fmt.Println(time.Now())
		output := str_build.String()
		io.WriteString(w, output)
		fmt.Println(time.Now())
		return
	case "pf":
		io.WriteString(w, "PF API")
		return
	case "pfwt":
		if len(urlArr) < 10 {
			io.WriteString(w, "WT: too few arguments")
			return
		}
		src, err := strconv.Atoi(urlArr[3])
		if err != nil {
			io.WriteString(w, "WT: wrong src id")
			return
		}
		stime, etime := parseDate(urlArr, 4)
		if stime == "err" {
			io.WriteString(w, fmt.Sprintf("WT: %s", etime))
		}
		fmt.Println(time.Now())
		rows, err := db.Query(`select * from pfwt where src = ? and time BETWEEN ? and ?`, src, stime, etime)
		if err != nil {
			print(err)
			io.WriteString(w, "WT: query error")
		}
		fmt.Println(time.Now())
		var str_build bytes.Buffer
		var id int64
		var itime, wt_data string
		for rows.Next() {
			err := rows.Scan(&id, &itime, &src, &wt_data)
			if err != nil {
				print(err)
				io.WriteString(w, "WT: scan error")
			}
			//print(fmt.Sprintf("%d: %s %s\n", id, time, wt_data))
			str_build.WriteString(wt_data)
			str_build.WriteString("\n")
		}
		fmt.Println(time.Now())
		output := str_build.String()
		io.WriteString(w, output)
		fmt.Println(time.Now())
		return
	case "pfwta":
		if len(urlArr) < 9 {
			io.WriteString(w, "WTA: too few arguments")
			return
		}
		stime, etime := parseDate(urlArr, 3)
		if stime == "err" {
			io.WriteString(w, fmt.Sprintf("WTA: %s", etime))
		}
		fmt.Println(time.Now())
		rows, err := db.Query(`select * from pfwt where time BETWEEN ? and ?`, stime, etime)
		if err != nil {
			print(err)
			io.WriteString(w, "WTA: query error")
		}
		fmt.Println(time.Now())
		var str_build bytes.Buffer
		var id, src int64
		var itime, wt_data string
		for rows.Next() {
			err := rows.Scan(&id, &itime, &src, &wt_data)
			if err != nil {
				print(err)
				io.WriteString(w, "WTA: scan error")
			}
			//print(fmt.Sprintf("%d: %s %s\n", id, time, wt_data))
			str_build.WriteString(wt_data)
			str_build.WriteString("\n")
		}
		fmt.Println(time.Now())
		output := str_build.String()
		io.WriteString(w, output)
		fmt.Println(time.Now())
		return
	default:
		io.WriteString(w, "unknown path:\n"+urlPath)
		return
	}

	io.WriteString(w, "test\n"+urlPath)
}

// assetsFileHandler for static Data
func assetsFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}

	file := r.URL.Path
	//	log.Printf("Open File '%s'",file)
	if file == "/" {
		file = "/index.html"
	}
	f, err := assetsDir.Open(file)
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		file = "/index.html"
		f, err = assetsDir.Open(file)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	} else {
		log.Printf("read[%s]\n", file)
	}
	//	log.Print(w.Header())
	if strings.HasSuffix(file, ".js") {
		w.Header().Add("Content-Type", "text/javascript")
	}
	http.ServeContent(w, r, file, fi.ModTime(), f)
}

func runSocketIOServer() *gosocketio.Server {
	server := gosocketio.NewServer(nil)
	if server == nil {
		log.Print("Socket IO open error: nil")
	}

	server.OnConnect("/", func(c gosocketio.Conn) error {
		c.SetContext("")
		// wait for a few milli seconds.
		resp := server.JoinRoom("/", "#", c)
		log.Printf("Connected ID: %s from %v with URL:%s  %t", c.ID(), c.RemoteAddr(), c.URL().Path, resp)
		return nil
	})

	server.OnDisconnect("/", func(c gosocketio.Conn, reason string) {
		resp := server.LeaveRoom("/", "#", c)
		log.Printf("Disconnected ID:%s from %v leave:%t reason:%s", c.ID(), c.RemoteAddr(), resp, reason)
		// need to reconnect?
	})

	return server
}

func supplyPCounterCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
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

func supplyPFlowCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	mu.Lock()
	if sp.SupplyName == "PFlow Supply" {
		pf := &protoPF.PFlow{}
		err := proto.Unmarshal(sp.Cdata.Entity, pf)
		if err == nil {
			pfj := &PFJ{
				ID:         pf.Id,
				FromSID:    pf.Operation[0].Sid,
				FromTime:   pf.Operation[0].Timestamp.AsTime().UnixNano() / 1000000,
				FromHeight: pf.Operation[0].Height,
				ToSID:      pf.Operation[1].Sid,
				ToTime:     pf.Operation[1].Timestamp.AsTime().UnixNano() / 1000000,
				ToHeight:   pf.Operation[1].Height,
			}
			// log.Printf("Broadcast PF (%4d bytes) %+v", len(sp.Cdata.Entity), pfj)
			jsonBytes, _ := json.Marshal(pfj)
			ioserv.BroadcastToRoom("/", "#", "pf", jsonBytes)
		} else {
			log.Printf("Unmarshaling err PF: %v", err)
		}
	} else {
		log.Printf("Broadcast Unknown (%4d bytes)", len(sp.Cdata.Entity))
		ioserv.BroadcastToRoom("/", "#", "unk_pf", sp.Cdata.Entity)
	}
	mu.Unlock()
}

func monitorStatus() {
	for {
		sxutil.SetNodeStatus(int32(runtime.NumGoroutine()), "PCWA")
		time.Sleep(time.Second * 3)
	}
}

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.InfoLevel)

	channelTypes := []uint32{pbase.PEOPLE_COUNTER_SVC, pbase.PEOPLE_FLOW_SVC}
	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "PCWA", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}
	log.Printf("Connecting SynerexServer at [%s]\n", sxServerAddress)

	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	wg := sync.WaitGroup{} // for syncing other goroutines

	ioserv = runSocketIOServer()
	fmt.Printf("Running PCWT Server..\n")
	if ioserv == nil {
		os.Exit(1)
	}

	client := sxutil.GrpcConnectServer(sxServerAddress) // if there is server address change, we should do it!

	argJSON := fmt.Sprintf("{Client:PCountWebApp}")

	pcClient := sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, argJSON)
	pfClient := sxutil.NewSXServiceClient(client, pbase.PEOPLE_FLOW_SVC, argJSON)

	pcMu, pcLoop = sxutil.SimpleSubscribeSupply(pcClient, supplyPCounterCallback)
	pfMu, pfLoop = sxutil.SimpleSubscribeSupply(pfClient, supplyPFlowCallback)

	wg.Add(1)

	go monitorStatus() // keep status

	assetsDir = http.Dir(*assetDir)
	log.Println("AssetDir:", assetsDir)

	go ioserv.Serve()

	http.Handle("/pwt/socket.io/", ioserv)
	http.HandleFunc("/api/", apiHandler)
	http.HandleFunc("/", assetsFileHandler)

	log.Printf("Starting PCounter Web App Provider %s  on port %d", version, *port)
	log.Printf("Go to http://localhost:%d", *port)
	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *port), nil)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
