package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	//	"github.com/golang/protobuf/ptypes"
	"github.com/jackc/pgx/v4/pgxpool"

	//	"github.com/mtfelian/golang-socketio/transport"
	//	pcounter "github.com/synerex/proto_pcounter"

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
	sxServerAddress string
	pcMu            *sync.Mutex = nil
	pcLoop          *bool       = nil
	db              *pgxpool.Pool
	db_host         = os.Getenv("POSTGRES_HOST")
	db_name         = os.Getenv("POSTGRES_DB")
	db_user         = os.Getenv("POSTGRES_USER")
	db_pswd         = os.Getenv("POSTGRES_PASSWORD")
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
	ctx := context.Background()
	addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
	print("connecting to " + addr + "\n")
	var err error
	db, err = pgxpool.Connect(ctx, addr)
	if err != nil {
		print("connection error: ")
		log.Println(err)
		log.Fatal("\n")
	}
	defer db.Close()

	// ping
	err = db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create users table
	_, err = db.Exec(ctx, "create table if not exists users(id BIGSERIAL NOT NULL, sub CHAR(36) NOT NULL, username VARCHAR(64) NOT NULL, email VARCHAR(256) NOT NULL, primary key(id))")
	if err != nil {
		print("create users table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create data_source_groups table
	_, err = db.Exec(ctx, "create table if not exists data_source_groups(id BIGSERIAL NOT NULL, parent_id BIGINT NOT NULL, user_id BIGINT NOT NULL, name VARCHAR(256) NOT NULL, opt VARCHAR(1024) NOT NULL, foreign key (user_id) references users(id), primary key(id))")
	if err != nil {
		print("create data_source_groups table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create data_sources table
	_, err = db.Exec(ctx, "create table if not exists data_sources(id BIGINT NOT NULL, user_id BIGINT NOT NULL, group_id BIGINT NOT NULL, name VARCHAR(256) NOT NULL, type INT NOT NULL, latitude DOUBLE PRECISION NOT NULL DEFAULT 0, longitude DOUBLE PRECISION NOT NULL DEFAULT 0, radius DOUBLE PRECISION NOT NULL DEFAULT 0, opt VARCHAR(1024) NOT NULL, foreign key (user_id) references users(id), foreign key (group_id) references data_source_groups(id), primary key(id))")
	if err != nil {
		print("create data_sources table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create data_stores table
	_, err = db.Exec(ctx, "create table if not exists data_stores(id BIGSERIAL NOT NULL, type INT NOT NULL, addr VARCHAR(1024) NOT NULL, auth_username VARCHAR(64) NOT NULL, auth_password VARCHAR(1024) NOT NULL, opt VARCHAR(1024) NOT NULL, primary key(id))")
	if err != nil {
		print("create data_stores table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create data_saves table
	_, err = db.Exec(ctx, "create table if not exists data_saves(id BIGSERIAL NOT NULL, data_source_id BIGINT NOT NULL, time_from TIMESTAMP NOT NULL, time_to TIMESTAMP NOT NULL, data_store_id BIGINT NOT NULL, path VARCHAR(1024) NOT NULL, opt VARCHAR(1024) NOT NULL, foreign key (data_source_id) references data_sources(id), foreign key (data_store_id) references data_stores(id), primary key(id))")
	if err != nil {
		print("create data_saves table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create perms table
	_, err = db.Exec(ctx, "create table if not exists perms(id BIGSERIAL NOT NULL, user_id BIGINT NOT NULL, data_source_id BIGINT NOT NULL, granularity_time INT, granularity_mesh INT, opt VARCHAR(1024), foreign key (user_id) references users(id), foreign key (data_source_id) references data_sources(id), primary key(id))")
	if err != nil {
		print("create perms table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create fast tablespace
	_, err = db.Exec(ctx, "CREATE TABLESPACE fast_space LOCATION '/fast_store'")
	if err != nil {
		print("create fast_space tablespace error: ")
		log.Println(err)
		// log.Fatal("\n")
	}

	// create slow tablespace
	_, err = db.Exec(ctx, "CREATE TABLESPACE slow_space LOCATION '/slow_store'")
	if err != nil {
		print("create slow_space tablespace error: ")
		log.Println(err)
		// log.Fatal("\n")
	}

	// create move_old_chunks procedure
	_, err = db.Exec(ctx, `CREATE OR REPLACE PROCEDURE move_old_chunks (job_id int, config jsonb)
	LANGUAGE PLPGSQL
	AS $$
	DECLARE
	  ht REGCLASS;
	  lag interval;
	  destination name;
	  chunk REGCLASS;
	BEGIN
	  SELECT jsonb_object_field_text (config, 'hypertable')::regclass INTO STRICT ht;
	  SELECT jsonb_object_field_text (config, 'lag')::interval INTO STRICT lag;
	  SELECT jsonb_object_field_text (config, 'tablespace') INTO STRICT destination;
	
	
	  IF ht IS NULL OR lag IS NULL OR destination IS NULL THEN
		RAISE EXCEPTION 'Config must have hypertable, lag and destination';
	  END IF;
	
	
	  PERFORM move_chunk(
		chunk => i,
		destination_tablespace => destination,
		index_destination_tablespace => destination
	  ) FROM show_chunks(ht, older_than => lag) i;
	END
	$$;`)
	if err != nil {
		print("CREATE OR REPLACE PROCEDURE move_old_chunks error: ")
		log.Println(err)
		// log.Fatal("\n")
	}
}

func supplyPCINFDBCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	mu.Lock()

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
