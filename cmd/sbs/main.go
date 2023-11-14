package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/dashjay/supreme-blob-storage/pkg/gateway"
	"github.com/dashjay/supreme-blob-storage/pkg/index/ibadger"
	"github.com/dashjay/supreme-blob-storage/pkg/iraft/hashicorp"
	"github.com/dashjay/supreme-blob-storage/pkg/storage/disk"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

var (
	myAddr = flag.String("address", "0.0.0.0:50051", "TCP host+port for this node")
	raftId = flag.String("raft_id", "", "Node id used by Raft")

	baseDir       = flag.String("raft_data_dir", "data/", "Raft data dir")
	raftBootstrap = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
	monitorAddr   = flag.String("monitor_addr", ":9191", "monitor")
)

func main() {
	flag.Parse()

	if *raftId == "" {
		log.Fatalf("flag --raft_id is required")
	}

	raftDir := filepath.Join(*baseDir, "raft")
	indexDir := filepath.Join(*baseDir, "index")
	objectsDir := filepath.Join(*baseDir, "objects")

	ctx := context.Background()
	index := ibadger.NewBadgerDB(indexDir)
	r, err := hashicorp.NewRaft(ctx, *raftId, *myAddr, index, raftDir, *raftBootstrap)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}
	s := grpc.NewServer()

	r.RegisterGrpc(s)

	gw := gateway.NewServer(index, disk.NewDisk(objectsDir), r)

	handler := h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(
			r.Header.Get("Content-Type"), "application/grpc") {
			s.ServeHTTP(w, r)
		} else {
			if r.Method == http.MethodGet {
				gw.Get(w, r)
			} else if r.Method == http.MethodPut {
				gw.Set(w, r)
			} else if r.Method == http.MethodPost {
				gw.SyncContent(w, r)
			} else {
				http.Error(w, "unknown method "+r.Method, http.StatusInternalServerError)
			}
		}
	}), &http2.Server{})

	srv := &http.Server{Addr: *myAddr, Handler: handler}

	go func() {
		err = srv.ListenAndServe()
		if err != nil {
			log.Panicf("listen and serve error: %s", err)
		}
	}()

	if *monitorAddr != "" {
		srv := &http.Server{Addr: *monitorAddr, Handler: promhttp.Handler()}
		go func() {
			err = srv.ListenAndServe()
			if err != nil {
				log.Printf("listen and serve error: %s", err)
			}
		}()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	time.Sleep(3 * time.Second)

	<-sigs
	log.Println("shutting down server")
	subCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = srv.Shutdown(subCtx)
	if err != nil {
		log.Printf("shutdown server error: %s", err)
	}
	s.Stop()
	log.Println("server stopped")
}
