package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

var (
	myAddr = flag.String("address", "localhost:50051", "TCP host+port for this node")
	raftId = flag.String("raft_id", "", "Node id used by Raft")

	baseDir       = flag.String("raft_data_dir", "data/", "Raft data dir")
	raftBootstrap = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
)

func main() {
	flag.Parse()

	if *raftId == "" {
		log.Fatalf("flag --raft_id is required")
	}

	raftDir := filepath.Join(*baseDir, "raft")
	appDir := filepath.Join(*baseDir, "app")

	ctx := context.Background()
	wt := NewBagerDB(filepath.Join(appDir, "application-index"))

	r, tm, err := NewRaft(ctx, *raftId, *myAddr, wt, raftDir)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}
	s := grpc.NewServer()

	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example"})
	raftadmin.Register(s, r)
	objStore := &ObjectStore{
		index:   wt,
		raft:    r,
		baseDir: *baseDir,
		client:  &http.Client{},
	}
	handler := h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(
			r.Header.Get("Content-Type"), "application/grpc") {
			s.ServeHTTP(w, r)
		} else {
			if r.Method == http.MethodGet {
				objStore.Get(w, r)
			} else if r.Method == http.MethodPut {
				objStore.Set(w, r)
			} else if r.Method == http.MethodPost {
				objStore.SyncContent(w, r)
			} else {
				http.Error(w, "unknown method "+r.Method, http.StatusInternalServerError)
			}
		}
	}), &http2.Server{})

	srv := &http.Server{Addr: *myAddr, Handler: handler}

	go func() {
		err = srv.ListenAndServe()
		if err != nil {
			log.Printf("listen and serve error: %s", err)
		}
	}()

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

func NewRaft(ctx context.Context, myID, myAddress string, fsm raft.FSM, raftDir string) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)
	err := os.MkdirAll(raftDir, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}
	ldb, err := boltdb.NewBoltStore(filepath.Join(raftDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(raftDir, "logs.dat"), err)
	}
	lastIndex, err := ldb.LastIndex()
	if err != nil {
		return nil, nil, fmt.Errorf("boltdb.LastIndex error: %s", err)
	}
	log.Printf("lastIndex: %d", lastIndex)

	sdb, err := boltdb.NewBoltStore(filepath.Join(raftDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(raftDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, raftDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}
	if *raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	}
	return r, tm, nil
}
