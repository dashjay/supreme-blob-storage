package hashicorp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/dashjay/supreme-blob-storage/pkg/iraft"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Raft struct {
	r *raft.Raft
	t *transport.Manager
}

func NewRaft(ctx context.Context, raftID, serverAddr string, fsm raft.FSM, raftDir string, isBootstrap bool) (iraft.Interface, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(raftID)
	err := os.MkdirAll(raftDir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	ldb, err := boltdb.NewBoltStore(filepath.Join(raftDir, "logs.dat"))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(raftDir, "logs.dat"), err)
	}
	sdb, err := boltdb.NewBoltStore(filepath.Join(raftDir, "stable.dat"))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(raftDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, raftDir, err)
	}
	tm := transport.New(raft.ServerAddress(serverAddr), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}
	if isBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(raftID),
					Address:  raft.ServerAddress(serverAddr),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	}
	return &Raft{r: r, t: tm}, nil
}

func (s *Raft) RegisterGrpc(srv *grpc.Server) {
	s.t.Register(srv)
	leaderhealth.Setup(s.r, srv, []string{"Example"})
	raftadmin.Register(srv, s.r)
}

func (s *Raft) Apply(data []byte, timeout time.Duration) (uint64, error) {
	res := s.r.Apply(data, timeout)
	err := res.Error()
	if err != nil {
		return 0, err
	}
	return res.Index(), nil
}

func (s *Raft) Peers() ([]string, error) {
	g := s.r.GetConfiguration()
	if err := g.Error(); err != nil {
		return nil, err
	}
	var out []string
	srvs := g.Configuration().Servers
	for i := range srvs {
		out = append(out, string(srvs[i].Address))
	}
	return out, nil
}
