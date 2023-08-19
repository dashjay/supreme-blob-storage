package iraft

import (
	"time"

	"google.golang.org/grpc"
)

type ApplyResponse interface {
	Error() error
	Index() uint64
}

type Interface interface {
	RegisterGrpc(srv *grpc.Server)
	Apply(data []byte, timeout time.Duration) (uint64, error)
	Peers() ([]string, error)
}
