package storage

import (
	"context"
	"errors"
	"io"
)

var ErrObjectNotFound = errors.New("object not found")

type Interface interface {
	GetObject(ctx context.Context, uuid string) ([]byte, error)
	WriteObject(ctx context.Context, uuid string, body []byte) error
	GetObjectReader(ctx context.Context, uuid string) (io.ReadSeekCloser, error)
	GetObjectWriter(ctx context.Context, uuid string) (io.WriteCloser, error)
	DeleteObject(ctx context.Context, uuid string) error
}
