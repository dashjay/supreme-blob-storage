package disk

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/dashjay/supreme-blob-storage/pkg/storage"
)

var (
	ErrShortWrite = errors.New("short write")
)

const hashPrefix = 2

type onDisk struct {
	baseDir string
}

func NewDisk(baseDir string) storage.Interface {
	return &onDisk{baseDir: baseDir}
}

func (d *onDisk) filePath(uuid string) string {
	return filepath.Join(d.baseDir, uuid[:hashPrefix], uuid)
}

func (d *onDisk) openReadOnly(fp string) (*os.File, error) {
	f, err := os.Open(fp)
	if os.IsNotExist(err) {
		return nil, storage.ErrObjectNotFound
	}
	return f, nil
}

func (d *onDisk) createForWrite(fp string) (*os.File, error) {
	return os.OpenFile(fp, os.O_RDWR|os.O_CREATE, 0755)
}

func (d *onDisk) GetObject(ctx context.Context, uuid string) ([]byte, error) {
	fd, err := d.openReadOnly(d.filePath(uuid))
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	return io.ReadAll(fd)
}

func (d *onDisk) WriteObject(ctx context.Context, uuid string, body []byte) error {
	fd, err := d.GetObjectWriter(ctx, uuid)
	if err != nil {
		return err
	}
	n, err := fd.Write(body)
	if err != nil {
		return err
	}
	if n != len(body) {
		return ErrShortWrite
	}
	return nil
}

func (d *onDisk) GetObjectReader(ctx context.Context, uuid string) (io.ReadSeekCloser, error) {
	return d.openReadOnly(d.filePath(uuid))
}

type syncOnClose struct {
	fd *os.File
}

func (s *syncOnClose) Close() error {
	err := s.fd.Sync()
	if err != nil {
		return err
	}
	return s.fd.Close()
}

func (s *syncOnClose) Write(p []byte) (int, error) {
	return s.fd.Write(p)
}

func (d *onDisk) GetObjectWriter(ctx context.Context, uuid string) (io.WriteCloser, error) {
	fp := d.filePath(uuid)
	err := os.MkdirAll(filepath.Dir(fp), 0755)
	if err != nil {
		return nil, err
	}
	fd, err := d.createForWrite(d.filePath(uuid))
	if err != nil {
		return nil, err
	}
	return &syncOnClose{fd: fd}, nil
}

func (d *onDisk) DeleteObject(ctx context.Context, uuid string) error {
	return os.Remove(d.filePath(uuid))
}

var _ storage.Interface = (*onDisk)(nil)
