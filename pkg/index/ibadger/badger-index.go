package ibadger

import (
	"context"
	"encoding/json"
	"io"
	"log"

	"github.com/Jille/raft-grpc-example/pkg/index"
	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
)

type indexBadger struct {
	db *badger.DB
}

var _ index.Interface = (*indexBadger)(nil)

func NewBagerDB(path string) *indexBadger {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}
	return &indexBadger{db}
}

func (f *indexBadger) Apply(l *raft.Log) interface{} {
	var kv = new(index.IndexRecord)
	err := json.Unmarshal(l.Data, kv)
	if err != nil {
		panic(err)
	}
	f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(kv.Key), l.Data)
	})
	return nil
}

func (f *indexBadger) Snapshot() (raft.FSMSnapshot, error) {
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		_, err := f.db.Backup(pw, f.db.MaxVersion())
		return err
	})
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &badgerSnapshot{r: pr, eg: eg, cancel: cancel, ctx: egCtx}, nil
}

func (f *indexBadger) Restore(r io.ReadCloser) error {
	_ = f.db.DropAll()
	return f.db.Load(r, 32)
}

type badgerSnapshot struct {
	r      io.ReadCloser
	eg     *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *badgerSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := io.Copy(sink, s.r)
	if err != nil {
		return err
	}
	return sink.Close()
}

func (s *badgerSnapshot) Release() {
	_ = s.r.Close()
	s.eg.Wait()
	s.cancel()
}
