package ibadger

import (
	"context"
	"encoding/json"
	"io"
	"log"

	"github.com/dashjay/supreme-blob-storage/pkg/index"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
)

type indexBadger struct {
	db *badger.DB
}

var _ index.Interface = (*indexBadger)(nil)

func NewBadgerDB(path string) index.Interface {
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
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(kv.Key), l.Data)
	})
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

func (f *indexBadger) Locate(key string) (*index.IndexRecord, error) {
	var out *index.IndexRecord
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out, err = index.UnmarshalIndexRecord(val)
			if err != nil {
				return err
			}
			return nil
		})
	})
	return out, err
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
	_ = s.eg.Wait()
	s.cancel()
}
