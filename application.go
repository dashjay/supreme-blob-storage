package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/errgroup"
)

const BigFileThrottle = 4096

type indexBadger struct {
	db *badger.DB
}

func NewBagerDB(path string) *indexBadger {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}
	return &indexBadger{db}
}

var _ raft.FSM = &indexBadger{}

func (f *indexBadger) Apply(l *raft.Log) interface{} {
	var kv = new(KV)
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
	return &snapshot{r: pr, eg: eg, cancel: cancel, ctx: egCtx}, nil
}

func (f *indexBadger) Restore(r io.ReadCloser) error {
	_ = f.db.DropAll()
	return f.db.Load(r, 32)
}

type snapshot struct {
	r      io.ReadCloser
	eg     *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := io.Copy(sink, s.r)
	if err != nil {
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {
	_ = s.r.Close()
	s.eg.Wait()
	s.cancel()
}

type ObjectStore struct {
	index   *indexBadger
	raft    *raft.Raft
	baseDir string
	client  *http.Client
}

type KV struct {
	Key               string
	Content           []byte
	Ptr               string
	CreationTimestamp int64
	Size              int64
}

func (o *ObjectStore) SyncContent(rw http.ResponseWriter, r *http.Request) {
	log.Printf("call SyncContent")
	defer log.Printf("call SyncContent finished")
	if r.Method != http.MethodPost {
		rw.Write([]byte("wrong method for sync"))
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	query := r.URL.Query()
	uid := query.Get("uuid")
	if r.ContentLength <= 0 {
		rw.Write([]byte("unknown content-length is unacceptable"))
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	file := filepath.Join(o.baseDir, "objects", uid[:2], uid)
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	fd, err := os.Create(file)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	m5 := md5.New()
	_, err = io.Copy(io.MultiWriter(fd, m5), io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("write finished with md5: %s", hex.EncodeToString(m5.Sum(nil)))
}

func (o *ObjectStore) Set(rw http.ResponseWriter, r *http.Request) {
	cfg := o.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		rw.Write([]byte("get config error"))
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	if r.ContentLength < 0 {
		rw.Write([]byte("unknown content-length is unacceptable"))
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	var kv *KV
	if r.ContentLength <= BigFileThrottle {
		content, err := ioutil.ReadAll(io.LimitReader(r.Body, r.ContentLength))
		if err != nil {
			rw.Write([]byte(fmt.Sprintf("read body error: %s", err)))
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		kv = &KV{
			Key:               r.RequestURI,
			Content:           content,
			Ptr:               "",
			CreationTimestamp: time.Now().Unix(),
			Size:              r.ContentLength,
		}
	} else {
		objectPtr := uuid.NewV4().String()
		srvs := cfg.Configuration().Servers
		var writeChans = make([]chan []byte, len(srvs))
		for i := 0; i < len(writeChans); i++ {
			writeChans[i] = make(chan []byte)
		}
		eg, ctxGroup := errgroup.WithContext(r.Context())
		for i := range srvs {
			srv := srvs[i]
			wc := writeChans[i]
			pr, pw := io.Pipe()
			eg.Go(func() error {
				requestUrl := "http://" + string(srv.Address) + "/?uuid=" + objectPtr
				defer log.Printf("do request to %s finished", requestUrl)
				req, err := http.NewRequestWithContext(ctxGroup, http.MethodPost, requestUrl, pr)
				if err != nil {
					http.Error(rw, fmt.Sprintf("new request error: %s", err), http.StatusInternalServerError)
					return err
				}
				req.Header.Set("Content-Type", "application/octet-stream")
				req.ContentLength = r.ContentLength
				log.Printf("do request to %s", requestUrl)
				resp, err := o.client.Do(req)
				if err != nil {
					log.Printf("do request to failed: %s error: %s", srv.Address, err)
					return err
				}
				defer resp.Body.Close()
				respContent, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				log.Printf("call sync content result: %s", string(respContent))
				return nil
			})

			eg.Go(func() error {
				sum := int64(0)
				for b := range wc {
					n, err := pw.Write(b)
					if err != nil {
						return err
					}
					if n != len(b) {
						return errors.New("short write")
					}
					sum += int64(n)
				}
				if sum != r.ContentLength {
					return errors.New("short write")
				}
				return pw.Close()
			})
		}
		var chunk = make([]byte, 32768)
		finished := false
		for !finished {
			n, err := r.Body.Read(chunk)
			if err != nil {
				if err != io.EOF {
					http.Error(rw, fmt.Sprintf("read body error: %s", err), http.StatusInternalServerError)
					return
				}
				finished = true
			}
			if n == 0 && finished {
				break
			}
			dup := make([]byte, n)
			copy(dup, chunk[:n])
			for i := range writeChans {
				writeChans[i] <- dup
			}
		}
		for i := range writeChans {
			close(writeChans[i])
		}
		err := eg.Wait()
		if err != nil {
			http.Error(rw, fmt.Sprintf("sync body error: %s", err), http.StatusInternalServerError)
			return
		}
		kv = &KV{
			Key:               r.RequestURI,
			Content:           nil,
			Ptr:               objectPtr,
			CreationTimestamp: time.Now().Unix(),
			Size:              r.ContentLength,
		}
	}
	indexBody, err := json.Marshal(kv)
	if err != nil {
		http.Error(rw, fmt.Sprintf("marshal index error: %s", err), http.StatusInternalServerError)
		return
	}
	f := o.raft.Apply(indexBody, time.Second)
	if err := f.Error(); err != nil {
		rw.Write([]byte(fmt.Sprintf("apply body error: %s", err)))
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.Write([]byte(strconv.Itoa(int(f.Index()))))
}

func (o *ObjectStore) Get(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/octet-stream")
	err := o.index.db.View(func(txn *badger.Txn) error {
		content, err := txn.Get([]byte(r.RequestURI))
		if err != nil {
			return err
		}
		var kv = new(KV)
		err = content.Value(func(val []byte) error {
			return json.Unmarshal(val, kv)
		})
		if err != nil {
			return err
		}

		if kv.Size >= BigFileThrottle {
			path := filepath.Join(o.baseDir, "objects", kv.Ptr[:2], kv.Ptr)
			fd, err := os.Open(path)
			if err != nil {
				return err
			}
			defer fd.Close()
			_, err = io.Copy(rw, fd)
			if err != nil {
				return err
			}
			return nil
		} else {
			rw.Write(kv.Content)
		}
		return nil
	})
	if err != nil {
		http.Error(rw, fmt.Sprintf("read index error: %s", err), http.StatusInternalServerError)
		return
	}
}
