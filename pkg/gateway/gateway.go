package gateway

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/dashjay/supreme-blob-storage/pkg/index"
	"github.com/dashjay/supreme-blob-storage/pkg/iraft"
	"github.com/dashjay/supreme-blob-storage/pkg/storage"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	idx        index.Interface
	objStore   storage.Interface
	httpClient *http.Client
	r          iraft.Interface
}

type ServerConfig struct {
	httpClient *http.Client
}

func WithCustomHTTPClient(c *http.Client) Option {
	return func(sc *ServerConfig) {
		sc.httpClient = c
	}
}

type Option func(sc *ServerConfig)

func (s *ServerConfig) Build(idx index.Interface, objStore storage.Interface, r iraft.Interface) *Server {
	if s.httpClient == nil {
		s.httpClient = &http.Client{}
	}
	return &Server{idx: idx, objStore: objStore, httpClient: s.httpClient, r: r}
}

func NewServer(idx index.Interface, objStore storage.Interface, r iraft.Interface, opts ...Option) *Server {
	sc := &ServerConfig{}
	for i := range opts {
		opts[i](sc)
	}
	return sc.Build(idx, objStore, r)
}

const BigFileThrottle = 4096

func (s *Server) SyncContent(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "wrong method for sync", http.StatusInternalServerError)
		return
	}
	query := r.URL.Query()
	uid := query.Get("uuid")
	if r.ContentLength <= 0 {
		http.Error(rw, "unknown content-length is unacceptable", http.StatusBadRequest)
		return
	}
	wc, err := s.objStore.GetObjectWriter(r.Context(), uid)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = io.Copy(wc, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	err = wc.Close()
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) Set(rw http.ResponseWriter, r *http.Request) {
	if r.ContentLength < 0 {
		http.Error(rw, "unknown content-length is unacceptable", http.StatusBadRequest)
		return
	}
	var kv *index.IndexRecord
	if r.ContentLength <= BigFileThrottle {
		content, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength))
		if err != nil {
			http.Error(rw, fmt.Sprintf("read body error: %s", err), http.StatusInternalServerError)
			return
		}
		kv = &index.IndexRecord{
			Key:               r.RequestURI,
			Content:           content,
			ContentPtr:        "",
			CreationTimestamp: time.Now().Unix(),
			Size:              r.ContentLength,
		}
	} else {
		peers, err := s.r.Peers()
		if err != nil {
			http.Error(rw, "get peers error", http.StatusBadRequest)
			return
		}
		objectPtr := uuid.NewV4().String()
		var writeChans = make([]chan []byte, len(peers))
		for i := 0; i < len(writeChans); i++ {
			writeChans[i] = make(chan []byte)
		}
		eg, ctxGroup := errgroup.WithContext(r.Context())
		for i := range peers {
			peer := peers[i]
			wc := writeChans[i]
			pr, pw := io.Pipe()
			eg.Go(func() error {
				requestUrl := "http://" + string(peer) + "/?uuid=" + objectPtr
				req, err := http.NewRequestWithContext(ctxGroup, http.MethodPost, requestUrl, pr)
				if err != nil {
					http.Error(rw, fmt.Sprintf("new request error: %s", err), http.StatusInternalServerError)
					return err
				}
				req.Header.Set("Content-Type", "application/octet-stream")
				req.ContentLength = r.ContentLength
				resp, err := s.httpClient.Do(req)
				if err != nil {
					return err
				}
				resp.Body.Close()
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
		err = eg.Wait()
		if err != nil {
			http.Error(rw, fmt.Sprintf("sync body error: %s", err), http.StatusInternalServerError)
			return
		}
		kv = &index.IndexRecord{
			Key:               r.RequestURI,
			Content:           nil,
			ContentPtr:        objectPtr,
			CreationTimestamp: time.Now().Unix(),
			Size:              r.ContentLength,
		}
	}
	indexBody, err := kv.Marshal()
	if err != nil {
		http.Error(rw, fmt.Sprintf("marshal index error: %s", err), http.StatusInternalServerError)
		return
	}
	appliedIndex, err := s.r.Apply(indexBody, time.Second)
	if err != nil {
		http.Error(rw, fmt.Sprintf("apply body error: %s", err), http.StatusInternalServerError)
		return
	}
	_, _ = rw.Write([]byte(strconv.Itoa(int(appliedIndex))))
}

func (s *Server) Get(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/octet-stream")
	ir, err := s.idx.Locate(r.RequestURI)
	if err != nil {
		http.Error(rw, fmt.Sprintf("read index error: %s", err), http.StatusInternalServerError)
		return
	}
	if ir.Size >= BigFileThrottle {
		rs, err := s.objStore.GetObjectReader(r.Context(), ir.ContentPtr)
		if err != nil {
			http.Error(rw, fmt.Sprintf("read index error: %s", err), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Length", strconv.Itoa(int(ir.Size)))
		_, err = io.Copy(rw, rs)
		if err != nil {
			http.Error(rw, fmt.Sprintf("copy body error: %s", err), http.StatusInternalServerError)
			return
		}
	} else {
		_, _ = rw.Write(ir.Content)
	}
}
