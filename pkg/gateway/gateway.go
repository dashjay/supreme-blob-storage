package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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
	objstore   storage.Interface
	httpClient *http.Client
	r          iraft.Interface
}

func NewServer(idx index.Interface, objstore storage.Interface, r iraft.Interface) *Server {
	return &Server{idx: idx, objstore: objstore, httpClient: &http.Client{}, r: r}
}

const BigFileThrottle = 4096

func (s *Server) SyncContent(rw http.ResponseWriter, r *http.Request) {
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
	wc, err := s.objstore.GetObjectWriter(r.Context(), uid)
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
		rw.Write([]byte("unknown content-length is unacceptable"))
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	var kv *index.IndexRecord
	if r.ContentLength <= BigFileThrottle {
		content, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength))
		if err != nil {
			rw.Write([]byte(fmt.Sprintf("read body error: %s", err)))
			rw.WriteHeader(http.StatusInternalServerError)
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
			rw.Write([]byte("get peers error"))
			rw.WriteHeader(http.StatusBadRequest)
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
				defer resp.Body.Close()
				respContent, err := io.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				log.Printf("call sync content result: %s\n", string(respContent))
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
	indexBody, err := json.Marshal(kv)
	if err != nil {
		http.Error(rw, fmt.Sprintf("marshal index error: %s", err), http.StatusInternalServerError)
		return
	}
	index, err := s.r.Apply(indexBody, time.Second)
	if err != nil {
		http.Error(rw, fmt.Sprintf("apply body error: %s", err), http.StatusInternalServerError)
		return
	}
	rw.Write([]byte(strconv.Itoa(int(index))))
}

func (o *Server) Get(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/octet-stream")
	ir, err := o.idx.Locate(r.RequestURI)
	if err != nil {
		http.Error(rw, fmt.Sprintf("read index error: %s", err), http.StatusInternalServerError)
		return
	}
	if ir.Size >= BigFileThrottle {
		rs, err := o.objstore.GetObjectReader(r.Context(), ir.ContentPtr)
		if err != nil {
			http.Error(rw, fmt.Sprintf("read index error: %s", err), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Length", strconv.Itoa(int(ir.Size)))
		io.Copy(rw, rs)

	} else {
		rw.Write(ir.Content)
	}
}
