package gateway_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dashjay/supreme-blob-storage/pkg/gateway"
	"github.com/dashjay/supreme-blob-storage/pkg/index/ibadger"
	"github.com/dashjay/supreme-blob-storage/pkg/iraft/hashicorp"
	"github.com/dashjay/supreme-blob-storage/pkg/storage/disk"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

func TestGateway(t *testing.T) {
	tempDir := t.TempDir()
	raftDir := filepath.Join(tempDir, "raft")
	indexDir := filepath.Join(tempDir, "index")
	objectsDir := filepath.Join(tempDir, "objects")

	ctx := context.Background()
	index := ibadger.NewBadgerDB(indexDir)
	r, err := hashicorp.NewRaft(ctx, "1", "localhost:8080", index, raftDir, true)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}
	s := grpc.NewServer()

	r.RegisterGrpc(s)

	gw := gateway.NewServer(index, disk.NewDisk(objectsDir), r)

	handler := h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(
			r.Header.Get("Content-Type"), "application/grpc") {
			s.ServeHTTP(w, r)
		} else {
			if r.Method == http.MethodGet {
				gw.Get(w, r)
			} else if r.Method == http.MethodPut {
				gw.Set(w, r)
			} else if r.Method == http.MethodPost {
				gw.SyncContent(w, r)
			} else {
				http.Error(w, "unknown method "+r.Method, http.StatusInternalServerError)
			}
		}
	}), &http2.Server{})

	srv := &http.Server{Addr: ":8080", Handler: handler}
	go func() {
		err = srv.ListenAndServe()
		if err != nil {
			log.Printf("listen and serve error: %s", err)
		}
	}()
	defer func() {
		assert.Nil(t, srv.Shutdown(context.TODO()))
	}()
	time.Sleep(2 * time.Second)
	var httpClient = http.DefaultClient
	t.Run("test write sbs", func(t *testing.T) {
		data := bytes.Repeat([]byte("1"), 1024*1024*64) // 16M
		var records []float64
		for i := 0; i < 10; i++ {
			start := time.Now()
			req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:8080/temp-file.%d", i), bytes.NewReader(data))
			assert.Nil(t, err)
			resp, err := httpClient.Do(req)
			assert.Nil(t, err)
			if err != nil {
				content, _ := io.ReadAll(resp.Body)
				t.Logf("%s\n", string(content))
			}
			assert.Equal(t, 200, resp.StatusCode)
			consume := time.Since(start)
			t.Logf("write %d MB in %.2f sec", len(data)/1024/1024, consume.Seconds())
			records = append(records, consume.Seconds())
		}

		sum := float64(0)
		for i := range records {
			sum += records[i]
		}
		t.Logf("ave: %.2f secc", sum/float64(len(records)))
	})
}
