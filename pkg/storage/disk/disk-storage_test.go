package disk_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/dashjay/supreme-blob-storage/pkg/storage"
	"github.com/dashjay/supreme-blob-storage/pkg/storage/disk"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestDisk(t *testing.T) {
	sto := disk.NewDisk(t.TempDir())
	t.Run("write and get", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), 4096)
		key := uuid.NewV4().String()
		assert.Nil(t, sto.WriteObject(context.TODO(), key, body))
		content, err := sto.GetObject(context.TODO(), key)
		assert.Nil(t, err)
		bytes.Equal(body, content)
	})

	t.Run("write with writer and get", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), 4096)
		key := uuid.NewV4().String()
		wc, err := sto.GetObjectWriter(context.TODO(), key)
		assert.Nil(t, err)
		n, err := wc.Write(body)
		assert.Nil(t, err)
		assert.Len(t, body, n)
		assert.Nil(t, wc.Close())
		content, err := sto.GetObject(context.TODO(), key)
		assert.Nil(t, err)
		bytes.Equal(body, content)
	})

	t.Run("write with writer and get with reader", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), 4096)
		key := uuid.NewV4().String()
		wc, err := sto.GetObjectWriter(context.TODO(), key)
		assert.Nil(t, err)
		n, err := wc.Write(body)
		assert.Nil(t, err)
		assert.Len(t, body, n)
		assert.Nil(t, wc.Close())
		rd, err := sto.GetObjectReader(context.TODO(), key)
		assert.Nil(t, err)
		content, err := io.ReadAll(rd)
		assert.Nil(t, rd.Close())
		assert.Nil(t, err)
		assert.Equal(t, body, content)
	})

	t.Run("write and delete", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), 4096)
		key := uuid.NewV4().String()
		assert.Nil(t, sto.WriteObject(context.TODO(), key, body))
		assert.Nil(t, sto.DeleteObject(context.TODO(), key))
		content, err := sto.GetObject(context.TODO(), key)
		assert.Equal(t, err, storage.ErrObjectNotFound)
		assert.Nil(t, content)
	})
}
