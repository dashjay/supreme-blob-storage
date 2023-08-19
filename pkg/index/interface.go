package index

import (
	"github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"
)

type Interface raft.FSM

type IndexRecord struct {
	Key               string
	Content           []byte
	ContentPtr        string
	CreationTimestamp int64
	Size              int64
	Meta              map[string]string
}

func (i *IndexRecord) Marshal() ([]byte, error) {
	return msgpack.Marshal(i)
}

func UnmarshalIndexRecord(in []byte) (*IndexRecord, error) {
	var ir IndexRecord
	return &ir, msgpack.Unmarshal(in, &ir)
}
