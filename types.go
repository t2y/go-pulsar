package pulsar

import (
	"github.com/golang/protobuf/proto"
	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type KeyValue struct {
	Key   string
	Value string
}

type KeyValues []KeyValue

func ConvertKeyValues(keyValues KeyValues) (kvs []*pulsar_proto.KeyValue) {
	kvs = make([]*pulsar_proto.KeyValue, 0, len(keyValues))
	for _, keyValue := range keyValues {
		kv := &pulsar_proto.KeyValue{
			Key:   proto.String(keyValue.Key),
			Value: proto.String(keyValue.Value),
		}
		kvs = append(kvs, kv)
	}
	return
}
