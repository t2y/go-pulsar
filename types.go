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

func (kvs KeyValues) Convert() (properties []*pulsar_proto.KeyValue) {
	properties = make([]*pulsar_proto.KeyValue, 0, len(kvs))
	for _, keyValue := range kvs {
		kv := &pulsar_proto.KeyValue{
			Key:   proto.String(keyValue.Key),
			Value: proto.String(keyValue.Value),
		}
		properties = append(properties, kv)
	}
	return
}
