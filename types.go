package pulsar

import (
	"github.com/golang/protobuf/proto"
	"github.com/t2y/go-pulsar/proto/command"
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

type Message struct {
	cmd          *pulsar_proto.CommandMessage
	meta         *pulsar_proto.MessageMetadata
	body         string
	batchMessage command.BatchMessage
}

func (m Message) GetMessageId() (data *pulsar_proto.MessageIdData) {
	data = m.cmd.GetMessageId()
	return
}

func (m Message) GetKeyValues() (keyValues KeyValues) {
	properties := m.meta.GetProperties()
	keyValues = ConvertPropertiesToKeyValues(properties)
	return
}

func (m Message) GetBody() (body string) {
	body = m.body
	return
}

func (m Message) HasBatchMessage() (result bool) {
	result = m.meta.GetNumMessagesInBatch() > 1
	return
}

func (m Message) GetBatchMessage() (batchMessage command.BatchMessage) {
	batchMessage = m.batchMessage
	return
}

func ConvertPropertiesToKeyValues(
	properties []*pulsar_proto.KeyValue,
) (keyValues KeyValues) {
	keyValues = make(KeyValues, 0, len(properties))
	for _, kv := range properties {
		if kv != nil {
			keyValue := KeyValue{Key: kv.GetKey(), Value: kv.GetValue()}
			keyValues = append(keyValues, keyValue)
		}
	}
	return
}

func NewMessage(
	cmd *pulsar_proto.CommandMessage,
	meta *pulsar_proto.MessageMetadata,
	body string,
	batchMessage command.BatchMessage,
) (msg *Message) {
	msg = &Message{
		cmd:          cmd,
		meta:         meta,
		body:         body,
		batchMessage: batchMessage,
	}
	return
}
