package command

import (
	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type Message struct {
	cmd  *pulsar_proto.CommandMessage
	meta *pulsar_proto.MessageMetadata
	body string
}

func (m Message) GetMessageId() (data *pulsar_proto.MessageIdData) {
	data = m.cmd.GetMessageId()
	return
}

func (m Message) GetBody() (body string) {
	body = m.body
	return
}

func NewMessage(
	cmd *pulsar_proto.CommandMessage,
	meta *pulsar_proto.MessageMetadata,
	body string,
) (msg *Message) {
	msg = &Message{
		cmd:  cmd,
		meta: meta,
		body: body,
	}
	return
}

func NewMessageIdData(
	ledgerId, entryId *uint64, partition, batchIndex *int32,
) (data *pulsar_proto.MessageIdData) {
	data = &pulsar_proto.MessageIdData{
		LedgerId:   ledgerId,
		EntryId:    entryId,
		Partition:  partition,
		BatchIndex: batchIndex,
	}
	return
}
