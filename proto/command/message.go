package command

import (
	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type BatchMessage map[string]*pulsar_proto.SingleMessageMetadata

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
