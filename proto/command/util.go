package command

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

func NewSizeFrame(size int) (frame []byte, err error) {
	b := new(bytes.Buffer)
	err = binary.Write(b, binary.BigEndian, uint32(size))
	if err != nil {
		err = errors.Wrap(err, "failed to write as binary")
		return
	}

	frame = b.Bytes()
	return
}

func MarshalMessage(msg proto.Message) (
	size int, data []byte, err error,
) {
	msgFrame, err := proto.Marshal(msg)
	if err != nil {
		err = errors.Wrap(err, "failed to proto.Marshal message")
		return
	}

	msgLength := len(msgFrame)
	sizeFrame, err := NewSizeFrame(msgLength)
	if err != nil {
		err = errors.Wrap(err, "failed to create size frame")
		return
	}

	size = msgLength + int(FrameSizeFieldSize)
	data = append(sizeFrame, msgFrame...)
	return
}

func UnmarshalBatchMessagePayload(
	numMessages int32, payloadBytes []byte,
) (batchMessage BatchMessage, err error) {
	batchMessage = make(BatchMessage, numMessages)

	var i int32
	boundary := 0
	for i = 0; i < numMessages; i++ {
		metadataSizePos := boundary + FrameMetadataFieldSize
		singleMetaSize := binary.BigEndian.Uint32(payloadBytes[boundary:metadataSizePos])

		singleMetaPos := metadataSizePos + int(singleMetaSize)
		singleMetaBytes := payloadBytes[metadataSizePos:singleMetaPos]
		singleMeta := new(pulsar_proto.SingleMessageMetadata)
		err = proto.Unmarshal(singleMetaBytes, singleMeta)
		if err != nil {
			err = errors.Wrap(err, "failed to proto.Unmarshal single meta data")
			return
		}

		singlePayloadSize := singleMeta.GetPayloadSize()
		singlePayloadPos := singleMetaPos + int(singlePayloadSize)
		singlePayload := string(payloadBytes[singleMetaPos:singlePayloadPos])

		batchMessage[singlePayload] = singleMeta
		boundary = singlePayloadPos
	}
	return
}

func MakeBatchMessagePayload(
	batchMessage BatchMessage,
) (data []byte, err error) {
	for singlePayload, singleMetadata := range batchMessage {
		var meta []byte
		_, meta, err = MarshalMessage(singleMetadata)
		if err != nil {
			err = errors.Wrap(err, "failed to marshal single meta message")
			return
		}
		data = append(data, meta...)
		data = append(data, []byte(singlePayload)...)
	}
	return
}

func HasChecksum(frame []byte) (r bool) {
	nextBytes := frame[0:FrameMagicNumberFieldSize]
	r = bytes.Equal(nextBytes, FrameMagicNumber)
	return
}

func VerifyChecksum(data []byte) (msgAndPayload []byte, err error) {
	pos := FrameMagicNumberFieldSize + FrameChecksumSize
	checksum := data[FrameMagicNumberFieldSize:pos]
	msgAndPayload = data[pos:]

	calcChecksum := CalculateChecksum(msgAndPayload)
	if !bytes.Equal(checksum, calcChecksum) {
		s := "unmatch checksum: received: %s, calculate %s"
		err = errors.Errorf(s, checksum, calcChecksum)
		return
	}

	return
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func CalculateChecksum(data []byte) (checksum []byte) {
	checksum = make([]byte, FrameChecksumSize)
	value := crc32.Checksum(data, crc32cTable)
	binary.BigEndian.PutUint32(checksum, value)
	return
}
