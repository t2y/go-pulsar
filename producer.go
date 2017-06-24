package pulsar

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/t2y/go-pulsar/proto/command"
	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type Producer struct {
	*PulsarClient
}

func (p *Producer) CreateProcuder(
	topic string, producerId, requestId uint64,
) (err error) {
	if err = p.SetLookupTopicConnection(topic, requestId, false); err != nil {
		err = errors.Wrap(err, "failed to set lookup topic connection")
		return
	}

	producer := &pulsar_proto.CommandProducer{
		Topic:      proto.String(topic),
		ProducerId: proto.Uint64(producerId),
		RequestId:  proto.Uint64(requestId),
	}

	if err = p.conn.Send(&Request{Message: producer}); err != nil {
		err = errors.Wrap(err, "failed to send producer command")
		return
	}

	log.Debug("sent producer")
	return
}

func (p *Producer) ReceiveProducerSuccess() (
	success *pulsar_proto.CommandProducerSuccess, err error,
) {
	res, err := p.conn.Receive()
	if err != nil {
		err = errors.Wrap(err, "failed to receive producerSuccess command")
		return
	}

	base := res.BaseCommand
	switch t := base.GetType(); *t {
	case pulsar_proto.BaseCommand_PRODUCER_SUCCESS:
		success = base.GetRawCommand().GetProducerSuccess()
		log.WithFields(log.Fields{
			"success": success,
		}).Debug("created producer")
	case pulsar_proto.BaseCommand_ERROR:
		cmdError := base.GetRawCommand().GetError()
		err = errors.New(cmdError.String())
	default:
		// TODO: may receive other commands
		err = errors.Errorf("unknown command type: %v", *t)
	}

	return
}

const defaultNumMessages = 1

func (p *Producer) SendSend(
	producerId, sequenceId uint64, producerName, payload string,
	keyValues KeyValues,
) (err error) {
	send := &pulsar_proto.CommandSend{
		ProducerId:  proto.Uint64(producerId),
		SequenceId:  proto.Uint64(sequenceId),
		NumMessages: proto.Int32(defaultNumMessages),
	}

	now := time.Now().Unix()
	meta := &pulsar_proto.MessageMetadata{
		ProducerName: proto.String(producerName),
		SequenceId:   proto.Uint64(sequenceId),
		PublishTime:  proto.Uint64(uint64(now)),
		Properties:   keyValues.Convert(),
	}

	request := &Request{Message: send, Meta: meta, Payload: payload}
	if err = p.conn.Send(request); err != nil {
		err = errors.Wrap(err, "failed to send 'send' command")
		return
	}

	log.Debug("sent 'send'")
	return
}

func (p *Producer) SendBatchSend(
	producerId, sequenceId uint64,
	producerName string, batchMessage command.BatchMessage,
	compression *pulsar_proto.CompressionType,
) (err error) {
	numMessages := int32(len(batchMessage))
	send := &pulsar_proto.CommandSend{
		ProducerId:  proto.Uint64(producerId),
		SequenceId:  proto.Uint64(sequenceId),
		NumMessages: proto.Int32(numMessages),
	}

	now := time.Now().Unix()
	meta := &pulsar_proto.MessageMetadata{
		ProducerName: proto.String(producerName),
		SequenceId:   proto.Uint64(sequenceId),
		PublishTime:  proto.Uint64(uint64(now)),
		Properties:   []*pulsar_proto.KeyValue{},
		// batch mode
		Compression:        compression,
		NumMessagesInBatch: proto.Int32(numMessages),
	}

	request := &Request{Message: send, Meta: meta, BatchMessage: batchMessage}
	if err = p.conn.Send(request); err != nil {
		err = errors.Wrap(err, "failed to send batch 'send' command")
		return
	}

	log.Debug("sent batch 'send'")
	return
}

func (p *Producer) ReceiveSendReceipt() (
	receipt *pulsar_proto.CommandSendReceipt, err error,
) {
	res, err := p.conn.Receive()
	if err != nil {
		err = errors.Wrap(err, "failed to receive sendReceipt command")
		return
	}

	receipt = res.BaseCommand.GetRawCommand().GetSendReceipt()
	log.WithFields(log.Fields{
		"receipt": receipt,
	}).Debug("receive sendReceipt")
	return
}

func (p *Producer) CloseProducer(
	producerId, requestId uint64,
) (err error) {
	close := &pulsar_proto.CommandCloseProducer{
		ProducerId: proto.Uint64(producerId),
		RequestId:  proto.Uint64(requestId),
	}

	if err = p.conn.Send(&Request{Message: close}); err != nil {
		err = errors.Wrap(err, "failed to send closeProducer command")
		return
	}

	log.Debug("sent closeProducer")
	return
}

func NewProducer(client *PulsarClient) (p *Producer) {
	p = &Producer{client}
	return
}
