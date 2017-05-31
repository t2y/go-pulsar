package pulsar

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type Producer struct {
	client *Client
}

func (p *Producer) CreateProcuder(
	topic string, producerId, requestId uint64,
) (err error) {
	if err = p.client.LookupTopic(topic, requestId, false); err != nil {
		err = errors.Wrap(err, "failed to request lookup command")
		return
	}

	producer := &pulsar_proto.CommandProducer{
		Topic:      proto.String(topic),
		ProducerId: proto.Uint64(producerId),
		RequestId:  proto.Uint64(requestId),
	}

	if err = p.client.Send(&Request{Message: producer}); err != nil {
		err = errors.Wrap(err, "failed to send producer command")
		return
	}

	log.Debug("sent producer")
	return
}

func (p *Producer) ReceiveProducerSuccess() (
	success *pulsar_proto.CommandProducerSuccess, err error,
) {
	res, err := p.client.Receive()
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

func (p *Producer) Send(
	producerId, sequenceId uint64, numMessages int32,
	producerName string, payload string, isAsync bool,
) (err error) {
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
	}

	request := &Request{Message: send, Meta: meta, Payload: payload}
	if err = p.client.Send(request); err != nil {
		err = errors.Wrap(err, "failed to send 'send' command")
		return
	}

	log.Debug("sent 'send'")
	return
}

func (p *Producer) ReceiveSendReceipt() (
	receipt *pulsar_proto.CommandSendReceipt, err error,
) {
	res, err := p.client.Receive()
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
) (success *pulsar_proto.CommandSuccess, err error) {
	close := &pulsar_proto.CommandCloseProducer{
		ProducerId: proto.Uint64(producerId),
		RequestId:  proto.Uint64(requestId),
	}

	if err = p.client.Send(&Request{Message: close}); err != nil {
		err = errors.Wrap(err, "failed to send closeProducer command")
		return
	}

	log.Debug("sent closeProducer")
	return
}

func NewProducer(client *Client) (p *Producer) {
	client.Connect() // nolint: errcheck
	p = &Producer{
		client: client,
	}
	return
}
