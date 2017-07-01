package pulsar

import (
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type Consumer struct {
	*PulsarClient
}

func (c *Consumer) Subscribe(
	topic, subscription string,
	subType pulsar_proto.CommandSubscribe_SubType,
	consumerId, requestId uint64,
) (err error) {
	if err = c.SetLookupTopicConnection(topic, requestId, false); err != nil {
		err = errors.Wrap(err, "failed to set lookup topic connection")
		return
	}

	sub := &pulsar_proto.CommandSubscribe{
		Topic:        proto.String(topic),
		Subscription: proto.String(subscription),
		SubType:      subType.Enum(),
		ConsumerId:   proto.Uint64(consumerId),
		RequestId:    proto.Uint64(requestId),
	}

	err = c.conn.Send(&Request{Message: sub})
	if err != nil {
		err = errors.Wrap(err, "failed to send subscribe command")
		return
	}

	log.Debug("sent subscribe")
	return
}

func (c *Consumer) Flow(
	consumerId uint64, messagePermits uint32,
) (err error) {
	flow := &pulsar_proto.CommandFlow{
		ConsumerId:     proto.Uint64(consumerId),
		MessagePermits: proto.Uint32(messagePermits),
	}

	err = c.conn.Send(&Request{Message: flow})
	if err != nil {
		err = errors.Wrap(err, "failed to request flow command")
		return
	}

	log.Debug("sent flow")
	return
}

func (c *Consumer) ReceiveMessage() (msg *Message, err error) {
	res, err := c.conn.Receive()
	if err != nil {
		err = errors.Wrap(err, "failed to receive message command")
		return
	}

	cmd := res.BaseCommand.GetRawCommand().GetMessage()
	msg = NewMessage(cmd, res.Meta, res.Payload, res.BatchMessage)

	log.WithFields(log.Fields{
		"message":      cmd,
		"meta":         res.Meta,
		"payload":      res.Payload,
		"batchMessage": res.BatchMessage,
	}).Debug("receive message")
	return
}

func (c *Consumer) SendAck(
	consumerId uint64, ackType pulsar_proto.CommandAck_AckType,
	msgIdData *pulsar_proto.MessageIdData,
	validationError *pulsar_proto.CommandAck_ValidationError,
) (err error) {
	ack := &pulsar_proto.CommandAck{
		ConsumerId:      proto.Uint64(consumerId),
		AckType:         ackType.Enum(),
		MessageId:       msgIdData,
		ValidationError: validationError,
	}

	err = c.conn.Send(&Request{Message: ack})
	if err != nil {
		err = errors.Wrap(err, "failed to send ack command")
		return
	}

	log.Debug("sent ack")
	return
}

func (c *Consumer) SendRedeliverUnacknowledgedMessages(
	subType pulsar_proto.CommandSubscribe_SubType,
	consumerId uint64,
	idsList []*pulsar_proto.MessageIdData,
) (err error) {
	redeliver := &pulsar_proto.CommandRedeliverUnacknowledgedMessages{
		ConsumerId: proto.Uint64(consumerId),
	}
	if subType == pulsar_proto.CommandSubscribe_Shared && len(idsList) > 0 {
		// TODO: investigate message ids list behavior
		redeliver.MessageIds = idsList
	}

	err = c.Send(&Request{Message: redeliver})
	if err != nil {
		msg := "failed to send redeliver unacknowledged messages command"
		err = errors.Wrap(err, msg)
		return
	}

	log.Debug("sent redeliver")
	return
}

func (c *Consumer) CloseConsumer(
	consumerId, requestId uint64,
) (err error) {
	close := &pulsar_proto.CommandCloseConsumer{
		ConsumerId: proto.Uint64(consumerId),
		RequestId:  proto.Uint64(requestId),
	}

	err = c.conn.Send(&Request{Message: close})
	if err != nil {
		err = errors.Wrap(err, "failed to send closeConsumer command")
		return
	}

	log.Debug("sent closeConsumer")
	return
}

func NewConsumer(client *PulsarClient) (c *Consumer) {
	c = &Consumer{client}
	return
}
