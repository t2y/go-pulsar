package pulsar

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

const (
	DefaultDeadlineTimeout = time.Duration(40) * time.Second
)

const (
	ClientName             = "go-pulsar"
	DefaultProtocolVersion = 7
)

type Client interface {
	Conn
	KeepAlive() error
	LookupTopic(topic string, requestId uint64, authoritative bool) error
	ReceiveSuccess() (*pulsar_proto.CommandSuccess, error)
}

type PulsarClient struct {
	Conn
}

func (c *PulsarClient) LookupTopic(
	topic string, requestId uint64, authoritative bool,
) (err error) {

	lookup := &pulsar_proto.CommandLookupTopic{
		Topic:         proto.String(topic),
		RequestId:     proto.Uint64(requestId),
		Authoritative: proto.Bool(authoritative),
	}

	var res *Response
	res, err = c.Request(&Request{Message: lookup})
	if err != nil {
		err = errors.Wrap(err, "failed to send lookupTopic command")
		return
	}
	log.Debug("sent lookupTopic")

	response := res.BaseCommand.GetRawCommand().GetLookupTopicResponse()
	if response == nil {
		err = errors.Wrap(err, "failed to receive lookupTopicResponse command")
		return
	}

	log.WithFields(log.Fields{
		"response": response,
	}).Debug("performed lookup topic")

	return
}

func (c *PulsarClient) KeepAlive() (err error) {
	var res *Response
	ping := &pulsar_proto.CommandPing{}
	res, err = c.Request(&Request{Message: ping})
	if err != nil {
		err = errors.Wrap(err, "failed to request ping command")
		return
	}

	if res.BaseCommand.GetRawCommand().GetPong() == nil {
		err = errors.New("failed to receive pong command")
		return
	}

	log.Debug("keepalive")
	return
}

func (c *PulsarClient) ReceiveSuccess() (success *pulsar_proto.CommandSuccess, err error) {
	res, err := c.Receive()
	if err != nil {
		err = errors.Wrap(err, "failed to receive succcess command")
		return
	}

	success = res.BaseCommand.GetRawCommand().GetSuccess()
	log.WithFields(log.Fields{
		"success": success,
	}).Debug("receive success")
	return
}

func NewClient(ac *AsyncTcpConn) (client *PulsarClient) {
	client = &PulsarClient{ac}
	return
}
