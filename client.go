package pulsar

import (
	"sync"
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

type ClientState int

const (
	ClientStateNone             ClientState = 0
	ClientStateSentConnectFrame ClientState = 1
	ClientStateReady            ClientState = 2
)

type Client struct {
	conn  *AsyncTcpConn
	mutex sync.Mutex
	state ClientState
}

func (c *Client) LookupTopic(
	topic string, requestId uint64, authoritative bool,
) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	lookup := &pulsar_proto.CommandLookupTopic{
		Topic:         proto.String(topic),
		RequestId:     proto.Uint64(requestId),
		Authoritative: proto.Bool(authoritative),
	}

	var res *Response
	res, err = c.conn.Request(&Request{Message: lookup})
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

func (c *Client) KeepAlive() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var res *Response
	ping := &pulsar_proto.CommandPing{}
	res, err = c.conn.Request(&Request{Message: ping})
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

func (c *Client) Connect() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.state == ClientStateReady {
		log.Debug("connection has already established")
		return
	}

	connect := &pulsar_proto.CommandConnect{
		ClientVersion:   proto.String(ClientName),
		AuthMethod:      pulsar_proto.AuthMethod_AuthMethodNone.Enum(),
		ProtocolVersion: proto.Int32(DefaultProtocolVersion),
	}
	var res *Response
	res, err = c.conn.Request(&Request{Message: connect})
	if err != nil {
		err = errors.Wrap(err, "failed to request connect command")
		return
	}

	connected := res.BaseCommand.GetRawCommand().GetConnected()
	if connected == nil {
		err = errors.New("failed to receive connected command")
		return
	}
	c.state = ClientStateReady

	log.WithFields(log.Fields{
		"connected": connected,
	}).Debug("connection is ready")

	return
}

func (c *Client) ReceiveSuccess() (success *pulsar_proto.CommandSuccess, err error) {
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

func (c *Client) Send(r *Request) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = c.conn.Send(r)
	return
}

func (c *Client) Receive() (res *Response, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res, err = c.conn.Receive()
	return
}

func (c *Client) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.conn.Close()
	c.state = ClientStateNone
	return
}

func NewClient(ac *AsyncTcpConn) (client *Client) {
	client = &Client{
		conn:  ac,
		state: ClientStateNone,
	}
	return
}
