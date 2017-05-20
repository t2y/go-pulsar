package pulsar

import (
	"net"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	command "github.com/t2y/go-pulsar/proto/command"
	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

const (
	DefaultDeadlineTimeout = time.Duration(10) * time.Second
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

func (c *Client) KeepAlive() (err error) {
	ping := &pulsar_proto.CommandPing{}
	frame := c.conn.Request(ping)
	cmd := command.NewBaseWithType(
		pulsar_proto.BaseCommand_PONG.Enum(),
	)
	_, err = cmd.Unmarshal(frame.Cmddata)
	if err != nil {
		err = errors.Wrap(err, "failed to request ping command")
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
	frame := c.conn.Request(connect)

	cmd := command.NewBaseWithType(
		pulsar_proto.BaseCommand_CONNECTED.Enum(),
	)
	msg, err := cmd.Unmarshal(frame.Cmddata)
	if err != nil {
		err = errors.Wrap(err, "failed to request command command")
		return
	}
	c.state = ClientStateReady

	connected := msg.(*pulsar_proto.CommandConnected)
	log.WithFields(log.Fields{
		"connected": connected,
	}).Debug("connection is ready")

	return
}

func (c *Client) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.conn.Close()
	c.state = ClientStateNone
	return
}

func NewClient(c *Config) (client *Client, err error) {
	conn, err := net.DialTCP(c.Proto, c.LocalAddr, c.RemoteAddr)
	if err != nil {
		err = errors.Wrap(err, "failed to dial via tcp")
		return
	}
	deadline := time.Now().Add(c.Timeout)
	conn.SetDeadline(deadline)

	log.WithFields(log.Fields{
		"remoteAddr": c.RemoteAddr,
		"deadline":   deadline,
	}).Debug("client settings")

	client = &Client{
		conn:  NewAsyncTcpConn(conn),
		state: ClientStateNone,
	}
	return
}
