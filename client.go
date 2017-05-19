package pulsar

import (
	"bytes"
	"encoding/binary"
	"io"
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
	FrameFieldSize         = 4
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
	conn  *net.TCPConn
	mutex sync.Mutex
	state ClientState
}

func (c *Client) Send(data []byte) (total int, err error) {
	if _, err = io.Copy(c.conn, bytes.NewBuffer(data)); err != nil {
		err = errors.Wrap(err, "failed to send data")
		c.Close() // nolint: errcheck
		return
	}
	return
}

func (c *Client) SendCommand(msg proto.Message) (n int, err error) {
	var cmd *command.Base
	cmd, err = command.NewBaseWithMessage(msg)
	if err != nil {
		err = errors.Wrap(err, "failed to create command")
		return
	}

	data, err := cmd.Marshal()
	if err != nil {
		err = errors.Wrap(err, "failed to marshal command")
		return
	}

	n, err = c.Send(data)
	if err != nil {
		err = errors.Wrap(err, "failed to send command")
		return
	}

	return
}

func (c *Client) ReceiveCommand(
	typ *pulsar_proto.BaseCommand_Type,
) (msg proto.Message, err error) {
	totalFrame := bytes.NewBuffer(make([]byte, 0, FrameFieldSize))
	if _, err = io.CopyN(totalFrame, c.conn, FrameFieldSize); err != nil {
		err = errors.Wrap(err, "failed to receive total frame")
		return
	}

	totalSize := binary.BigEndian.Uint32(totalFrame.Bytes())
	cmdSizeAndData := bytes.NewBuffer(make([]byte, 0, totalSize))
	if _, err = io.CopyN(cmdSizeAndData, c.conn, int64(totalSize)); err != nil {
		err = errors.Wrap(err, "failed to receive command frame")
		return
	}

	frames := cmdSizeAndData.Bytes()
	cmdSize := binary.BigEndian.Uint32(frames[0:FrameFieldSize])
	data := frames[FrameFieldSize:]
	dataLength := len(data)
	if cmdSize != uint32(dataLength) {
		err = errors.Errorf(
			"command size value and actual data length are not matched:"+
				" size: %d, length: %d", cmdSize, dataLength)
		return
	}

	cmd, err := command.NewBaseWithType(typ)
	if err != nil {
		err = errors.Wrap(err, "failed to create command")
		return
	}

	msg, err = cmd.Unmarshal(typ, data)
	if err != nil {
		err = errors.Wrap(err, "failed to unmarshal command")
		return
	}

	return
}

func (c *Client) KeepAlive() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, err = c.SendCommand(&pulsar_proto.CommandPing{})
	if err != nil {
		err = errors.Wrap(err, "failed to send ping command")
		return
	}

	_, err = c.ReceiveCommand(
		pulsar_proto.BaseCommand_PONG.Enum(),
	)
	if err != nil {
		err = errors.Wrap(err, "failed to receive pong command")
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

	_, err = c.SendCommand(connect)
	if err != nil {
		err = errors.Wrap(err, "failed to send connect command")
		return
	}
	log.Debug("sent connect")

	msg, err := c.ReceiveCommand(
		pulsar_proto.BaseCommand_CONNECTED.Enum(),
	)
	if err != nil {
		err = errors.Wrap(err, "failed to receive connected command")
		return
	}
	c.state = ClientStateReady

	connected := msg.(*pulsar_proto.CommandConnected)
	log.WithFields(log.Fields{
		"connected": connected,
	}).Debug("connection is ready")

	return
}

func (c *Client) Close() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = c.conn.Close()
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
		conn:  conn,
		state: ClientStateNone,
	}
	return
}
