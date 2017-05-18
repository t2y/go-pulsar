package pulsar

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

const (
	FrameFieldSize = 4
)

type Client struct {
	conn *net.TCPConn
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
	var cmd *Command
	cmd, err = NewCommandWithMessage(msg)
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

	cmd, err := NewCommandWithType(typ)
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

func (c *Client) Close() (err error) {
	err = c.conn.Close()
	return
}

func NewClient(c *Config) (client *Client, err error) {
	conn, err := net.DialTCP(c.Proto, c.LocalAddr, c.RemoteAddr)
	now := time.Now()
	conn.SetDeadline(now.Add(3 * time.Second))
	if err != nil {
		err = errors.Wrap(err, "failed to dial via tcp")
		return
	}

	client = &Client{
		conn: conn,
	}
	return
}
