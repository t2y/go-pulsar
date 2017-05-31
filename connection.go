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
	writeChanSize = 32
	readChanSize  = 32
)

type Request struct {
	Message proto.Message
	Meta    *pulsar_proto.MessageMetadata
	Payload string
}

type Response struct {
	BaseCommand *command.Base
	Meta        *pulsar_proto.MessageMetadata
	Payload     string
	Error       error
}

type AsyncTcpConn struct {
	wch     chan *Request
	ech     chan error
	rch     chan *Response
	timeout time.Duration

	readMutex sync.Mutex
	reqMutex  sync.Mutex
	conn      *net.TCPConn
}

func (ac *AsyncTcpConn) write(data []byte) (total int, err error) {
	if _, err = io.Copy(ac.conn, bytes.NewBuffer(data)); err != nil {
		err = errors.Wrap(err, "failed to write to connection")
		return
	}
	return
}

func (ac *AsyncTcpConn) writeLoop() {
	for {
		r, ok := <-ac.wch
		if !ok {
			return
		}

		data, err := command.NewMarshaledBase(r.Message, r.Meta, r.Payload)
		if err != nil {
			ac.ech <- errors.Wrap(err, "failed to marshal message")
			continue
		}

		_, err = ac.write(data)
		if err != nil {
			ac.ech <- errors.Wrap(err, "failed to write in writeLoop")
			continue
		}

		ac.ech <- nil
	}
}

func (ac *AsyncTcpConn) readFrame(size int64) (frame *bytes.Buffer, err error) {
	frame = bytes.NewBuffer(make([]byte, 0, size))
	if _, err = io.CopyN(frame, ac.conn, size); err != nil {
		err = errors.Wrap(err, "failed to read frame")
		return
	}

	return
}

func (ac *AsyncTcpConn) read() (frame *command.Frame, err error) {
	// there are 2 framing formats.
	//
	// 1. simple:
	//
	//	  [TOTAL_SIZE] [CMD_SIZE] [CMD]
	//
	// 2. payload:
	//
	//    [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM]
	//	  [METADATA_SIZE][METADATA] [PAYLOAD]
	//
	// note: it may receive without checksum for backward compatibility
	// https://github.com/yahoo/pulsar/issues/428
	//
	//	  [TOTAL_SIZE] [CMD_SIZE][CMD] [METADATA_SIZE][METADATA] [PAYLOAD]

	ac.readMutex.Lock()
	defer ac.readMutex.Unlock()

	totalSizeFrame, err := ac.readFrame(int64(command.FrameSizeFieldSize))
	if err != nil {
		err = errors.Wrap(err, "failed to read total size frame")
		return
	}
	totalSize := binary.BigEndian.Uint32(totalSizeFrame.Bytes())

	cmdSizeFrame, err := ac.readFrame(int64(command.FrameSizeFieldSize))
	if err != nil {
		err = errors.Wrap(err, "failed to read command size frame")
		return
	}

	cmdSize := binary.BigEndian.Uint32(cmdSizeFrame.Bytes())

	cmdFrame, err := ac.readFrame(int64(cmdSize))
	if err != nil {
		err = errors.Wrap(err, "failed to read command body frame")
		return
	}

	frame = new(command.Frame)
	frame.Cmddata = cmdFrame.Bytes()

	otherFramesSize := totalSize - (cmdSize + command.FrameSizeFieldSize)
	if otherFramesSize > 0 {
		var otherFrames *bytes.Buffer
		otherFrames, err = ac.readFrame(int64(otherFramesSize))
		if err != nil {
			err = errors.Wrap(err, "failed to read other frames")
			return
		}
		msgAndPayload := otherFrames.Bytes()

		if command.HasChecksum(msgAndPayload) {
			msgAndPayload, err = command.VerifyChecksum(msgAndPayload)
			if err != nil {
				err = errors.Wrap(err, "failed to verify checksum")
				return
			}
		}

		metadataSizePos := command.FrameMetadataFieldSize
		metadataSize := binary.BigEndian.Uint32(msgAndPayload[0:metadataSizePos])
		metadataPos := metadataSizePos + int(metadataSize)
		frame.Metadata = msgAndPayload[metadataSizePos:metadataPos]
		frame.Payload = msgAndPayload[metadataPos:]
	}

	return
}

func (ac *AsyncTcpConn) handleFrame(frame *command.Frame) (response *Response) {
	base := command.NewBase()
	if _, err := base.Unmarshal(frame.Cmddata); err != nil {
		err = errors.Wrap(err, "failed to unmarshal base")
		return &Response{Error: err}
	}

	switch t := base.GetType(); *t {
	case pulsar_proto.BaseCommand_PING:
		log.Debug("received ping")
		ac.conn.SetDeadline(time.Now().Add(ac.timeout))
		ac.wch <- &Request{Message: &pulsar_proto.CommandPong{}}
		log.Debug("send pong")
		return
	}

	response = &Response{BaseCommand: base}
	if frame.HasPayload() {
		meta, payload, err := base.UnmarshalMeta(frame.Metadata, frame.Payload)
		if err != nil {
			err = errors.Wrap(err, "failed to unmarshal meta")
			return &Response{Error: err}
		}

		response.Meta = meta
		response.Payload = payload
	}
	return
}

func (ac *AsyncTcpConn) readLoop() {
	for {
		frame, err := ac.read()
		if err != nil {
			switch e := errors.Cause(err); e {
			case io.EOF:
				return // maybe connection was closed
			default:
				if ne, ok := e.(net.Error); ok && ne.Timeout() {
					return // closed connection due to timeout
				}

				err = errors.Wrap(err, "failed to read in readLoop")
				ac.rch <- &Response{BaseCommand: nil, Error: err}
				continue
			}
		}

		if ac.rch == nil {
			return
		}

		response := ac.handleFrame(frame)
		if response == nil {
			continue
		}

		ac.rch <- response
	}
}

func (ac *AsyncTcpConn) Send(r *Request) (err error) {
	ac.wch <- r
	err, _ = <-ac.ech
	return
}

func (ac *AsyncTcpConn) Receive() (response *Response, err error) {
	ac.reqMutex.Lock()
	response, ok := <-ac.rch
	defer ac.reqMutex.Unlock()

	if !ok {
		err = errors.New("read channel has closed")
		return
	}

	err = response.Error
	if err == nil {
		log.WithFields(log.Fields{
			"base":    response.BaseCommand.GetRawCommand(),
			"meta":    response.Meta,
			"payload": response.Payload,
			"error":   response.Error,
		}).Debug("receive in AsyncTcpConn")
	}
	return
}

func (ac *AsyncTcpConn) Request(r *Request) (response *Response, err error) {
	ac.reqMutex.Lock()
	err = ac.Send(r)
	if err != nil {
		err = errors.Wrap(err, "failed to send in request")
		return
	}

	ac.reqMutex.Unlock()
	response, err = ac.Receive()
	if err != nil {
		err = errors.Wrap(err, "failed to receive in request")
		return
	}

	return
}

func (ac *AsyncTcpConn) Close() {
	ac.reqMutex.Lock()
	defer ac.reqMutex.Unlock()

	ac.conn.Close()
	close(ac.wch)
	close(ac.ech)
	close(ac.rch)
	ac.rch = nil
}

func (ac *AsyncTcpConn) Run() {
	go ac.writeLoop()
	go ac.readLoop()
}

func NewAsyncTcpConn(c *Config, tc *net.TCPConn) (ac *AsyncTcpConn) {
	ac = &AsyncTcpConn{
		conn: tc,
		wch:  make(chan *Request, writeChanSize),
		ech:  make(chan error, writeChanSize),
		rch:  make(chan *Response, readChanSize),

		timeout: c.Timeout,
	}
	ac.Run()
	return
}

func NewTcpConn(c *Config) (tc *net.TCPConn, err error) {
	tc, err = net.DialTCP(c.Proto, c.LocalAddr, c.RemoteAddr)
	if err != nil {
		err = errors.Wrap(err, "failed to dial via tcp")
		return
	}
	deadline := time.Now().Add(c.Timeout)
	tc.SetDeadline(deadline)

	log.WithFields(log.Fields{
		"remoteAddr": c.RemoteAddr,
		"deadline":   deadline,
	}).Debug("client settings")

	return
}
