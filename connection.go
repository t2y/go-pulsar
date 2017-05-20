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

type Response struct {
	BaseCommand *command.Base
	Error       error
}

type AsyncTcpConn struct {
	wch  chan proto.Message
	ech  chan error
	rch  chan *Response
	conn *net.TCPConn

	readMutex        sync.Mutex
	reqMutex         sync.Mutex
	sendReceiveMutex sync.Mutex
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
		msg, ok := <-ac.wch
		if !ok {
			return
		}

		data, err := command.NewMarshaledBase(msg)
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
	// 1. simple: [TOTAL_SIZE] [CMD_SIZE] [CMD]
	//
	// 2. with payload: [TOTAL_SIZE] [CMD_SIZE][CMD]
	//					[MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA]
	//					[PAYLOAD]

	ac.readMutex.Lock()
	defer ac.readMutex.Unlock()

	totalSizeFrame, err := ac.readFrame(int64(command.FrameSizeFieldSize))
	if err != nil {
		err = errors.Wrap(err, "failed to read total size frame")
		return
	}

	totalSize := binary.BigEndian.Uint32(totalSizeFrame.Bytes())
	log.Debug(totalSize)

	cmdSizeFrame, err := ac.readFrame(int64(command.FrameSizeFieldSize))
	if err != nil {
		err = errors.Wrap(err, "failed to read command size frame")
		return
	}

	cmdSize := binary.BigEndian.Uint32(cmdSizeFrame.Bytes())
	log.Debug(cmdSize)

	cmdFrame, err := ac.readFrame(int64(cmdSize))
	if err != nil {
		err = errors.Wrap(err, "failed to read command body frame")
		return
	}

	frame = new(command.Frame)
	frame.Cmddata = cmdFrame.Bytes()

	otherFramesSize := totalSize - (cmdSize + command.FrameSizeFieldSize)
	if otherFramesSize > 0 {
		var _otherFrames *bytes.Buffer
		_otherFrames, err = ac.readFrame(int64(otherFramesSize))
		if err != nil {
			err = errors.Wrap(err, "failed to read other frames")
			return
		}
		otherFrames := _otherFrames.Bytes()

		magicNumber := otherFrames[0:command.FrameMagicNumberFieldSize]
		log.Debug(magicNumber)

		checksumPos := command.FrameMagicNumberFieldSize + command.FrameChecksumSize
		frame.Checksum = otherFrames[command.FrameMagicNumberFieldSize:checksumPos]
		log.Debug(frame.Checksum)

		metadataSizePos := checksumPos + command.FrameMetadataFieldSize
		metadataSize := binary.BigEndian.Uint32(otherFrames[checksumPos:metadataSizePos])
		log.Debug(metadataSize)

		metadataPos := metadataSizePos + metadataSize
		frame.Metadata = otherFrames[metadataSizePos:metadataPos]
		log.Debug(frame.Metadata)
		frame.Payload = otherFrames[metadataPos:]
		log.Debug(frame.Payload)
	}

	return
}

func (ac *AsyncTcpConn) handleFrame(frame *command.Frame) (response *Response) {
	base := command.NewBase()
	_, err := base.Unmarshal(frame.Cmddata)
	if err != nil {
		err = errors.Wrap(err, "failed to unmarshal command")
		return &Response{Error: err}
	}

	switch t := base.GetType(); *t {
	case pulsar_proto.BaseCommand_PING:
		log.Debug("received ping")
		ac.wch <- &pulsar_proto.CommandPong{}
		log.Debug("send pong")
		return
	}

	response = &Response{BaseCommand: base}
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

func (ac *AsyncTcpConn) Send(msg proto.Message) (err error) {
	ac.sendReceiveMutex.Lock()
	ac.wch <- msg
	err, _ = <-ac.ech
	ac.sendReceiveMutex.Unlock()
	return
}

func (ac *AsyncTcpConn) Receive() (base *command.Base, err error) {
	ac.sendReceiveMutex.Lock()
	response, ok := <-ac.rch
	ac.sendReceiveMutex.Unlock()

	if !ok {
		err = errors.New("read channel has closed")
		return
	}

	base = response.BaseCommand
	err = response.Error
	return
}

func (ac *AsyncTcpConn) Request(msg proto.Message) (base *command.Base, err error) {
	ac.reqMutex.Lock()
	defer ac.reqMutex.Unlock()

	err = ac.Send(msg)
	if err != nil {
		err = errors.Wrap(err, "failed to send in request")
		return
	}

	base, err = ac.Receive()
	if err != nil {
		err = errors.Wrap(err, "failed to receive in request")
		return
	}

	return
}

func (ac *AsyncTcpConn) Close() {
	ac.sendReceiveMutex.Lock()
	defer ac.sendReceiveMutex.Unlock()

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

func NewAsyncTcpConn(tc *net.TCPConn) (ac *AsyncTcpConn) {
	ac = &AsyncTcpConn{
		conn: tc,
		wch:  make(chan proto.Message, writeChanSize),
		ech:  make(chan error, writeChanSize),
		rch:  make(chan *Response, readChanSize),
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
