package pulsar

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	command "github.com/t2y/go-pulsar/proto/command"
)

const (
	writeChanSize = 32
	readChanSize  = 32
)

type AsyncTcpConn struct {
	wch  chan proto.Message
	rch  chan *command.Frame
	conn *net.TCPConn

	reqMutex sync.Mutex
	mutex    sync.Mutex
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
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to marshal message")
			continue
		}

		_, err = ac.write(data)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to write in writeLoop")
			continue
		}
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

	ac.mutex.Lock()
	defer ac.mutex.Unlock()

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

func (ac *AsyncTcpConn) readLoop() {
	for {
		frame, err := ac.read()
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to read in readLoop")

			switch e := errors.Cause(err); e {
			case io.EOF:
				return // maybe connection was closed
			default:
				continue
			}
		}

		if ac.rch == nil {
			return
		}
		ac.rch <- frame
	}
}

func (ac *AsyncTcpConn) Send(msg proto.Message) {
	ac.wch <- msg
}

func (ac *AsyncTcpConn) Receive() (frame *command.Frame) {
	frame, _ = <-ac.rch
	return
}

func (ac *AsyncTcpConn) Request(msg proto.Message) (frame *command.Frame) {
	ac.reqMutex.Lock()
	defer ac.reqMutex.Unlock()

	ac.Send(msg)
	frame = ac.Receive()
	return
}

func (ac *AsyncTcpConn) Close() {
	ac.mutex.Lock()
	defer ac.mutex.Unlock()

	ac.conn.Close()
	close(ac.wch)
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
		rch:  make(chan *command.Frame, readChanSize),
	}
	ac.Run()
	return
}
