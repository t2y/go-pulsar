package command

import (
	"encoding/hex"
	"fmt"
)

const (
	FrameSizeFieldSize        = 4
	FrameMagicNumberFieldSize = 2
	FrameChecksumSize         = 4
	FrameMetadataFieldSize    = 4
)

var FrameMagicNumber []byte
var FrameMagicAndChecksumSize int

func init() {
	FrameMagicNumber = []byte{0x0e, 0x01}
	FrameMagicAndChecksumSize = FrameMagicNumberFieldSize + FrameChecksumSize
}

type Frame struct {
	Cmddata  []byte
	Metadata []byte
	Payload  []byte
}

func (f *Frame) HasPayload() (r bool) {
	r = len(f.Metadata) > 0 || len(f.Payload) > 0
	return
}

func (f *Frame) String() (s string) {
	s = fmt.Sprintf(
		"\ncmd data:\n%smeta data:\n%s:\npayload:\n%s",
		hex.Dump(f.Cmddata), hex.Dump(f.Metadata), hex.Dump(f.Payload),
	)
	return
}
