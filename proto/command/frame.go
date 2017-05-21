package command

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
	Checksum []byte
	Metadata []byte
	Payload  []byte
}

func (f *Frame) HasPayload() (r bool) {
	r = len(f.Metadata) > 0 || len(f.Payload) > 0
	return
}
