package command

const (
	FrameSizeFieldSize        uint32 = 4
	FrameMagicNumberFieldSize uint32 = 2
	FrameChecksumSize         uint32 = 2
	FrameMetadataFieldSize    uint32 = 4
)

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
