package pulsar

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
)

func CreateSizeFrame(size int) (frame []byte, err error) {
	b := new(bytes.Buffer)
	err = binary.Write(b, binary.BigEndian, uint32(size))
	if err != nil {
		err = errors.Wrap(err, "failed to write as binary")
		return
	}

	frame = b.Bytes()
	return
}
