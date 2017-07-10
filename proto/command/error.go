package command

import (
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type commandError struct {
	message string
	raw     *pulsar_proto.CommandError
}

func (e *commandError) Error() string {
	return e.message
}

func NewCommandError(cmdErr *pulsar_proto.CommandError) (err error) {
	return &commandError{
		message: cmdErr.GetMessage(),
		raw:     cmdErr, // TODO: how to handle raw attribute
	}
}

func IsErrorOnProtocol(err error) (r bool) {
	var e interface{}
	e = errors.Cause(err)

	switch e.(type) {
	case *commandError:
		r = true
	default:
		r = false
	}

	return
}
