package pulsar

import (
	"net"
	"net/url"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

const (
	DefaultDeadlineTimeout = time.Duration(40) * time.Second
)

const (
	ClientName             = "go-pulsar"
	DefaultProtocolVersion = 9
)

var (
	ErrKeepAlive                 = errors.New("failed to receive pong command")
	ErrLookupTopicResponseFailed = errors.New(
		"got failed as response type from lookup topic",
	)
)

type PulsarClient struct {
	Conn
	conn       Conn // own connection for a broker created by topic lookup response
	partitions uint32
}

func (c *PulsarClient) LookupTopicWithConnect(
	conn Conn, topic string, requestId uint64, authoritative bool,
) (ac *AsyncConn, response *pulsar_proto.CommandLookupTopicResponse, err error) {
	lookup := &pulsar_proto.CommandLookupTopic{
		Topic:         proto.String(topic),
		RequestId:     proto.Uint64(requestId),
		Authoritative: proto.Bool(authoritative),
	}
	response, err = conn.LookupTopic(lookup)
	if err != nil {
		err = errors.Wrap(err, "failed to call LookupTopic")
		return
	}

	switch r := response.GetResponse(); r {
	case pulsar_proto.CommandLookupTopicResponse_Redirect:
		config := c.GetConfig().Copy()
		ac, err = newAsyncConnFromLookupTopicResponse(config, response)
		if err != nil {
			err = errors.Wrap(err, "failed to create async tcp connection from topic response")
			return
		}

		var connect *pulsar_proto.CommandConnect
		connect, err = NewCommandConnect(config, true)
		if err != nil {
			err = errors.Wrap(err, "failed to create connect command")
			return
		}

		if err = ac.Connect(connect); err != nil {
			err = errors.Wrap(err, "failed to connect service url from topic lookup")
			return
		}
	case pulsar_proto.CommandLookupTopicResponse_Connect:
		// do nothing
	case pulsar_proto.CommandLookupTopicResponse_Failed:
		err = ErrLookupTopicResponseFailed
	default:
		err = errors.Errorf("unknown lookup topic response type: %v", r)
	}

	return
}

// Set p.conn to a broker received by lookup topic response
func (c *PulsarClient) SetLookupTopicConnection(
	topic string, requestId uint64, authoritative bool,
) (err error) {
	var lookupConn *AsyncConn
	lookupConn, _, err = c.LookupTopicWithConnect(c.conn, topic, requestId, false)
	if err != nil {
		err = errors.Wrap(err, "failed to get connection and topic lookup")
		return
	}

	if lookupConn != nil {
		for {
			conn, _, e := c.LookupTopicWithConnect(
				lookupConn, topic, requestId, false,
			)
			if e != nil {
				lookupConn.Close()
				err = errors.Wrap(e, "failed to resend lookup topic to a broker")
				return
			} else if conn == nil {
				c.conn = lookupConn
				break
			}
			// need to close previous lookup connection before
			// replacing lookupConn with service url from lookup topic response
			lookupConn.Close()
			lookupConn = conn
		}
	}

	return
}

func (c *PulsarClient) GetPartitionedTopicMetadata(
	topic string, requestId uint64,
) (err error) {
	var res *Response
	metadata := &pulsar_proto.CommandPartitionedTopicMetadata{
		Topic:     proto.String(topic),
		RequestId: proto.Uint64(requestId),
	}
	res, err = c.Request(&Request{Message: metadata})
	if err != nil {
		err = errors.Wrap(err, "failed to request PartitionedTopicMetadata command")
		return
	}

	response := res.BaseCommand.GetRawCommand().GetPartitionMetadataResponse()
	c.partitions = response.GetPartitions()
	return
}

func (c *PulsarClient) KeepAlive() (err error) {
	var res *Response
	ping := &pulsar_proto.CommandPing{}
	res, err = c.Request(&Request{Message: ping})
	if err != nil {
		err = errors.Wrap(err, "failed to request ping command")
		return
	}

	if res.BaseCommand.GetRawCommand().GetPong() == nil {
		err = ErrKeepAlive
		return
	}

	log.Debug("keepalive")
	return
}

func (c *PulsarClient) ReceiveSuccess() (success *pulsar_proto.CommandSuccess, err error) {
	res, err := c.Receive()
	if err != nil {
		err = errors.Wrap(err, "failed to receive succcess command")
		return
	}

	success = res.BaseCommand.GetRawCommand().GetSuccess()
	log.WithFields(log.Fields{
		"success": success,
	}).Debug("receive success")
	return
}

func (c *PulsarClient) Close() {
	if c.GetConnection() != c.conn {
		c.conn.Close()
	}
}

func newAsyncConnFromLookupTopicResponse(
	config *Config, response *pulsar_proto.CommandLookupTopicResponse,
) (ac *AsyncConn, err error) {
	config.ServiceURL, err = url.Parse(response.GetBrokerServiceUrl())
	if err != nil {
		err = errors.Wrap(err, "failed to parse service url from lookup topic")
		return
	}
	config.RemoteAddr, err = net.ResolveTCPAddr(PROTO_TCP, config.ServiceURL.Host)
	if err != nil {
		err = errors.Wrap(err, "failed to resolve remote tcp address")
		return
	}

	var conn net.Conn
	conn, err = NewConn(config)
	if err != nil {
		err = errors.Wrap(err, "failed to create tcp connection")
		return
	}
	ac = NewAsyncConn(config, conn)

	return
}

func NewClient(ac *AsyncConn) (client *PulsarClient) {
	client = &PulsarClient{ac, ac.GetConnection(), 0}
	return
}
