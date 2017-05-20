package command

import (
	"regexp"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

type Base struct {
	base pulsar_proto.BaseCommand
}

func (c *Base) GetRawCommand() (raw *pulsar_proto.BaseCommand) {
	raw = &c.base
	return
}

func (c *Base) Marshal() (data []byte, err error) {
	data, err = proto.Marshal(&c.base)
	if err != nil {
		err = errors.Wrap(err, "failed to proto.Marshal")
		return
	}

	dataLength := len(data)
	totalFrame, err := NewSizeFrame(dataLength + int(FrameSizeFieldSize))
	if err != nil {
		err = errors.Wrap(err, "failed to get total frame")
		return
	}

	cmdSizeFrame, err := NewSizeFrame(dataLength)
	if err != nil {
		err = errors.Wrap(err, "failed to get command frame")
		return
	}
	data = append(append(totalFrame, cmdSizeFrame...), data...)
	return
}

func (c *Base) Unmarshal(buf []byte) (msg proto.Message, err error) {
	err = proto.Unmarshal(buf, &c.base)
	if err != nil {
		err = errors.Wrap(err, "failed to proto.Unmarshal")
		return
	}

	if c.base.Type == nil {
		if err = c.SetTypeFromData(); err != nil {
			err = errors.Wrap(err, "failed to set type from data")
			return
		}
	}

	switch t := *c.base.Type; t {
	case pulsar_proto.BaseCommand_CONNECT:
		msg = c.base.Connect
	case pulsar_proto.BaseCommand_CONNECTED:
		msg = c.base.Connected
	case pulsar_proto.BaseCommand_SUBSCRIBE:
		msg = c.base.Subscribe
	case pulsar_proto.BaseCommand_PRODUCER:
		msg = c.base.Producer
	case pulsar_proto.BaseCommand_SEND:
		msg = c.base.Send
	case pulsar_proto.BaseCommand_SEND_RECEIPT:
		msg = c.base.SendReceipt
	case pulsar_proto.BaseCommand_SEND_ERROR:
		msg = c.base.SendError
	case pulsar_proto.BaseCommand_MESSAGE:
		msg = c.base.Message
	case pulsar_proto.BaseCommand_ACK:
		msg = c.base.Ack
	case pulsar_proto.BaseCommand_FLOW:
		msg = c.base.Flow
	case pulsar_proto.BaseCommand_UNSUBSCRIBE:
		msg = c.base.Unsubscribe
	case pulsar_proto.BaseCommand_SUCCESS:
		msg = c.base.Success
	case pulsar_proto.BaseCommand_ERROR:
		msg = c.base.Error
	case pulsar_proto.BaseCommand_CLOSE_PRODUCER:
		msg = c.base.CloseProducer
	case pulsar_proto.BaseCommand_CLOSE_CONSUMER:
		msg = c.base.CloseConsumer
	case pulsar_proto.BaseCommand_PRODUCER_SUCCESS:
		msg = c.base.ProducerSuccess
	case pulsar_proto.BaseCommand_PING:
		msg = c.base.Ping
	case pulsar_proto.BaseCommand_PONG:
		msg = c.base.Pong
	case pulsar_proto.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		msg = c.base.RedeliverUnacknowledgedMessages
	case pulsar_proto.BaseCommand_PARTITIONED_METADATA:
		msg = c.base.PartitionMetadata
	case pulsar_proto.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		msg = c.base.PartitionMetadataResponse
	case pulsar_proto.BaseCommand_LOOKUP:
		msg = c.base.LookupTopic
	case pulsar_proto.BaseCommand_LOOKUP_RESPONSE:
		msg = c.base.LookupTopicResponse
	case pulsar_proto.BaseCommand_CONSUMER_STATS:
		msg = c.base.ConsumerStats
	case pulsar_proto.BaseCommand_CONSUMER_STATS_RESPONSE:
		msg = c.base.ConsumerStatsResponse
	default:
		err = errors.Errorf("unknown command type: %v", c.base.Type)
	}

	return
}

func (c *Base) SetType(typ *pulsar_proto.BaseCommand_Type) (err error) {
	c.base.Type = typ
	switch t := *typ; t {
	case pulsar_proto.BaseCommand_CONNECT:
		c.base.Connect = new(pulsar_proto.CommandConnect)
	case pulsar_proto.BaseCommand_CONNECTED:
		c.base.Connected = new(pulsar_proto.CommandConnected)
	case pulsar_proto.BaseCommand_SUBSCRIBE:
		c.base.Subscribe = new(pulsar_proto.CommandSubscribe)
	case pulsar_proto.BaseCommand_PRODUCER:
		c.base.Producer = new(pulsar_proto.CommandProducer)
	case pulsar_proto.BaseCommand_SEND:
		c.base.Send = new(pulsar_proto.CommandSend)
	case pulsar_proto.BaseCommand_SEND_RECEIPT:
		c.base.SendReceipt = new(pulsar_proto.CommandSendReceipt)
	case pulsar_proto.BaseCommand_SEND_ERROR:
		c.base.SendError = new(pulsar_proto.CommandSendError)
	case pulsar_proto.BaseCommand_MESSAGE:
		c.base.Message = new(pulsar_proto.CommandMessage)
	case pulsar_proto.BaseCommand_ACK:
		c.base.Ack = new(pulsar_proto.CommandAck)
	case pulsar_proto.BaseCommand_FLOW:
		c.base.Flow = new(pulsar_proto.CommandFlow)
	case pulsar_proto.BaseCommand_UNSUBSCRIBE:
		c.base.Unsubscribe = new(pulsar_proto.CommandUnsubscribe)
	case pulsar_proto.BaseCommand_SUCCESS:
		c.base.Success = new(pulsar_proto.CommandSuccess)
	case pulsar_proto.BaseCommand_ERROR:
		c.base.Error = new(pulsar_proto.CommandError)
	case pulsar_proto.BaseCommand_CLOSE_PRODUCER:
		c.base.CloseProducer = new(pulsar_proto.CommandCloseProducer)
	case pulsar_proto.BaseCommand_CLOSE_CONSUMER:
		c.base.CloseConsumer = new(pulsar_proto.CommandCloseConsumer)
	case pulsar_proto.BaseCommand_PRODUCER_SUCCESS:
		c.base.ProducerSuccess = new(pulsar_proto.CommandProducerSuccess)
	case pulsar_proto.BaseCommand_PING:
		c.base.Ping = new(pulsar_proto.CommandPing)
	case pulsar_proto.BaseCommand_PONG:
		c.base.Pong = new(pulsar_proto.CommandPong)
	case pulsar_proto.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		c.base.RedeliverUnacknowledgedMessages = new(pulsar_proto.CommandRedeliverUnacknowledgedMessages)
	case pulsar_proto.BaseCommand_PARTITIONED_METADATA:
		c.base.PartitionMetadata = new(pulsar_proto.CommandPartitionedTopicMetadata)
	case pulsar_proto.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		c.base.PartitionMetadataResponse = new(pulsar_proto.CommandPartitionedTopicMetadataResponse)
	case pulsar_proto.BaseCommand_LOOKUP:
		c.base.LookupTopic = new(pulsar_proto.CommandLookupTopic)
	case pulsar_proto.BaseCommand_LOOKUP_RESPONSE:
		c.base.LookupTopicResponse = new(pulsar_proto.CommandLookupTopicResponse)
	case pulsar_proto.BaseCommand_CONSUMER_STATS:
		c.base.ConsumerStats = new(pulsar_proto.CommandConsumerStats)
	case pulsar_proto.BaseCommand_CONSUMER_STATS_RESPONSE:
		c.base.ConsumerStatsResponse = new(pulsar_proto.CommandConsumerStatsResponse)
	default:
		err = errors.Errorf("unknown command type: %v", typ)
	}

	return
}

var reCommandType = regexp.MustCompile(`^type:(.+?)\s.*`)

func (c *Base) SetTypeFromData() (err error) {
	s := c.base.String()
	group := reCommandType.FindStringSubmatch(s)
	if group == nil {
		err = errors.Errorf("failed to find type from data: %s", s)
		return
	}

	typeStr := group[1]
	typValue, ok := pulsar_proto.BaseCommand_Type_value[typeStr]
	if !ok {
		err = errors.Errorf("failed to find type value from type string: %s", typeStr)
		return
	}

	c.base.Type = pulsar_proto.BaseCommand_Type(typValue).Enum()
	return
}

func (c *Base) SetMessage(msg proto.Message) (err error) {
	switch t := msg.(type) {
	case *pulsar_proto.CommandConnect:
		c.base.Type = pulsar_proto.BaseCommand_CONNECT.Enum()
		c.base.Connect = t
	case *pulsar_proto.CommandConnected:
		c.base.Type = pulsar_proto.BaseCommand_CONNECTED.Enum()
		c.base.Connected = t
	case *pulsar_proto.CommandSubscribe:
		c.base.Type = pulsar_proto.BaseCommand_SUBSCRIBE.Enum()
		c.base.Subscribe = t
	case *pulsar_proto.CommandProducer:
		c.base.Type = pulsar_proto.BaseCommand_PRODUCER.Enum()
		c.base.Producer = t
	case *pulsar_proto.CommandSend:
		c.base.Type = pulsar_proto.BaseCommand_SEND.Enum()
		c.base.Send = t
	case *pulsar_proto.CommandSendReceipt:
		c.base.Type = pulsar_proto.BaseCommand_SEND_RECEIPT.Enum()
		c.base.SendReceipt = t
	case *pulsar_proto.CommandSendError:
		c.base.Type = pulsar_proto.BaseCommand_SEND_ERROR.Enum()
		c.base.SendError = t
	case *pulsar_proto.CommandMessage:
		c.base.Type = pulsar_proto.BaseCommand_MESSAGE.Enum()
		c.base.Message = t
	case *pulsar_proto.CommandAck:
		c.base.Type = pulsar_proto.BaseCommand_ACK.Enum()
		c.base.Ack = t
	case *pulsar_proto.CommandFlow:
		c.base.Type = pulsar_proto.BaseCommand_FLOW.Enum()
		c.base.Flow = t
	case *pulsar_proto.CommandUnsubscribe:
		c.base.Type = pulsar_proto.BaseCommand_UNSUBSCRIBE.Enum()
		c.base.Unsubscribe = t
	case *pulsar_proto.CommandSuccess:
		c.base.Type = pulsar_proto.BaseCommand_SUCCESS.Enum()
		c.base.Success = t
	case *pulsar_proto.CommandError:
		c.base.Type = pulsar_proto.BaseCommand_ERROR.Enum()
		c.base.Error = t
	case *pulsar_proto.CommandCloseProducer:
		c.base.Type = pulsar_proto.BaseCommand_CLOSE_PRODUCER.Enum()
		c.base.CloseProducer = t
	case *pulsar_proto.CommandCloseConsumer:
		c.base.Type = pulsar_proto.BaseCommand_CLOSE_CONSUMER.Enum()
		c.base.CloseConsumer = t
	case *pulsar_proto.CommandProducerSuccess:
		c.base.Type = pulsar_proto.BaseCommand_PRODUCER_SUCCESS.Enum()
		c.base.ProducerSuccess = t
	case *pulsar_proto.CommandPing:
		c.base.Type = pulsar_proto.BaseCommand_PING.Enum()
		c.base.Ping = t
	case *pulsar_proto.CommandPong:
		c.base.Type = pulsar_proto.BaseCommand_PONG.Enum()
		c.base.Pong = t
	case *pulsar_proto.CommandRedeliverUnacknowledgedMessages:
		c.base.Type = pulsar_proto.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES.Enum()
		c.base.RedeliverUnacknowledgedMessages = t
	case *pulsar_proto.CommandPartitionedTopicMetadata:
		c.base.Type = pulsar_proto.BaseCommand_PARTITIONED_METADATA.Enum()
		c.base.PartitionMetadata = t
	case *pulsar_proto.CommandPartitionedTopicMetadataResponse:
		c.base.Type = pulsar_proto.BaseCommand_PARTITIONED_METADATA_RESPONSE.Enum()
		c.base.PartitionMetadataResponse = t
	case *pulsar_proto.CommandLookupTopic:
		c.base.Type = pulsar_proto.BaseCommand_LOOKUP.Enum()
		c.base.LookupTopic = t
	case *pulsar_proto.CommandLookupTopicResponse:
		c.base.Type = pulsar_proto.BaseCommand_LOOKUP_RESPONSE.Enum()
		c.base.LookupTopicResponse = t
	case *pulsar_proto.CommandConsumerStats:
		c.base.Type = pulsar_proto.BaseCommand_CONSUMER_STATS.Enum()
		c.base.ConsumerStats = t
	case *pulsar_proto.CommandConsumerStatsResponse:
		c.base.Type = pulsar_proto.BaseCommand_CONSUMER_STATS_RESPONSE.Enum()
		c.base.ConsumerStatsResponse = t
	default:
		err = errors.Errorf("unknown command message type: %v", t)
	}

	return
}

func NewBaseWithType(typ *pulsar_proto.BaseCommand_Type) (c *Base) {
	c = new(Base)
	if err := c.SetType(typ); err != nil {
		panic(err)
	}
	return
}

func NewBaseWithMessage(msg proto.Message) (c *Base) {
	c = new(Base)
	if err := c.SetMessage(msg); err != nil {
		panic(err)
	}
	return
}

func NewMarshaledBase(msg proto.Message) (data []byte, err error) {
	data, err = NewBaseWithMessage(msg).Marshal()
	if err != nil {
		err = errors.Wrap(err, "failed to marshal command")
		return
	}
	return
}

func NewBase() (c *Base) {
	c = new(Base)
	return
}
