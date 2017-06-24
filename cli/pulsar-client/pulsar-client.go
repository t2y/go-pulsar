package main

import (
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	flags "github.com/jessevdk/go-flags"

	pulsar "github.com/t2y/go-pulsar"
	"github.com/t2y/go-pulsar/proto/command"
	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})
	log.SetOutput(os.Stdout)
}

var opts pulsar.Options

func mergeConfig(c *pulsar.Config, opts *pulsar.Options) {
	if opts.URL != nil {
		c.URL = opts.URL
	}
	if opts.Verbose {
		c.LogLevel = log.DebugLevel
		log.SetLevel(log.DebugLevel)
	}
}

func getConfig(opts *pulsar.Options) (c *pulsar.Config) {
	var err error
	if opts.Conf == nil {
		c, err = pulsar.NewConfigFromOptions(opts)
		if err != nil {
			log.WithFields(log.Fields{
				"opts": opts,
				"err":  err,
			}).Fatal("Failed to initialize config")
		}
	} else {
		var iniConf *pulsar.IniConfig
		iniConf, err = pulsar.LoadIniFile(*opts.Conf)
		if err != nil {
			log.WithFields(log.Fields{
				"conf": opts.Conf,
				"err":  err,
			}).Fatal("Failed to load ini file")
		}
		c, err = pulsar.NewConfigFromIni(iniConf)
		if err != nil {
			log.WithFields(log.Fields{
				"conf":    opts.Conf,
				"iniConf": iniConf,
				"err":     err,
			}).Fatal("Failed to initialize config")
		}
		mergeConfig(c, opts)
	}
	return
}

func getClient(opts *pulsar.Options) (client *pulsar.PulsarClient) {
	config := getConfig(opts)
	tc, err := pulsar.NewTcpConn(config)
	if err != nil {
		log.WithFields(log.Fields{
			"config": config,
			"err":    err,
		}).Fatal("Failed to create tcp connection")
	}

	ac := pulsar.NewAsyncTcpConn(config, tc)
	client = pulsar.NewClient(ac)
	if err != nil {
		log.WithFields(log.Fields{
			"opts": opts,
			"err":  err,
		}).Fatal("Failed to initialize client")
	}

	connect := &pulsar_proto.CommandConnect{
		ClientVersion:   proto.String(pulsar.ClientName),
		AuthMethod:      pulsar_proto.AuthMethod_AuthMethodNone.Enum(),
		ProtocolVersion: proto.Int32(pulsar.DefaultProtocolVersion),
	}

	err = client.Connect(connect)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to connect to a broker")
	}

	time.Sleep(1 * time.Second) // waiting for connected command
	client.Receive()

	return
}

func parseKeyValues(property string) (keyValues pulsar.KeyValues) {
	prop := strings.Split(property, ",")
	keyValues = make(pulsar.KeyValues, 0, len(prop))
	for _, kv := range prop {
		keyValue := strings.Split(kv, ":")
		keyValues = append(keyValues, pulsar.KeyValue{
			Key: keyValue[0], Value: keyValue[1],
		})
	}

	return
}

func parseProperties(messages, properties []string) (batchMessage command.BatchMessage) {
	propertiesLength := len(properties)
	batchMessage = make(command.BatchMessage, len(opts.Messages))
	for i, payload := range messages {
		var keyValues pulsar.KeyValues
		if i < propertiesLength {
			keyValues = parseKeyValues(properties[i])
		}

		single := &pulsar_proto.SingleMessageMetadata{
			PayloadSize: proto.Int32(int32(len(payload))),
		}
		if keyValues != nil {
			single.Properties = keyValues.Convert()
		}

		batchMessage[payload] = single
	}

	return
}

func runProduceCommand(opts *pulsar.Options, client *pulsar.PulsarClient) {
	var (
		producerId uint64 = 1
		requestId  uint64 = 100
		sequenceId uint64 = 0
	)

	producer := pulsar.NewProducer(client)
	if err := producer.CreateProcuder(opts.Topic, producerId, requestId); err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to send producer")
	}
	defer producer.Close()

	success, err := producer.ReceiveProducerSuccess()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to create producer")
	}

	if len(opts.Messages) > 1 {
		batchMessage := parseProperties(opts.Messages, opts.Properties)
		producer.SendBatchSend(
			producerId, sequenceId, success.GetProducerName(),
			batchMessage, pulsar_proto.CompressionType_NONE.Enum(),
		)
	} else {
		var keyValues pulsar.KeyValues
		if len(opts.Properties) > 0 {
			keyValues = parseKeyValues(opts.Properties[0])
		}
		producer.SendSend(
			producerId, sequenceId, success.GetProducerName(),
			opts.Messages[0], keyValues,
		)
	}

	_, err = producer.ReceiveSendReceipt()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to receive sendReceipt")
	}

	producer.CloseProducer(producerId, requestId)
	_, err = client.ReceiveSuccess()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to receive success")
	}

	log.WithFields(log.Fields{
		"messages":   opts.Messages,
		"properties": opts.Properties,
	}).Info("messages successfully produced")
}

func receiveSubscribeMessage(
	consumer *pulsar.Consumer, consumerId uint64,
) {
	ackType := pulsar_proto.CommandAck_Individual

	msg, err := consumer.ReceiveMessage()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("failed to receive message in receiveSubscribeMessage")
	}

	idData := msg.GetMessageId()
	consumer.SendAck(consumerId, ackType, idData, nil)

	if msg.HasBatchMessage() {
		batchMessage := msg.GetBatchMessage()
		for body, single := range batchMessage {
			properties := single.GetProperties()
			keyValues := pulsar.ConvertPropertiesToKeyValues(properties)
			log.WithFields(log.Fields{
				"message":    body,
				"properties": keyValues,
			}).Info("messages successfully consumed")
		}
	} else {
		log.WithFields(log.Fields{
			"message":    msg.GetBody(),
			"properties": msg.GetKeyValues(),
		}).Info("messages successfully consumed")
	}
}

func runConsumeCommand(opts *pulsar.Options, client *pulsar.PulsarClient) {
	var (
		consumerId     uint64 = 1
		requestId      uint64 = 200
		messagePermits uint32 = 10
	)
	consumer := pulsar.NewConsumer(client)
	defer consumer.Close()

	subsTypeName := strings.Title(opts.SubscriptionType)
	subsTypeValue := pulsar_proto.CommandSubscribe_SubType_value[subsTypeName]
	subsType := pulsar_proto.CommandSubscribe_SubType(subsTypeValue)

	err := consumer.Subscribe(
		opts.Topic, opts.SubscriptionName, subsType, consumerId, requestId,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to subscribe")
	}

	success, err := client.ReceiveSuccess()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to receive success command")
	}
	log.Debug(success)

	consumer.Flow(consumerId, messagePermits)

	if opts.NumMessages == 0 {
		for {
			receiveSubscribeMessage(consumer, consumerId)
		}
	} else {
		for i := 0; i < opts.NumMessages; i++ {
			receiveSubscribeMessage(consumer, consumerId)
		}
	}

	consumer.CloseConsumer(consumerId, requestId)
	client.ReceiveSuccess()
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		os.Exit(1)
	}

	if opts.Verbose {
		log.SetLevel(log.DebugLevel)
	}

	if err := pulsar.InitOptions(&opts); err != nil {
		log.WithFields(log.Fields{
			"opts": opts,
			"err":  err,
		}).Fatal("Failed to initialize options")
	}

	client := getClient(&opts)
	defer client.Close()

	if *opts.Command == pulsar.OptionsCommandProduce {
		if len(opts.Messages) == 0 {
			log.WithFields(log.Fields{
				"opts": opts,
			}).Fatal("Need messages to produece")
		}
		runProduceCommand(&opts, client)
	} else if *opts.Command == pulsar.OptionsCommandConsume {
		runConsumeCommand(&opts, client)
	}
}
