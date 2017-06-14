package pulsar

import (
	"net/url"
	"time"

	"github.com/pkg/errors"
)

const (
	OptionsCommandConsume = "consume"
	OptionsCommandProduce = "produce"
)

type Options struct {
	// common options
	URLString  *string        `long:"url" env:"PULSAR_URL" description:"pulsar blocker url"`
	AuthParams *string        `long:"authParams" env:"PULSAR_AUTH_PARAMS" description:"authentication params"`
	AuthMethod *string        `long:"authMethod" env:"PULSAR_AUTH_METHOD" description:"authentication method"`
	Conf       *string        `long:"conf" env:"PULSAR_CONF" description:"path to pulsar config file"`
	Verbose    bool           `long:"verbose" env:"VERBOSE" description:"use verbose mode"`
	Timeout    *time.Duration `long:"timeout" env:"PULSAR_TIMEOUT" description:"timeout to communicate with pulsar broker"`

	Command *string `long:"command" env:"PULSAR_COMMAND" description:"produce or consume"`
	Topic   string  `long:"topic" env:"PULSAR_TOPIC" required:"true" description:"topic name"`

	// for producer
	Messages   []string `long:"messages" env:"PULSAR_MESSAGES" description:"messages to produce"`
	Properties []string `long:"properties" env:"PULSAR_PROPERTIES" description:"properties to produce. e.g) key1:value1,key2:value2"`

	// for consumer
	NumMessages      int    `long:"numMessages" env:"PULSAR_NUM_MESSAGES" description:"number of messages to consume"`
	SubscriptionName string `long:"subscriptionName" env:"PULSAR_SUBSCRIPTION_NAME" description:"subscription name"`
	SubscriptionType string `long:"subscriptionType" env:"PULSAR_SUBSCRIPTION_TYPE" default:"exclusive" description:"subscription type: exclusive, shared, failover"`

	// internal use
	URL *url.URL
}

func InitOptions(opts *Options) (err error) {
	if opts.URLString != nil {
		var u *url.URL
		u, err = url.Parse(*opts.URLString)
		if err != nil {
			err = errors.Wrap(err, "failed to parse url")
			return
		}
		opts.URL = u
	}

	return
}
