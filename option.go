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
	URLString  *string        `long:"url" env:"PULSAR_URL" description:"pulsar blocker url"`
	AuthParams *string        `long:"authParams" env:"PULSAR_AUTH_PARAMS" description:"authentication params"`
	AuthPlugin *string        `long:"authPlugin" env:"PULSAR_AUTH_PLUGIN" description:"authentication plugin"`
	Command    *string        `long:"command" env:"PULSAR_COMMAND" description:"produce or consume"`
	Conf       *string        `long:"conf" env:"PULSAR_CONF" description:"path to pulsar config file"`
	Timeout    *time.Duration `long:"timeout" env:"PULSAR_TIMEOUT" description:"timeout to communicate pulsar broker"`
	Verbose    bool           `long:"verbose" env:"VERBOSE" description:"use verbose mode"`

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
