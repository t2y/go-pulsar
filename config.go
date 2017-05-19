package pulsar

import (
	"net"
	"net/url"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"gopkg.in/ini.v1"
)

const (
	PROTO_TCP = "tcp"
)

type IniConfig struct {
	LogLevelString string `ini:"log_level"`

	URLString string        `ini:"url"`
	Timeout   time.Duration `ini:"timeout"`

	// internal use
	URL      *url.URL  `ini:"-"`
	LogLevel log.Level `ini:"-"`
}

type Config struct {
	Proto      string
	LocalAddr  *net.TCPAddr
	RemoteAddr *net.TCPAddr
	Timeout    time.Duration

	URL      *url.URL
	LogLevel log.Level
}

func ReadIniFile(path string) (c *Config, err error) {
	f, err := ini.Load(path)
	if err != nil {
		err = errors.Wrap(err, "failed to load ini")
		return
	}

	iniConf := new(IniConfig)
	if err = f.MapTo(iniConf); err != nil {
		err = errors.Wrap(err, "failed to map ini struct:")
		return
	}

	u, err := url.Parse(iniConf.URLString)
	if err != nil {
		err = errors.Wrap(err, "failed to parse url")
		return
	}
	iniConf.URL = u

	level, err := log.ParseLevel(iniConf.LogLevelString)
	if err != nil {
		err = errors.Wrap(err, "failed to parse log level")
		return
	}
	iniConf.LogLevel = level
	log.SetLevel(level)

	log.WithFields(log.Fields{
		"path":    path,
		"iniConf": iniConf,
	}).Debug("read and parse ini file")

	c, err = NewConfig(iniConf)
	return
}

func NewConfig(iniConf *IniConfig) (c *Config, err error) {
	remoteTcpAddr, err := net.ResolveTCPAddr(PROTO_TCP, iniConf.URL.Host)
	if err != nil {
		err = errors.Wrap(err, "failed to resolve remote tcp address")
		return
	}

	c = &Config{
		Proto:      PROTO_TCP,
		LocalAddr:  nil,
		RemoteAddr: remoteTcpAddr,
		Timeout:    iniConf.Timeout,

		URL:      iniConf.URL,
		LogLevel: iniConf.LogLevel,
	}
	return
}
