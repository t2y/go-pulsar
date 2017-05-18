package pulsar

import (
	"net"

	"github.com/pkg/errors"
	"gopkg.in/ini.v1"
)

const (
	PROTO_TCP = "tcp"
)

type IniConfig struct {
	LocalAddress  string `ini:"local_address"`
	RemoteAddress string `ini:"remote_address"`

	LogLevel string `ini:"log_level"`
}

type Config struct {
	Proto      string
	LocalAddr  *net.TCPAddr
	RemoteAddr *net.TCPAddr

	LogLevel string
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

	c, err = NewConfig(iniConf)
	return
}

func NewConfig(iniConf *IniConfig) (c *Config, err error) {
	localTcpAddr, err := net.ResolveTCPAddr(PROTO_TCP, iniConf.LocalAddress)
	if err != nil {
		err = errors.Wrap(err, "failed to resolve local tcp address")
		return
	}

	remoteTcpAddr, err := net.ResolveTCPAddr(PROTO_TCP, iniConf.RemoteAddress)
	if err != nil {
		err = errors.Wrap(err, "failed to resolve remote tcp address")
		return
	}

	c = &Config{
		Proto:      PROTO_TCP,
		LocalAddr:  localTcpAddr,
		RemoteAddr: remoteTcpAddr,

		LogLevel: iniConf.LogLevel,
	}
	return
}
