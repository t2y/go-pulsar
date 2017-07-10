package pulsar

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"gopkg.in/ini.v1"

	"github.com/t2y/go-pulsar/internal/parse"
)

const (
	PROTO_TCP = "tcp"
)

type IniConfig struct {
	LogLevelString string `ini:"log_level"`

	ServiceURLString string        `ini:"service_url"`
	Timeout          time.Duration `ini:"timeout"`
	MinConnectionNum int           `ini:"min_connection_num"`
	MaxConnectionNum int           `ini:"max_connection_num"`

	AuthMethod                 string `ini:"auth_method"`
	AuthParams                 string `ini:"auth_params"`
	UseTLS                     bool   `ini:"use_tls"`
	TLSAllowInsecureConnection bool   `ini:"tls_allow_insecure_connection"`
	TLSTrustCertsFilepath      string `ini:"tls_trust_certs_filepath"`
	AthenzConf                 string `ini:"athenz_conf"`
	AthenzAuthHeader           string `ini:"athenz_auth_header"`

	// internal use
	ServiceURL   *url.URL      `ini:"-"`
	LogLevel     log.Level     `ini:"-"`
	AthenzConfig *AthenzConfig `ini:"-"`
}

type Config struct {
	Proto            string
	LocalAddr        *net.TCPAddr
	RemoteAddr       *net.TCPAddr
	Timeout          time.Duration
	MinConnectionNum int
	MaxConnectionNum int

	AuthMethod                 string
	AuthParams                 map[string]string
	UseTLS                     bool
	TLSAllowInsecureConnection bool
	TLSTrustCertsFilepath      string

	AuthenticationDataProvider AuthenticationDataProvider

	AthenzConfig     *AthenzConfig
	AthenzAuthHeader string

	ServiceURL *url.URL
	LogLevel   log.Level
}

func (c *Config) Copy() (config *Config) {
	// copy Config data except net.TCPAddr, url.URL
	config = &Config{
		Proto:            c.Proto,
		LocalAddr:        nil,
		RemoteAddr:       nil,
		Timeout:          c.Timeout,
		MinConnectionNum: c.MinConnectionNum,
		MaxConnectionNum: c.MaxConnectionNum,

		AuthMethod: c.AuthMethod,
		AuthParams: c.AuthParams,
		UseTLS:     c.UseTLS,
		TLSAllowInsecureConnection: c.TLSAllowInsecureConnection,
		TLSTrustCertsFilepath:      c.TLSTrustCertsFilepath,

		AuthenticationDataProvider: c.AuthenticationDataProvider,

		AthenzConfig:     c.AthenzConfig,
		AthenzAuthHeader: c.AthenzAuthHeader,

		ServiceURL: nil,
		LogLevel:   c.LogLevel,
	}
	return
}

func LoadIniFile(path string) (iniConf *IniConfig, err error) {
	f, err := ini.Load(path)
	if err != nil {
		err = errors.Wrap(err, "failed to load ini")
		return
	}

	iniConf = new(IniConfig)
	if err = f.MapTo(iniConf); err != nil {
		err = errors.Wrap(err, "failed to map ini struct:")
		return
	}

	u, err := url.Parse(iniConf.ServiceURLString)
	if err != nil {
		err = errors.Wrap(err, "failed to parse url")
		return
	}
	iniConf.ServiceURL = u

	if iniConf.UseTLS {
		if iniConf.TLSTrustCertsFilepath != "" {
			if _, err = os.Stat(iniConf.TLSTrustCertsFilepath); !os.IsNotExist(err) {
				path := iniConf.TLSTrustCertsFilepath
				msg := fmt.Sprintf("not found trust certs file: %s", path)
				err = errors.Wrap(err, msg)
				return
			}
		}
	}

	if iniConf.AthenzConf != "" {
		if _, err = os.Stat(iniConf.AthenzConf); !os.IsNotExist(err) {
			msg := fmt.Sprintf("not found athenz conf: %s", iniConf.AthenzConf)
			err = errors.Wrap(err, msg)
			return
		}
	}

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
	}).Info("read and parse ini file")
	return
}

func GetAthenzConfig(path string) (athenzConfig *AthenzConfig, err error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		err = errors.Wrap(err, "failed to read athenz conf file")
		return
	}

	if err = json.Unmarshal(contents, &athenzConfig); err != nil {
		err = errors.Wrap(err, "failed to unmarshal athenz conf")
		return
	}

	return
}

func NewConfigFromIni(iniConf *IniConfig) (c *Config, err error) {
	remoteTcpAddr, err := net.ResolveTCPAddr(PROTO_TCP, iniConf.ServiceURL.Host)
	if err != nil {
		err = errors.Wrap(err, "failed to resolve remote tcp address")
		return
	}

	minConnNum := iniConf.MinConnectionNum
	if minConnNum == 0 {
		minConnNum = defaultMinConnNum
	}
	maxConnNum := iniConf.MaxConnectionNum
	if maxConnNum == 0 {
		maxConnNum = defaultMaxConnNum
	}

	var authParams map[string]string
	if iniConf.AuthParams != "" {
		authParams = parse.ParseAuthParams(iniConf.AuthParams)
	}

	var athenzConfig *AthenzConfig
	if iniConf.AthenzConf != "" {
		if athenzConfig, err = GetAthenzConfig(iniConf.AthenzConf); err != nil {
			err = errors.Wrap(err, "failed to get athenz conf from ini")
			return
		}
	}

	c = &Config{
		Proto:            PROTO_TCP,
		LocalAddr:        nil,
		RemoteAddr:       remoteTcpAddr,
		Timeout:          iniConf.Timeout,
		MinConnectionNum: minConnNum,
		MaxConnectionNum: maxConnNum,

		AuthMethod: iniConf.AuthMethod,
		AuthParams: authParams,
		UseTLS:     iniConf.UseTLS,
		TLSAllowInsecureConnection: iniConf.TLSAllowInsecureConnection,
		TLSTrustCertsFilepath:      iniConf.TLSTrustCertsFilepath,

		AthenzConfig:     athenzConfig,
		AthenzAuthHeader: iniConf.AthenzAuthHeader,

		ServiceURL: iniConf.ServiceURL,
		LogLevel:   iniConf.LogLevel,
	}
	return
}

func NewConfigFromOptions(opts *Options) (c *Config, err error) {
	c = &Config{
		Proto:     PROTO_TCP,
		LocalAddr: nil,
		LogLevel:  log.InfoLevel,
	}

	if opts.ServiceURL != nil {
		var remoteTcpAddr *net.TCPAddr
		remoteTcpAddr, err = net.ResolveTCPAddr(PROTO_TCP, opts.ServiceURL.Host)
		if err != nil {
			err = errors.Wrap(err, "failed to resolve remote tcp address")
			return
		}
		c.RemoteAddr = remoteTcpAddr
		c.ServiceURL = opts.ServiceURL
	}

	if opts.AuthMethod != nil {
		c.AuthMethod = *opts.AuthMethod
	}

	if opts.AuthParams != nil {
		c.AuthParams = parse.ParseAuthParams(*opts.AuthParams)
	}

	if opts.UseTLS {
		c.UseTLS = opts.UseTLS
	}

	if opts.TLSAllowInsecureConnection {
		c.TLSAllowInsecureConnection = opts.TLSAllowInsecureConnection
	}

	if opts.AthenzConf != nil {
		var athenzConfig *AthenzConfig
		if athenzConfig, err = GetAthenzConfig(*opts.AthenzConf); err != nil {
			err = errors.Wrap(err, "failed to get athenz conf from options")
			return
		}
		c.AthenzConfig = athenzConfig
	}

	if opts.AthenzAuthHeader != nil {
		c.AthenzAuthHeader = *opts.AthenzAuthHeader
	}

	if opts.Timeout == nil {
		c.Timeout = DefaultDeadlineTimeout
	} else {
		c.Timeout = *opts.Timeout
	}

	if opts.Verbose {
		c.LogLevel = log.DebugLevel
	}

	return
}
