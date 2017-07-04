package pulsar

import (
	"net/http"

	"github.com/pkg/errors"
)

type Certificate struct {
}

type PrivateKey interface {
}

type AuthenticationDataProvider interface {
	HasDataForTls() bool
	GetTlsCertificates() []Certificate
	GetTlsPrivateKey() PrivateKey
	HasDataForHttp() bool
	GetHttpAuthType() string
	GetHttpHeaders() http.Header
	HasDataFromCommand() bool
	GetCommandData() string
}

type defaultAuthenticationDataProvider struct {
}

func (p *defaultAuthenticationDataProvider) HasDataForTls() bool {
	return false
}

func (p *defaultAuthenticationDataProvider) GetTlsCertificates() []Certificate {
	return nil
}

func (p *defaultAuthenticationDataProvider) GetTlsPrivateKey() PrivateKey {
	return nil
}

func (p *defaultAuthenticationDataProvider) HasDataForHttp() bool {
	return false
}

func (p *defaultAuthenticationDataProvider) GetHttpAuthType() string {
	return ""
}

func (p *defaultAuthenticationDataProvider) GetHttpHeaders() http.Header {
	return nil
}

func (p *defaultAuthenticationDataProvider) HasDataFromCommand() bool {
	return false
}

func (p *defaultAuthenticationDataProvider) GetCommandData() string {
	return ""
}

type Authentication interface {
	GetAuthMethodName() string
	GetAuthData() (AuthenticationDataProvider, error)
	Configure(map[string]string)
	Start() error
}

func NewAuthentication(name string, config *Config) (auth Authentication, err error) {
	if name == OptionsAuthMethodAthenz {
		auth = NewAuthenticationAthenz(config)
	} else {
		err = errors.Errorf("unsupport auth method: %s", name)
	}
	return
}
