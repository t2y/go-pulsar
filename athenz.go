package pulsar

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/yahoo/athenz/clients/go/zts"
	"github.com/yahoo/athenz/libs/go/zmssvctoken"
)

type AthenzKey struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type AthenzKeys []AthenzKey

type AthenzConfig struct {
	ZmsUrl        string     `json:"zmsUrl"`
	ZtsUrl        string     `json:"ztsUrl"`
	ZtsPublicKeys AthenzKeys `json:"ztsPublicKeys"`
	ZmsPublicKeys AthenzKeys `json:"zmsPublicKeys"`
}

type AuthenticationDataAthenz struct {
	AuthenticationDataProvider
	roleToken      string
	httpHeaderName string
}

func (a *AuthenticationDataAthenz) HasDataForTls() bool {
	return false
}

func (a *AuthenticationDataAthenz) GetHttpHeaders() (headers http.Header) {
	headers = make(http.Header)
	headers.Set(a.httpHeaderName, a.roleToken)
	return
}

func (a *AuthenticationDataAthenz) HasDataFromCommand() bool {
	return true
}

func (a *AuthenticationDataAthenz) GetCommandData() string {
	return a.roleToken
}

func NewAuthenticationDataAthenz(
	roleToken, httpHeaderName string,
) (provider *AuthenticationDataAthenz) {
	defaultProvider := &defaultAuthenticationDataProvider{}
	provider = &AuthenticationDataAthenz{
		defaultProvider, roleToken, httpHeaderName,
	}
	return
}

const (
	defaultKeyVersion = "0"
	defaultExpiration = 60 * time.Minute
)

var (
	defaultRoleTokenMinExpiryTime int32 = 2 * 60 * 60
	defaultRoleTokenMaxExpiryTime int32 = 24 * 60 * 60
)

type AuthenticationAthenz struct {
	authParams map[string]string
	config     *Config
}

func (a *AuthenticationAthenz) GetAuthMethodName() (name string) {
	name = "athenz" // never use "AuthMethodAthens" in PulsarApi.pb.go
	return
}

func (a *AuthenticationAthenz) Configure(authParams map[string]string) {
	a.authParams = authParams
}

func (a *AuthenticationAthenz) Start() (err error) {
	return
}

func (a *AuthenticationAthenz) GetAuthData() (
	provider AuthenticationDataProvider, err error,
) {
	privateKeyPath := a.authParams["privateKeyPath"]
	domain := a.authParams["tenantDomain"]
	service := a.authParams["tenantService"]
	keyVersion, ok := a.authParams["keyId"]
	if !ok {
		keyVersion = defaultKeyVersion
	}

	var ntoken string
	ntoken, err = GetNToken(
		privateKeyPath, domain, service, keyVersion, defaultExpiration,
	)
	if err != nil {
		err = errors.Wrap(err, "failed to get ntoken")
		return
	}

	ztsUrl := a.config.AthenzConfig.ZtsUrl + "/zts/v1"
	providerDomain := a.authParams["providerDomain"]

	var roleToken *zts.RoleToken
	roleToken, err = GetRoleToken(
		ztsUrl, a.config.AthenzAuthHeader, ntoken, domain, providerDomain, service, "",
	)
	if err != nil {
		err = errors.Wrap(err, "failed to get role token")
		return
	}

	provider = NewAuthenticationDataAthenz(
		roleToken.Token, a.config.AthenzAuthHeader,
	)
	return
}

func GetNToken(
	privateKeyPath, domain, service, keyVersion string, expireTime time.Duration,
) (ntoken string, err error) {
	var bytes []byte
	bytes, err = ioutil.ReadFile(privateKeyPath)
	if err != nil {
		err = errors.Wrap(err, "failed to read private key file")
		return
	}

	var builder zmssvctoken.TokenBuilder
	builder, err = zmssvctoken.NewTokenBuilder(domain, service, bytes, keyVersion)
	if err != nil {
		err = errors.Wrap(err, "failed to create token builder")
		return
	}
	builder.SetExpiration(expireTime)

	token := builder.Token()
	ntoken, err = token.Value()
	if err != nil {
		err = errors.Wrap(err, "failed to get ntoken")
		return
	}

	log.WithFields(log.Fields{
		"ntoken": ntoken,
	}).Debug("got ntoken")
	return
}

func GetRoleToken(
	url, authHeader, ntoken, tenantDomain, providerDomain, service, role string,
) (roleToken *zts.RoleToken, err error) {
	client := zts.NewClient(url, nil)
	client.AddCredentials(authHeader, ntoken)
	roleToken, err = client.GetRoleToken(
		zts.DomainName(providerDomain), zts.EntityList(role),
		&defaultRoleTokenMinExpiryTime, &defaultRoleTokenMaxExpiryTime, "",
	)
	if err != nil {
		err = errors.Wrap(err, "failed to get role token")
		return
	}

	log.WithFields(log.Fields{
		"roleToken": roleToken,
	}).Debug("got role token")
	return
}

func NewAuthenticationAthenz(config *Config) (athenz *AuthenticationAthenz) {
	athenz = &AuthenticationAthenz{
		config: config,
	}
	return
}
