package pulsar

import (
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"

	pulsar_proto "github.com/t2y/go-pulsar/proto/pb"
)

func NewCommandConnect(
	c *Config, useCache bool,
) (connect *pulsar_proto.CommandConnect, err error) {
	connect = &pulsar_proto.CommandConnect{
		ClientVersion:   proto.String(ClientName),
		AuthMethod:      pulsar_proto.AuthMethod_AuthMethodNone.Enum(),
		ProtocolVersion: proto.Int32(DefaultProtocolVersion),
	}

	if c.AuthMethod != "" {
		var auth Authentication
		auth, err = NewAuthentication(c.AuthMethod, c)
		if err != nil {
			log.WithFields(log.Fields{
				"config": c,
				"err":    err,
			}).Fatal("Failed to initialize authentication")
		}

		authMethodName := auth.GetAuthMethodName()
		connect.AuthMethod = pulsar_proto.AuthMethod(
			pulsar_proto.AuthMethod_value[authMethodName],
		).Enum()
		connect.AuthMethodName = proto.String(authMethodName)

		if useCache && c.AuthenticationDataProvider != nil {
			authData := c.AuthenticationDataProvider.GetCommandData()
			connect.AuthData = []byte(authData)
			log.Debug("use cached auth data")
			return
		}

		auth.Configure(c.AuthParams)
		err = auth.Start()
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Fatal("Failed to start authentication")
		}

		c.AuthenticationDataProvider, err = auth.GetAuthData()
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Fatal("Failed to get authentication data provider")
		}

		authData := c.AuthenticationDataProvider.GetCommandData()
		connect.AuthData = []byte(authData)
	}

	return
}
