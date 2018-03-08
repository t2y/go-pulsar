package pulsar

import (
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	// default logger configuration
	log.SetFormatter(&log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})
}
