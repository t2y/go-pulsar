package pulsar

import (
	"time"

	log "github.com/Sirupsen/logrus"
)

func init() {
	// default logger configuration
	log.SetFormatter(&log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})
}
