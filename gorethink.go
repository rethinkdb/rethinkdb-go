package gorethink

import (
	"github.com/Sirupsen/logrus"
	"reflect"

	"github.com/dancannon/gorethink/encoding"
)

var (
	log *logrus.Logger
)

func init() {
	// Set encoding package
	encoding.IgnoreType(reflect.TypeOf(Term{}))

	log = logrus.New()
}

func SetDebug(debug bool) {
	level := logrus.InfoLevel
	if debug {
		level = logrus.DebugLevel
	}

	log.Level = level
}
