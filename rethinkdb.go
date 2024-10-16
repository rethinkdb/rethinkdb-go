package rethinkdb

import (
	"log/slog"
	"reflect"

	"gopkg.in/rethinkdb/rethinkdb-go.v6/encoding"
)

const (
	SystemDatabase = "rethinkdb"

	TableConfigSystemTable   = "table_config"
	ServerConfigSystemTable  = "server_config"
	DBConfigSystemTable      = "db_config"
	ClusterConfigSystemTable = "cluster_config"
	TableStatusSystemTable   = "table_status"
	ServerStatusSystemTable  = "server_status"
	CurrentIssuesSystemTable = "current_issues"
	UsersSystemTable         = "users"
	PermissionsSystemTable   = "permissions"
	JobsSystemTable          = "jobs"
	StatsSystemTable         = "stats"
	LogsSystemTable          = "logs"
)

func init() {
	// Set encoding package
	encoding.IgnoreType(reflect.TypeOf(Term{}))
}

// SetTags allows you to override the tags used when decoding or encoding
// structs. The driver will check for the tags in the same order that they were
// passed into this function. If no parameters are passed then the driver will
// default to checking for the rethinkdb tag (the rethinkdb tag is always included)
// Old-style gorethink tag is also supported but deprecated
func SetTags(tags ...string) {
	encoding.Tags = append(tags, encoding.TagName, encoding.OldTagName)
}

// SetVerbose allows the driver logging level to be set. If true is passed then
// the log level is set to Debug otherwise it defaults to Info.
func SetVerbose(verbose bool) {
	if verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		return
	}

	slog.SetLogLoggerLevel(slog.LevelInfo)
}
