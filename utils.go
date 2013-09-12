package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"strings"
)

func enforceArgLength(min, max int, args []interface{}) {
	if max == 0 {
		max = len(args)
	}

	if len(args) < min || len(args) > max {
		panic("Function has incorrect number of arguments")
	}
}

func mergeArgs(args ...interface{}) []interface{} {
	newArgs := []interface{}{}

	for _, arg := range args {
		switch v := arg.(type) {
		case []interface{}:
			newArgs = append(newArgs, v...)
		default:
			newArgs = append(newArgs, v)
		}
	}

	return newArgs
}

func protoStringOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

func protoInt64OrNil(i int64) *int64 {
	if i == 0 {
		return nil
	}
	return proto.Int64(i)
}

func prefixLines(s string, prefix string) (result string) {
	for _, line := range strings.Split(s, "\n") {
		result += prefix + line + "\n"
	}
	return
}

func protobufToString(p proto.Message, indentLevel int) string {
	return prefixLines(proto.MarshalTextString(p), strings.Repeat("    ", indentLevel))
}
