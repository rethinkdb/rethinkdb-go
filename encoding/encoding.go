package encoding

import "reflect"

type EncodeHook func(src reflect.Value) (success bool, ret reflect.Value, err error)

var encodeHooks []EncodeHook

func init() {
	encodeHooks = []EncodeHook{}
}

func RegisterEncodeHook(hook EncodeHook) {
	encodeHooks = append(encodeHooks, hook)
}
