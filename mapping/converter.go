// This file is based on the github.com/gorilla/schema package

// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mapping

import (
	"reflect"
)

type Converter func(interface{}) reflect.Value

// Default converters for basic types.
var converters = map[reflect.Kind]Converter{
	reflect.Interface: convertInterface,
	reflect.Bool:      convertBool,
	reflect.Float32:   convertFloat32,
	reflect.Float64:   convertFloat64,
	reflect.Int:       convertInt,
	reflect.Int8:      convertInt8,
	reflect.Int16:     convertInt16,
	reflect.Int32:     convertInt32,
	reflect.Int64:     convertInt64,
	reflect.String:    convertString,
	reflect.Uint:      convertUint,
	reflect.Uint8:     convertUint8,
	reflect.Uint16:    convertUint16,
	reflect.Uint32:    convertUint32,
	reflect.Uint64:    convertUint64,
}

func convertInterface(value interface{}) reflect.Value {
	return reflect.ValueOf(value)
}

func convertBool(value interface{}) reflect.Value {
	switch val := value.(type) {
	case bool:
		return reflect.ValueOf(val)
	default:
		return reflect.Value{}
	}
}

func convertFloat32(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(float32(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(float32(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(float32(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertFloat64(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(float64(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(float64(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(float64(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertInt(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertInt8(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int8(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int8(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int8(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertInt16(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int16(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int16(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int16(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertInt32(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int32(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int32(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int32(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertInt64(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int64(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int64(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(int64(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertString(value interface{}) reflect.Value {
	switch val := value.(type) {
	case string:
		return reflect.ValueOf(val)
	default:
		return reflect.Value{}
	}
}

func convertUint(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertUint8(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint8(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint8(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint8(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertUint16(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint16(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint16(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint16(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertUint32(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint32(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint32(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint32(rv.Float()))
	default:
		return reflect.Value{}
	}
}

func convertUint64(value interface{}) reflect.Value {
	switch val := value.(type) {
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint64(rv.Uint()))
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint64(rv.Int()))
	case float32, float64:
		rv := reflect.ValueOf(val)
		return reflect.ValueOf(uint64(rv.Float()))
	default:
		return reflect.Value{}
	}
}
