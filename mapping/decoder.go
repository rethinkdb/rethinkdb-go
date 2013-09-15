// This file is based on the github.com/gorilla/schema package

// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mapping

import (
	"errors"
	"fmt"
	"reflect"
)

// NewDecoder returns a new Decoder.
func NewDecoder() *Decoder {
	return &Decoder{cache: newCache()}
}

// Decoder decodes values from a map[string][]string to a struct.
type Decoder struct {
	cache *fieldCache
}

// RegisterConverter registers a converter function for a custom type.
func (d *Decoder) RegisterConverter(value interface{}, converterFunc Converter) {
	d.cache.conv[reflect.TypeOf(value).Kind()] = converterFunc
}

// Decode decodes a map[string][]string to a struct.
//
// The first parameter must be a pointer to a struct.
//
// The second parameter is a map, typically url.Values from an HTTP request.
// Keys are "paths" in dotted notation to the struct fields and nested structs.
//
// See the package documentation for a full explanation of the mechanics.
func (d *Decoder) Decode(dst interface{}, src interface{}) error {
	dv := reflect.ValueOf(dst)
	sv := reflect.ValueOf(src)

	// Ensure that the destination is a pointer
	if dv.Kind() != reflect.Ptr {
		return errors.New("schema: destination must be a pointer")
	}
	dv = dv.Elem()
	return d.decode(dv, sv)
}

func (d *Decoder) decode(dv, sv reflect.Value) error {
	if !dv.IsValid() || !sv.IsValid() {
		return fmt.Errorf("Value is invalid")
	}

	switch sv.Kind() {
	case reflect.Interface:
		return d.decode(dv, reflect.ValueOf(sv.Interface()))
	case reflect.Slice, reflect.Array:
		return d.decodeArray(dv, sv)
	case reflect.Struct, reflect.Map:
		return d.decodeObject(dv, sv)
	default:
		return d.decodeLiteral(dv, sv)
	}
}

func (d *Decoder) decodeLiteral(dv reflect.Value, sv reflect.Value) error {
	dv = d.indirect(dv)
	dt := dv.Type()

	if conv := d.cache.conv[dt.Kind()]; conv != nil {
		if value := conv(sv.Interface()); value.IsValid() {
			dv.Set(value)
		} else {
			return ConversionError{Index: -1}
		}
	} else {
		return fmt.Errorf("schema: converter not found for %v", dt)
	}

	return nil
}

func (d *Decoder) decodeArray(dv reflect.Value, sv reflect.Value) error {
	dv = d.indirect(dv)
	dt := dv.Type()

	if dv.Kind() == reflect.Slice {
		dv.Set(reflect.MakeSlice(dt, 0, 0))
	}

	for i := 0; i < sv.Len(); i++ {
		if dv.Kind() == reflect.Slice {
			// Get element of array, growing if necessary.
			if i >= dv.Cap() {
				newcap := dv.Cap() + dv.Cap()/2
				if newcap < 4 {
					newcap = 4
				}
				newdv := reflect.MakeSlice(dv.Type(), dv.Len(), newcap)
				reflect.Copy(newdv, dv)
				dv.Set(newdv)
			}
			if i >= dv.Len() {
				dv.SetLen(i + 1)
			}
		}

		err := d.decode(dv.Index(i), sv.Index(i))
		if err != nil {
			return err
		}
	}

	return nil
}

// decode fills a struct field using a parsed path.
func (d *Decoder) decodeObject(dv reflect.Value, sv reflect.Value) error {
	dv = d.indirect(dv)
	dt := dv.Type()

	if dv.Kind() == reflect.Map {
		// map must have string kind
		if dt.Key().Kind() != reflect.String {
			// d.saveError(&UnmarshalTypeError{"object", dv.Type()})
			return fmt.Errorf("Map key not string...")
		}

		if dv.IsNil() {
			dv.Set(reflect.MakeMap(dt))
		}
	}

	var mapElem reflect.Value

	for _, key := range sv.MapKeys() {
		var subdv reflect.Value
		var subsv reflect.Value = sv.MapIndex(key)

		if dv.Kind() == reflect.Map {
			elemType := dv.Type().Elem()
			if !mapElem.IsValid() {
				mapElem = reflect.New(elemType).Elem()
			} else {
				mapElem.Set(reflect.Zero(elemType))
			}
			subdv = mapElem
		} else {
			var f *field
			fields := d.cache.typeFields(dv.Type())
			for i := range fields {
				ff := &fields[i]
				if ff.name == key.Interface().(string) {
					f = ff
					break
				}
			}
			if f != nil {
				subdv = dv
				for _, i := range f.index {
					if subdv.Kind() == reflect.Ptr {
						if subdv.IsNil() {
							subdv.Set(reflect.New(subdv.Type().Elem()))
						}
						subdv = subdv.Elem()
					}
					subdv = subdv.Field(i)
				}
			}
		}

		err := d.decode(subdv, subsv)
		if err != nil {
			return err
		}

		if dv.Kind() == reflect.Map {
			kv := reflect.ValueOf(key).Convert(dv.Type().Key())
			dv.SetMapIndex(kv, subdv)
		}
	}

	return nil
}

// indirect walks down v allocating pointers as needed,
// until it gets to a non-pointer.
func (d *Decoder) indirect(v reflect.Value) reflect.Value {
	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && e.Elem().Kind() == reflect.Ptr {
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	return v
}

// Errors ---------------------------------------------------------------------

// ConversionError stores information about a failed conversion.
type ConversionError struct {
	Key   string // key from the source map.
	Index int    // index for multi-value fields; -1 for single-value fields.
}

func (e ConversionError) Error() string {
	if e.Index < 0 {
		return fmt.Sprintf("schema: error converting value for %q", e.Key)
	}
	return fmt.Sprintf("schema: error converting value for index %d of %q",
		e.Index, e.Key)
}

// MultiError stores multiple decoding errors.
//
// Borrowed from the App Engine SDK.
type MultiError map[string]error

func (e MultiError) Error() string {
	s := ""
	for _, err := range e {
		s = err.Error()
		break
	}
	switch len(e) {
	case 0:
		return "(0 errors)"
	case 1:
		return s
	case 2:
		return s + " (and 1 other error)"
	}
	return fmt.Sprintf("%s (and %d other errors)", s, len(e)-1)
}
