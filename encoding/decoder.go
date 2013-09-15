// This code is based on encoding/json and gorilla/schema

package encoding

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
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
	if dv.IsValid() && sv.IsValid() {
		// Ensure that the source value has the correct type of parsing
		if sv.Kind() == reflect.Interface {
			sv = reflect.ValueOf(sv.Interface())
		}

		switch sv.Kind() {
		case reflect.Slice, reflect.Array:
			return d.decodeArray(dv, sv)
		case reflect.Struct, reflect.Map:
			return d.decodeObject(dv, sv)
		default:
			return d.decodeLiteral(dv, sv)
		}
	}

	return nil
}

func (d *Decoder) decodeLiteral(dv reflect.Value, sv reflect.Value) error {
	dv = d.indirect(dv)
	dt := dv.Type()

	if dv.Kind() == reflect.Interface {
		dv.Set(reflect.ValueOf(d.decodeLiteralInterface(sv)))
		return nil
	}

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

	if dt.Kind() == reflect.Interface {
		if dv.NumMethod() == 0 {
			// Decoding into nil interface?  Switch to non-reflect code.
			dv.Set(reflect.ValueOf(d.decodeArrayInterface(sv)))

			return nil
		} else {
			return nil
		}
	}

	if dv.Kind() == reflect.Slice {
		dv.Set(reflect.MakeSlice(dt, 0, 0))
	}

	i := 0
	for i < sv.Len() {
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

		if i < dv.Len() {
			// Decode into element.
			err := d.decode(dv.Index(i), sv.Index(i))
			if err != nil {
				return err
			}
		} else {
			// Ran out of fixed array: skip.
			err := d.decode(reflect.Value{}, sv.Index(i))
			if err != nil {
				return err
			}
		}

		i++
	}
	if i < dv.Len() {
		if dv.Kind() == reflect.Array {
			// Array.  Zero the rest.
			z := reflect.Zero(dv.Type().Elem())
			for ; i < dv.Len(); i++ {
				dv.Index(i).Set(z)
			}
		} else {
			dv.SetLen(i)
		}
	}
	if i == 0 && dv.Kind() == reflect.Slice {
		dv.Set(reflect.MakeSlice(dv.Type(), 0, 0))
	}

	return nil
}

// decode fills a struct field using a parsed path.
func (d *Decoder) decodeObject(dv reflect.Value, sv reflect.Value) error {
	dv = d.indirect(dv)
	dt := dv.Type()

	// Decoding into nil interface?  Switch to non-reflect code.
	if dv.Kind() == reflect.Interface && dv.NumMethod() == 0 {
		dv.Set(reflect.ValueOf(d.decodeObjectInterface(sv)))
		return nil
	}

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

		skey := key.Interface().(string)

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
				if ff.name == skey {
					f = ff
					break
				}
				if f == nil && strings.EqualFold(ff.name, skey) {
					f = ff
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
			kv := reflect.ValueOf(skey)
			dv.SetMapIndex(kv, subdv)
		}
	}

	return nil
}

// The xxxInterface routines build up a value to be stored
// in an empty interface.  They are not strictly necessary,
// but they avoid the weight of reflection in this common case.

// valueInterface is like value but returns interface{}
func (d *Decoder) decodeInterface(sv reflect.Value) interface{} {
	switch sv.Kind() {
	case reflect.Array, reflect.Slice:
		return d.decodeArrayInterface(sv)
	case reflect.Struct, reflect.Map:
		return d.decodeObjectInterface(sv)
	default:
		return d.decodeLiteralInterface(sv)
	}
}

// arrayInterface is like array but returns []interface{}.
func (d *Decoder) decodeArrayInterface(sv reflect.Value) []interface{} {
	var arr = make([]interface{}, 0)
	for _, v := range sv.Interface().([]interface{}) {
		arr = append(arr, d.decodeInterface(reflect.ValueOf(v)))
	}
	return arr
}

// objectInterface is like object but returns map[string]interface{}.
func (d *Decoder) decodeObjectInterface(sv reflect.Value) map[string]interface{} {
	m := make(map[string]interface{})
	for k, v := range sv.Interface().(map[interface{}]interface{}) {
		// Ensure that key is of type string
		if key, ok := k.(string); ok {
			m[key] = d.decodeInterface(reflect.ValueOf(v))
		}
	}
	return m
}

// literalInterface is like literal but returns an interface value.
func (d *Decoder) decodeLiteralInterface(sv reflect.Value) interface{} {
	return sv.Interface()
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
