// This code is based on encoding/json and gorilla/schema

package encoding

import (

	// "errors"
	"reflect"
	// "runtime"
	"strconv"
	"strings"
)

// Decode decodes map[string]interface{} into a struct. The first parameter
// must be a pointer.
func Decode(dst interface{}, src interface{}) (err error) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		if _, ok := r.(runtime.Error); ok {
	// 			panic(r)
	// 		}
	// 		if v, ok := r.(string); ok {
	// 			err = errors.New(v)
	// 		} else {
	// 			err = r.(error)
	// 		}
	// 	}
	// }()

	dv := reflect.ValueOf(dst)
	sv := reflect.ValueOf(src)

	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return &InvalidDecodeError{reflect.TypeOf(dst)}
	}
	s := &decodeState{}
	decode(s, dv, sv)

	return s.savedError
}

type decodeState struct {
	savedError error
}

// saveError saves the first err it is called with.
func (d *decodeState) saveError(err error) {
	if d.savedError == nil {
		d.savedError = err
	}
}

// decodeInterface decodes the source value into the destination value
func decode(s *decodeState, dv, sv reflect.Value) {
	if dv.IsValid() && sv.IsValid() {
		// Ensure that the source value has the correct type of parsing
		if sv.Kind() == reflect.Interface {
			sv = reflect.ValueOf(sv.Interface())
		}

		switch sv.Kind() {
		default:
			decodeLiteral(s, dv, sv)
		case reflect.Slice, reflect.Array:
			decodeArray(s, dv, sv)
		case reflect.Map:
			decodeObject(s, dv, sv)
		case reflect.Struct:
			dv = indirect(dv, false)
			dv.Set(sv)
		}
	}
}

// decodeLiteral decodes the source value into the destination value. This function
// is used to decode literal values.
func decodeLiteral(s *decodeState, dv reflect.Value, sv reflect.Value) {
	dv = indirect(dv, true)

	// Special case for if sv is nil:
	switch sv.Kind() {
	case reflect.Invalid:
		dv.Set(reflect.Zero(dv.Type()))
		return
	}

	// Attempt to convert the value from the source type to the destination type
	switch value := sv.Interface().(type) {
	case nil:
		switch dv.Kind() {
		case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice:
			dv.Set(reflect.Zero(dv.Type()))
		}
	case bool:
		switch dv.Kind() {
		default:
			s.saveError(&DecodeTypeError{"bool", dv.Type()})
			return
		case reflect.Bool:
			dv.SetBool(value)
		case reflect.String:
			dv.SetString(strconv.FormatBool(value))
		case reflect.Interface:
			if dv.NumMethod() == 0 {
				dv.Set(reflect.ValueOf(value))
			} else {
				s.saveError(&DecodeTypeError{"bool", dv.Type()})
				return
			}
		}

	case string:
		switch dv.Kind() {
		default:
			s.saveError(&DecodeTypeError{"string", dv.Type()})
			return
		case reflect.String:
			dv.SetString(value)
		case reflect.Bool:
			b, err := strconv.ParseBool(value)
			if err != nil {
				s.saveError(&DecodeTypeError{"string", dv.Type()})
				return
			}
			dv.SetBool(b)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || dv.OverflowInt(n) {
				s.saveError(&DecodeTypeError{"string", dv.Type()})
				return
			}
			dv.SetInt(n)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil || dv.OverflowUint(n) {
				s.saveError(&DecodeTypeError{"string", dv.Type()})
				return
			}
			dv.SetUint(n)
		case reflect.Float32, reflect.Float64:
			n, err := strconv.ParseFloat(value, 64)
			if err != nil || dv.OverflowFloat(n) {
				s.saveError(&DecodeTypeError{"string", dv.Type()})
				return
			}
			dv.SetFloat(n)
		case reflect.Interface:
			if dv.NumMethod() == 0 {
				dv.Set(reflect.ValueOf(string(value)))
			} else {
				s.saveError(&DecodeTypeError{"string", dv.Type()})
				return
			}
		}

	case int, int8, int16, int32, int64:
		switch dv.Kind() {
		default:
			s.saveError(&DecodeTypeError{"int", dv.Type()})
			return
		case reflect.Interface:
			if dv.NumMethod() != 0 {
				s.saveError(&DecodeTypeError{"int", dv.Type()})
				return
			}
			dv.Set(reflect.ValueOf(value))

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetInt(int64(reflect.ValueOf(value).Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			dv.SetUint(uint64(reflect.ValueOf(value).Int()))
		case reflect.Float32, reflect.Float64:
			dv.SetFloat(float64(reflect.ValueOf(value).Int()))
		case reflect.String:
			dv.SetString(strconv.FormatInt(int64(reflect.ValueOf(value).Int()), 10))
		}
	case uint, uint8, uint16, uint32, uint64:
		switch dv.Kind() {
		default:
			s.saveError(&DecodeTypeError{"uint", dv.Type()})
			return
		case reflect.Interface:
			if dv.NumMethod() != 0 {
				s.saveError(&DecodeTypeError{"uint", dv.Type()})
				return
			}
			dv.Set(reflect.ValueOf(value))

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetInt(int64(reflect.ValueOf(value).Uint()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			dv.SetUint(uint64(reflect.ValueOf(value).Uint()))
		case reflect.Float32, reflect.Float64:
			dv.SetFloat(float64(reflect.ValueOf(value).Uint()))
		case reflect.String:
			dv.SetString(strconv.FormatUint(uint64(reflect.ValueOf(value).Uint()), 10))
		}
	case float32, float64:
		switch dv.Kind() {
		default:
			s.saveError(&DecodeTypeError{"float", dv.Type()})
			return
		case reflect.Interface:
			if dv.NumMethod() != 0 {
				s.saveError(&DecodeTypeError{"float", dv.Type()})
				return
			}
			dv.Set(reflect.ValueOf(value))

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetInt(int64(reflect.ValueOf(value).Float()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			dv.SetUint(uint64(reflect.ValueOf(value).Float()))
		case reflect.Float32, reflect.Float64:
			dv.SetFloat(float64(reflect.ValueOf(value).Float()))
		case reflect.String:
			dv.SetString(strconv.FormatFloat(float64(reflect.ValueOf(value).Float()), 'g', -1, 64))
		}
	default:
		s.saveError(&DecodeTypeError{sv.Type().String(), dv.Type()})
		return
	}

	return
}

// decodeArray decodes the source value into the destination value. This function
// is used when the source value is a slice or array.
func decodeArray(s *decodeState, dv reflect.Value, sv reflect.Value) {
	dv = indirect(dv, false)
	dt := dv.Type()

	// Ensure that the dest is also a slice or array
	switch dt.Kind() {
	case reflect.Interface:
		if dv.NumMethod() == 0 {
			// Decoding into nil interface?  Switch to non-reflect code.
			dv.Set(reflect.ValueOf(decodeArrayInterface(s, sv)))

			return
		}
		// Otherwise it's invalid.
		fallthrough
	default:
		s.saveError(&DecodeTypeError{"array", dv.Type()})
		return
	case reflect.Array:
	case reflect.Slice:
		break
	}

	if dv.Kind() == reflect.Slice {
		dv.Set(reflect.MakeSlice(dt, 0, 0))
	}

	// Iterate through the slice/array and decode each element before adding it
	// to the dest slice/array
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
			decode(s, dv.Index(i), sv.Index(i))
		} else {
			// Ran out of fixed array: skip.
			decode(s, reflect.Value{}, sv.Index(i))
		}

		i++
	}

	// Ensure that the destination is the correct size
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
}

// decodeObject decodes the source value into the destination value. This function
// is used when the source value is a map or struct.
func decodeObject(s *decodeState, dv reflect.Value, sv reflect.Value) (err error) {
	dv = indirect(dv, false)
	dt := dv.Type()

	// Decoding into nil interface?  Switch to non-reflect code.
	if dv.Kind() == reflect.Interface && dv.NumMethod() == 0 {
		dv.Set(reflect.ValueOf(decodeObjectInterface(s, sv)))
		return nil
	}

	// Check type of target: struct or map[string]T
	switch dv.Kind() {
	case reflect.Map:
		// map must have string kind
		if dt.Key().Kind() != reflect.String {
			s.saveError(&DecodeTypeError{"object", dv.Type()})
			break
		}
		if dv.IsNil() {
			dv.Set(reflect.MakeMap(dt))
		}
	case reflect.Struct:
	default:
		s.saveError(&DecodeTypeError{"object", dv.Type()})
		return
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
			fields := cachedTypeFields(dv)
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

		decode(s, subdv, subsv)

		if dv.Kind() == reflect.Map {
			kv := reflect.ValueOf(skey)
			dv.SetMapIndex(kv, subdv)
		}
	}

	return nil
}

// The following methods are simplified versions of those above designed to use
// less reflection

// decodeInterface decodes the source value into interface{}
func decodeInterface(s *decodeState, sv reflect.Value) interface{} {
	// Ensure that the source value has the correct type of parsing
	if sv.Kind() == reflect.Interface {
		sv = reflect.ValueOf(sv.Interface())
	}

	switch sv.Kind() {
	case reflect.Slice, reflect.Array:
		return decodeArrayInterface(s, sv)
	case reflect.Map:
		return decodeObjectInterface(s, sv)
	default:
		return decodeLiteralInterface(s, sv)
	}
}

// decodeArrayInterface decodes the source value into []interface{}
func decodeArrayInterface(s *decodeState, sv reflect.Value) []interface{} {
	arr := []interface{}{}
	for i := 0; i < sv.Len(); i++ {
		arr = append(arr, decodeInterface(s, sv.Index(i)))
	}
	return arr
}

// decodeObjectInterface decodes the source value into map[string]interface{}
func decodeObjectInterface(s *decodeState, sv reflect.Value) map[string]interface{} {
	m := map[string]interface{}{}
	for _, key := range sv.MapKeys() {
		m[key.Interface().(string)] = decodeInterface(s, sv.MapIndex(key))
	}
	return m
}

// decodeLiteralInterface returns the interface of the source value
func decodeLiteralInterface(s *decodeState, sv reflect.Value) interface{} {
	if !sv.IsValid() {
		return nil
	}

	return sv.Interface()
}

// indirect walks down v allocating pointers as needed,
// until it gets to a non-pointer.
func indirect(v reflect.Value, decodeNull bool) reflect.Value {
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
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodeNull || e.Elem().Kind() == reflect.Ptr) {
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
