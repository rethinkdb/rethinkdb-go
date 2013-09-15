// This code is based on encoding/json and gorilla/schema

package encoding

import (
	"reflect"
	"sort"
)

func Encode(v interface{}) (ev interface{}, err error) {
	val, err := encode(reflect.ValueOf(v))
	if err != nil {
		return nil, err
	}

	ev = val.Interface()

	return
}

func encode(v reflect.Value) (reflect.Value, error) {
	if !v.IsValid() {
		return reflect.Value{}, nil
	}

	switch v.Kind() {
	case reflect.Struct:
		m := reflect.MakeMap(reflect.TypeOf(map[string]interface{}{}))

		for _, f := range cachedTypeFields(v.Type()) {
			fv := fieldByIndex(v, f.index)
			if !fv.IsValid() || f.omitEmpty && isEmptyValue(fv) {
				continue
			}

			ev, err := encode(fv)
			if err != nil {
				break
			}
			m.SetMapIndex(reflect.ValueOf(f.name), ev)
		}

		return m, nil
	case reflect.Map:
		m := reflect.MakeMap(reflect.TypeOf(map[string]interface{}{}))

		if v.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, &UnsupportedTypeError{v.Type()}
		}
		if v.IsNil() {
			return reflect.Value{}, nil
		}

		var sv stringValues = v.MapKeys()
		sort.Sort(sv)
		for _, k := range sv {
			ev, err := encode(v.MapIndex(k))
			if err != nil {
				break
			}

			m.SetMapIndex(k, ev)
		}

		return m, nil
	case reflect.Slice:
		s := reflect.MakeSlice(reflect.TypeOf([]interface{}{}), v.Len(), v.Len())

		if v.IsNil() {
			return reflect.Value{}, nil
		}

		for i := 0; i < v.Len(); i++ {
			ev, err := encode(v.Index(i))
			if err != nil {
				break
			}

			s.Index(i).Set(ev)
		}

		return s, nil
	case reflect.Interface, reflect.Ptr:
		if v.IsNil() {
			return reflect.Value{}, nil
		}

		return encode(v.Elem())
	default:
		return v, nil
	}
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return reflect.Value{}
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

// stringValues is a slice of reflect.Value holding *reflect.StringValue.
// It implements the methods to sort by string.
type stringValues []reflect.Value

func (sv stringValues) Len() int           { return len(sv) }
func (sv stringValues) Swap(i, j int)      { sv[i], sv[j] = sv[j], sv[i] }
func (sv stringValues) Less(i, j int) bool { return sv.get(i) < sv.get(j) }
func (sv stringValues) get(i int) string   { return sv[i].String() }
