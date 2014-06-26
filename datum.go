package gorethink

import (
	"encoding/json"
	"fmt"
	"reflect"

	"code.google.com/p/goprotobuf/proto"
	p "github.com/dancannon/gorethink/ql2"
)

// Converts a query term to a datum. If the term cannot be converted to a datum
// object then the function panics.
func constructDatum(t Term) (*p.Term, error) {
	if t.data == nil {
		return &p.Term{
			Type: p.Term_DATUM.Enum(),
			Datum: &p.Datum{
				Type: p.Datum_R_NULL.Enum(),
			},
		}, nil
	} else {
		typ := reflect.TypeOf(t.data)

		switch typ.Kind() {
		case reflect.Bool:
			// Cast value to string
			rv := reflect.ValueOf(t.data)
			fv := rv.Bool()

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type:  p.Datum_R_BOOL.Enum(),
					RBool: proto.Bool(fv),
				},
			}, nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// Cast value to float64
			rv := reflect.ValueOf(t.data)
			fv := float64(rv.Uint())

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_NUM.Enum(),
					RNum: proto.Float64(fv),
				},
			}, nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// Cast value to float64
			rv := reflect.ValueOf(t.data)
			fv := float64(rv.Int())

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_NUM.Enum(),
					RNum: proto.Float64(fv),
				},
			}, nil
		case reflect.Float32, reflect.Float64:
			// Cast value to float64
			rv := reflect.ValueOf(t.data)
			fv := rv.Float()

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_NUM.Enum(),
					RNum: proto.Float64(fv),
				},
			}, nil
		case reflect.String:
			// Cast value to string
			rv := reflect.ValueOf(t.data)
			fv := rv.String()

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_STR.Enum(),
					RStr: proto.String(fv),
				},
			}, nil
		default:
			return &p.Term{}, fmt.Errorf("Cannot convert type '%T' to Datum", t.data)
		}
	}
}

func deconstructDatums(datums []*p.Datum, opts map[string]interface{}) ([]interface{}, error) {
	res := []interface{}{}

	for _, datum := range datums {
		value, err := deconstructDatum(datum, opts)
		if err != nil {
			return []interface{}{}, err
		}

		res = append(res, value)
	}

	return res, nil
}

// deconstructDatum converts a datum object to an arbitrary type
func deconstructDatum(datum *p.Datum, opts map[string]interface{}) (interface{}, error) {
	switch datum.GetType() {
	case p.Datum_R_NULL:
		return nil, nil
	case p.Datum_R_JSON:
		var v interface{}
		err := json.Unmarshal([]byte(datum.GetRStr()), &v)
		if err != nil {
			return nil, err
		}

		v, err = recursivelyConvertPseudotype(v, opts)
		if err != nil {
			return nil, err
		}

		return v, nil
	case p.Datum_R_BOOL:
		return datum.GetRBool(), nil
	case p.Datum_R_NUM:
		return datum.GetRNum(), nil
	case p.Datum_R_STR:
		return datum.GetRStr(), nil
	case p.Datum_R_ARRAY:
		items := []interface{}{}
		for _, d := range datum.GetRArray() {
			item, err := deconstructDatum(d, opts)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	case p.Datum_R_OBJECT:
		obj := map[string]interface{}{}

		for _, assoc := range datum.GetRObject() {
			key := assoc.GetKey()

			val, err := deconstructDatum(assoc.GetVal(), opts)
			if err != nil {
				return nil, err
			}

			obj[string(key)] = val
		}

		pobj, err := convertPseudotype(obj, opts)
		if err != nil {
			return nil, err
		}

		return pobj, nil
	}

	return nil, fmt.Errorf("Unknown Datum type %s encountered in response.", datum.GetType().String())
}

func convertPseudotype(obj map[string]interface{}, opts map[string]interface{}) (interface{}, error) {
	if reqlType, ok := obj["$reql_type$"]; ok {
		if reqlType == "TIME" {
			// load timeFormat, set to native if the option was not set
			timeFormat := "native"
			if opt, ok := opts["time_format"]; ok {
				if sopt, ok := opt.(string); ok {
					timeFormat = sopt
				} else {
					return nil, fmt.Errorf("Invalid time_format run option \"%s\".", opt)
				}
			}

			if timeFormat == "native" {
				return reqlTimeToNativeTime(obj["epoch_time"].(float64), obj["timezone"].(string))
			} else if timeFormat == "raw" {
				return obj, nil
			} else {
				return nil, fmt.Errorf("Unknown time_format run option \"%s\".", reqlType)
			}
		} else if reqlType == "GROUPED_DATA" {
			// load groupFormat, set to native if the option was not set
			groupFormat := "native"
			if opt, ok := opts["group_format"]; ok {
				if sopt, ok := opt.(string); ok {
					groupFormat = sopt
				} else {
					return nil, fmt.Errorf("Invalid group_format run option \"%s\".", opt)
				}
			}

			if groupFormat == "native" {
				return reqlGroupedDataToObj(obj)
			} else if groupFormat == "raw" {
				return obj, nil
			} else {
				return nil, fmt.Errorf("Unknown group_format run option \"%s\".", reqlType)
			}
		} else {
			return obj, nil
		}
	}

	return obj, nil
}

func recursivelyConvertPseudotype(obj interface{}, opts map[string]interface{}) (interface{}, error) {
	var err error

	switch obj := obj.(type) {
	case []interface{}:
		for key, val := range obj {
			obj[key], err = recursivelyConvertPseudotype(val, opts)
			if err != nil {
				return nil, err
			}
		}
	case map[string]interface{}:
		for key, val := range obj {
			obj[key], err = recursivelyConvertPseudotype(val, opts)
			if err != nil {
				return nil, err
			}
		}

		pobj, err := convertPseudotype(obj, opts)
		if err != nil {
			return nil, err
		}

		return pobj, nil
	}

	return obj, nil
}
