package gorethink

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	p "github.com/dancannon/gorethink/ql2"
	"reflect"
)

// Converts a query term to a datum. If the term cannot be converted to a datum
// object then the function panics.
func constructDatum(t RqlTerm) *p.Term {
	if t.data == nil {
		return &p.Term{
			Type: p.Term_DATUM.Enum(),
			Datum: &p.Datum{
				Type: p.Datum_R_NULL.Enum(),
			},
		}
	} else {
		switch val := t.data.(type) {
		case bool:
			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type:  p.Datum_R_BOOL.Enum(),
					RBool: proto.Bool(val),
				},
			}
		case uint, uint8, uint16, uint32, uint64:
			// Cast value to float64
			rv := reflect.ValueOf(val)
			fv := float64(rv.Uint())

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_NUM.Enum(),
					RNum: proto.Float64(fv),
				},
			}
		case int, int8, int16, int32, int64:
			// Cast value to float64
			rv := reflect.ValueOf(val)
			fv := float64(rv.Int())

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_NUM.Enum(),
					RNum: proto.Float64(fv),
				},
			}
		case float32, float64:
			// Cast value to float64
			rv := reflect.ValueOf(val)
			fv := rv.Float()

			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_NUM.Enum(),
					RNum: proto.Float64(fv),
				},
			}
		case string:
			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_STR.Enum(),
					RStr: proto.String(val),
				},
			}
		default:
			panic(fmt.Sprintf("Cannot convert type '%T' to Datum", val))
		}
	}
}

// deconstructDatum converts a datum object to an arbitrary type
func deconstructDatum(datum *p.Datum, opts map[string]interface{}) (interface{}, error) {
	switch datum.GetType() {
	case p.Datum_R_NULL:
		return nil, nil
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
		obj := map[interface{}]interface{}{}

		for _, assoc := range datum.GetRObject() {
			key := assoc.GetKey()

			val, err := deconstructDatum(assoc.GetVal(), opts)
			if err != nil {
				return nil, err
			}

			obj[key] = val
		}

		// Handle ReQL pseudo-types
		if reqlType, ok := obj["$reql_type$"]; ok {
			if reqlType == "TIME" {
				// load timeformat, set to native if the option was not set
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
			} else {
				return obj, nil
			}
		}

		return obj, nil
	}

	return nil, fmt.Errorf("Unknown Datum type %s encountered in response.", datum.GetType().String())
}
