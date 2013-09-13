package rethinkgo

import (
	"fmt"
	p "github.com/dancannon/rethinkgo/ql2"
	"math"
	"reflect"
)

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
					RBool: &val,
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
					RNum: &fv,
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
					RNum: &fv,
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
					RNum: &fv,
				},
			}
		case string:
			return &p.Term{
				Type: p.Term_DATUM.Enum(),
				Datum: &p.Datum{
					Type: p.Datum_R_STR.Enum(),
					RStr: &val,
				},
			}
		default:
			panic(fmt.Sprintf("Cannot convert type '%T' to Datum", val))
		}
	}
}

func deconstructDatum(datum *p.Datum) (interface{}, error) {
	switch datum.GetType() {
	case p.Datum_R_NULL:
		return nil, nil
	case p.Datum_R_BOOL:
		return datum.GetRBool(), nil
	case p.Datum_R_NUM:
		num := datum.GetRNum()
		// Convert to an integer if we think maybe the user might think of this
		// number as an integer. I have been assured that this is a "temporary"
		// behavior change until RQL supports native integers.
		if math.Mod(num, 1) == 0 {
			// Then we assume that in the user's data model this floating point
			// number is meant be an integer and "helpfully" convert types for them.
			return int(num), nil
		}
		return num, nil
	case p.Datum_R_STR:
		return datum.GetRStr(), nil
	case p.Datum_R_ARRAY:
		items := []interface{}{}
		for _, d := range datum.GetRArray() {
			item, err := deconstructDatum(d)
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

			val, err := deconstructDatum(assoc.GetVal())
			if err != nil {
				return nil, err
			}

			obj[key] = val
		}

		if reqlType, ok := obj["$reql_type$"]; ok {
			if reqlType == "TIME" {
				// TODO: Add optional arguments and pass time format here
				timeformat := "native"
				if timeformat == "native" {
					return reqlTimeToNativeTime(obj["epoch_time"].(int64), obj["timezone"].(string))
				} else if timeformat == "raw" {
					return obj, nil
				} else {
					return nil, fmt.Errorf("Unknown time_format run option \"%s\".", reqlType)
				}
			} else {
				return nil, fmt.Errorf("Unknown psudo-type %.", reqlType)
			}
		}

		return obj, nil
	}

	return nil, fmt.Errorf("Unknown Datum type %d encountered in response.", datum.Type.String())
}
