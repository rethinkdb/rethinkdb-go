package encoding

import (
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"time"
)

// newTypeEncoder constructs an encoderFunc for a type.
// The returned encoder only checks CanAddr when allowAddr is true.
func newTypeEncoder(t reflect.Type, allowAddr bool) encoderFunc {
	if t.Implements(marshalerType) {
		return marshalerEncoder
	}
	if t.Kind() != reflect.Ptr && allowAddr {
		if reflect.PointerTo(t).Implements(marshalerType) {
			return newCondAddrEncoder(addrMarshalerEncoder, newTypeEncoder(t, false))
		}
	}

	// Check for pseudo-types first
	switch t {
	case timeType:
		return timePseudoTypeEncoder
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintEncoder
	case reflect.Float32:
		return float32Encoder
	case reflect.Float64:
		return floatEncoder
	case reflect.String:
		return stringEncoder
	case reflect.Interface:
		return interfaceEncoder
	case reflect.Struct:
		return newStructEncoder(t)
	case reflect.Map:
		return newMapEncoder(t)
	case reflect.Slice:
		return newSliceEncoder(t)
	case reflect.Array:
		return newArrayEncoder(t)
	case reflect.Ptr:
		return newPtrEncoder(t)
	case reflect.Func:
		// functions are a special case as they can be used internally for
		// optional arguments. Just return the raw function, if somebody tries
		// to pass a function to the database the JSON marshaller will catch this
		// anyway.
		return funcEncoder
	default:
		return unsupportedTypeEncoder
	}
}

func invalidValueEncoder(v reflect.Value) (interface{}, error) {
	return nil, nil
}

func doNothingEncoder(v reflect.Value) (interface{}, error) {
	return v.Interface(), nil
}

func marshalerEncoder(v reflect.Value) (interface{}, error) {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return nil, nil
	}
	m := v.Interface().(Marshaler)
	ev, err := m.MarshalRQL()
	if err != nil {
		return nil, &MarshalerError{v.Type(), err}
	}

	return ev, nil
}

func addrMarshalerEncoder(v reflect.Value) (interface{}, error) {
	va := v.Addr()
	if va.IsNil() {
		return nil, nil
	}
	m := va.Interface().(Marshaler)
	ev, err := m.MarshalRQL()
	if err != nil {
		return nil, &MarshalerError{v.Type(), err}
	}

	return ev, nil
}

func boolEncoder(v reflect.Value) (interface{}, error) {
	if v.Bool() {
		return true, nil
	} else {
		return false, nil
	}
}

func intEncoder(v reflect.Value) (interface{}, error) {
	return v.Int(), nil
}

func uintEncoder(v reflect.Value) (interface{}, error) {
	return v.Uint(), nil
}

func floatEncoder(v reflect.Value) (interface{}, error) {
	return v.Float(), nil
}

func float32Encoder(v reflect.Value) (interface{}, error) {
	return float32(v.Float()), nil
}

func stringEncoder(v reflect.Value) (interface{}, error) {
	return v.String(), nil
}

func interfaceEncoder(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return nil, nil
	}
	return encode(v.Elem())
}

func funcEncoder(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return nil, nil
	}
	return v.Interface(), nil
}

func asStringEncoder(v reflect.Value) (interface{}, error) {
	return fmt.Sprintf("%v", v.Interface()), nil
}

func unsupportedTypeEncoder(v reflect.Value) (interface{}, error) {
	return nil, &UnsupportedTypeError{v.Type()}
}

type structEncoder struct {
	fields    []field
	fieldEncs []encoderFunc
}

func (se *structEncoder) encode(v reflect.Value) (interface{}, error) {
	m := make(map[string]interface{})
	for i, f := range se.fields {
		fv := fieldByIndex(v, f.index)
		if !fv.IsValid() || f.omitEmpty && se.isEmptyValue(fv) {
			continue
		}

		encField, err := se.fieldEncs[i](fv)
		if err != nil {
			return nil, err
		}

		// If this field is a referenced field then attempt to extract the value.
		if f.reference {
			// Override the encoded field with the referenced field
			encField = getReferenceField(f, v, encField)
		}

		if f.compound {
			compoundField, ok := m[f.name].([]interface{})
			if !ok {
				compoundField = make([]interface{}, f.compoundIndex+1)
			} else if len(compoundField) < f.compoundIndex+1 {
				tmp := make([]interface{}, f.compoundIndex+1)
				copy(tmp, compoundField)
				compoundField = tmp
			}

			compoundField[f.compoundIndex] = encField
			encField = compoundField
		}

		m[f.name] = encField
	}

	return m, nil
}

func getReferenceField(f field, v reflect.Value, encField interface{}) interface{} {
	refName := f.name
	if f.refName != "" {
		refName = f.refName
	}

	encFields, isArray := encField.([]interface{})
	if isArray {
		refVals := make([]interface{}, len(encFields))
		for i, e := range encFields {
			refVals[i] = extractValue(e, v, f.name, refName)
		}
		return refVals
	}
	refVal := extractValue(encField, v, f.name, refName)
	return refVal
}

func extractValue(encField interface{}, v reflect.Value, name string, refName string) interface{} {
	// referenced fields can only handle maps so return an error if the
	// encoded field is of a different type
	m, ok := encField.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("Error refing field %s in %s, expected object but got %t", refName, name, encField)
		panic(&MarshalerError{v.Type(), err})
	}
	refVal, ok := m[refName]
	if !ok {
		err := fmt.Errorf("Error refing field %s in %s, could not find referenced field", refName, name)
		panic(&MarshalerError{v.Type(), err})
	}
	return refVal
}

func (se *structEncoder) isEmptyValue(v reflect.Value) bool {
	if v.Type() == timeType {
		return v.Interface().(time.Time) == time.Time{}
	}

	return isEmptyValue(v)
}

func newStructEncoder(t reflect.Type) encoderFunc {
	fields := cachedTypeFields(t)
	se := &structEncoder{
		fields:    fields,
		fieldEncs: make([]encoderFunc, len(fields)),
	}
	for i, f := range fields {
		se.fieldEncs[i] = typeEncoder(typeByIndex(t, f.index))
	}
	return se.encode
}

type mapEncoder struct {
	keyEnc, elemEnc encoderFunc
}

func (me *mapEncoder) encode(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return nil, nil
	}

	m := make(map[string]interface{})

	for _, k := range v.MapKeys() {
		encV, err := me.elemEnc(v.MapIndex(k))
		if err != nil {
			return nil, err
		}
		encK, err := me.keyEnc(k)
		if err != nil {
			return nil, err
		}
		m[encK.(string)] = encV
	}

	return m, nil
}

func newMapEncoder(t reflect.Type) encoderFunc {
	var keyEnc encoderFunc
	switch t.Key().Kind() {
	case reflect.Bool:
		keyEnc = asStringEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		keyEnc = asStringEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		keyEnc = asStringEncoder
	case reflect.Float32, reflect.Float64:
		keyEnc = asStringEncoder
	case reflect.String:
		keyEnc = stringEncoder
	case reflect.Interface:
		keyEnc = asStringEncoder
	default:
		return unsupportedTypeEncoder
	}

	me := &mapEncoder{keyEnc, typeEncoder(t.Elem())}
	return me.encode
}

// sliceEncoder just wraps an arrayEncoder, checking to make sure the value isn't nil.
type sliceEncoder struct {
	arrayEnc encoderFunc
}

func (se *sliceEncoder) encode(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return []interface{}(nil), nil
	}
	return se.arrayEnc(v)
}

func newSliceEncoder(t reflect.Type) encoderFunc {
	// Byte slices get special treatment; arrays don't.
	if t.Elem().Kind() == reflect.Uint8 {
		return encodeByteSlice
	}
	enc := &sliceEncoder{newArrayEncoder(t)}
	return enc.encode
}

type arrayEncoder struct {
	elemEnc encoderFunc
}

func (ae *arrayEncoder) encode(v reflect.Value) (interface{}, error) {
	n := v.Len()

	a := make([]interface{}, n)
	for i := 0; i < n; i++ {
		var err error
		a[i], err = ae.elemEnc(v.Index(i))
		if err != nil {
			return nil, err
		}
	}

	return a, nil
}

func newArrayEncoder(t reflect.Type) encoderFunc {
	if t.Elem().Kind() == reflect.Uint8 {
		return encodeByteArray
	}
	enc := &arrayEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

type ptrEncoder struct {
	elemEnc encoderFunc
}

func (pe *ptrEncoder) encode(v reflect.Value) (interface{}, error) {
	if v.IsNil() {
		return nil, nil
	}
	return pe.elemEnc(v.Elem())
}

func newPtrEncoder(t reflect.Type) encoderFunc {
	enc := &ptrEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

type condAddrEncoder struct {
	canAddrEnc, elseEnc encoderFunc
}

func (ce *condAddrEncoder) encode(v reflect.Value) (interface{}, error) {
	if v.CanAddr() {
		return ce.canAddrEnc(v)
	} else {
		return ce.elseEnc(v)
	}
}

// newCondAddrEncoder returns an encoder that checks whether its value
// CanAddr and delegates to canAddrEnc if so, else to elseEnc.
func newCondAddrEncoder(canAddrEnc, elseEnc encoderFunc) encoderFunc {
	enc := &condAddrEncoder{canAddrEnc: canAddrEnc, elseEnc: elseEnc}
	return enc.encode
}

// Pseudo-type encoders

// Encode a time.Time value to the TIME RQL type
func timePseudoTypeEncoder(v reflect.Value) (interface{}, error) {
	t := v.Interface().(time.Time)

	timeVal := float64(t.UnixNano()) / float64(time.Second)

	// use seconds-since-epoch precision if time.Time `t`
	// is before the oldest nanosecond time
	if t.Before(time.Unix(0, math.MinInt64)) {
		timeVal = float64(t.Unix())
	}

	return map[string]interface{}{
		"$reql_type$": "TIME",
		"epoch_time":  timeVal,
		"timezone":    t.Format("-07:00"),
	}, nil
}

// Encode a byte slice to the BINARY RQL type
func encodeByteSlice(v reflect.Value) (interface{}, error) {
	var b []byte
	if !v.IsNil() {
		b = v.Bytes()
	}

	dst := make([]byte, base64.StdEncoding.EncodedLen(len(b)))
	base64.StdEncoding.Encode(dst, b)

	return map[string]interface{}{
		"$reql_type$": "BINARY",
		"data":        string(dst),
	}, nil
}

// Encode a byte array to the BINARY RQL type
func encodeByteArray(v reflect.Value) (interface{}, error) {
	b := make([]byte, v.Len())
	for i := 0; i < v.Len(); i++ {
		b[i] = v.Index(i).Interface().(byte)
	}

	dst := make([]byte, base64.StdEncoding.EncodedLen(len(b)))
	base64.StdEncoding.Encode(dst, b)

	return map[string]interface{}{
		"$reql_type$": "BINARY",
		"data":        string(dst),
	}, nil
}
