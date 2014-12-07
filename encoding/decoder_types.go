package encoding

import (
	"reflect"
	"strconv"

	"github.com/k0kubun/pp"
)

// newTypeDecoder constructs an decoderFunc for a type.
// The returned decoder only checks CanAddr when allowAddr is true.
func newTypeDecoder(dt, st reflect.Type, allowAddr bool) decoderFunc {
	switch dt.Kind() {
	case reflect.Bool:
		switch st.Kind() {
		case reflect.Bool:
			return boolAsBoolDecoder
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return intAsBoolDecoder
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return uintAsBoolDecoder
		case reflect.Float32, reflect.Float64:
			return floatAsBoolDecoder
		case reflect.String:
			return stringAsBoolDecoder
		default:
			return unconvertibleTypeDecoder
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch st.Kind() {
		case reflect.Bool:
			return boolAsIntDecoder
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return intAsIntDecoder
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return uintAsIntDecoder
		case reflect.Float32, reflect.Float64:
			return floatAsIntDecoder
		case reflect.String:
			return stringAsIntDecoder
		default:
			return unconvertibleTypeDecoder
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		switch st.Kind() {
		case reflect.Bool:
			return boolAsUintDecoder
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return intAsUintDecoder
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return uintAsUintDecoder
		case reflect.Float32, reflect.Float64:
			return floatAsUintDecoder
		case reflect.String:
			return stringAsUintDecoder
		default:
			return unconvertibleTypeDecoder
		}
	case reflect.Float32, reflect.Float64:
		switch st.Kind() {
		case reflect.Bool:
			return boolAsFloatDecoder
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return intAsFloatDecoder
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return uintAsFloatDecoder
		case reflect.Float32, reflect.Float64:
			return floatAsFloatDecoder
		case reflect.String:
			return stringAsFloatDecoder
		default:
			return unconvertibleTypeDecoder
		}
	case reflect.String:
		switch st.Kind() {
		case reflect.Bool:
			return boolAsStringDecoder
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return intAsStringDecoder
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return uintAsStringDecoder
		case reflect.Float32, reflect.Float64:
			return floatAsStringDecoder
		case reflect.String:
			return stringAsStringDecoder
		default:
			return unconvertibleTypeDecoder
		}
	case reflect.Interface:
		if !st.AssignableTo(dt) {
			return unexpectedTypeDecoder
		}

		return interfaceDecoder
	case reflect.Ptr:
		return newPtrDecoder(dt, st)
	// case reflect.Struct:
	// 	switch st.Kind() {
	// 	case reflect.Map:
	// 		if kind := st.Key().Kind(); kind != reflect.String && kind != reflect.Interface {
	// 			return newInvalidTypeError(fmt.Errorf("map needs string keys"))
	// 		}

	// 		return newStructDecoder(dt, st)
	// 	default:
	// 		return unconvertibleTypeDecoder
	// 	}
	case reflect.Map:
		switch st.Kind() {
		case reflect.Map:
			return newMapAsMapDecoder(dt, st)
		default:
			return unconvertibleTypeDecoder
		}
	// case reflect.Slice:
	// 	return newSliceDecoder(dt)
	// case reflect.Array:
	// 	return newArrayDecoder(dt)
	default:
		return unsupportedTypeDecoder
	}
}

func invalidValueDecoder(dv, sv reflect.Value) {
	dv.Set(reflect.Zero(dv.Type()))
}

func unsupportedTypeDecoder(dv, sv reflect.Value) {
	panic(&UnsupportedTypeError{dv.Type()})
}

func unexpectedTypeDecoder(dv, sv reflect.Value) {
	panic(&UnexpectedTypeError{dv.Type(), sv.Type()})
}

func unconvertibleTypeDecoder(dv, sv reflect.Value) {
	panic(&UnconvertibleTypeError{dv.Type(), sv.Type()})
}

func newInvalidTypeError(err error) decoderFunc {
	return func(dv, sv reflect.Value) {
		panic(&InvalidTypeError{dv.Type(), sv.Type(), err})
	}
}

func interfaceDecoder(dv, sv reflect.Value) {
	dv.Set(sv)
}

type ptrDecoder struct {
	elemDec decoderFunc
}

func (d *ptrDecoder) decode(dv, sv reflect.Value) {
	v := reflect.New(dv.Type().Elem())
	d.elemDec(v, sv)
	dv.Set(v)
}

func newPtrDecoder(dt, st reflect.Type) decoderFunc {
	dec := &ptrDecoder{typeDecoder(dt.Elem(), st)}

	return dec.decode
}

// Boolean decoders

func boolAsBoolDecoder(dv, sv reflect.Value) {
	dv.SetBool(sv.Bool())
}
func boolAsIntDecoder(dv, sv reflect.Value) {
	if sv.Bool() {
		dv.SetInt(1)
	} else {
		dv.SetInt(0)
	}
}
func boolAsUintDecoder(dv, sv reflect.Value) {
	if sv.Bool() {
		dv.SetUint(1)
	} else {
		dv.SetUint(0)
	}
}
func boolAsFloatDecoder(dv, sv reflect.Value) {
	if sv.Bool() {
		dv.SetFloat(1)
	} else {
		dv.SetFloat(0)
	}
}
func boolAsStringDecoder(dv, sv reflect.Value) {
	if sv.Bool() {
		dv.SetString("1")
	} else {
		dv.SetString("0")
	}
}

// Int decoders

func intAsBoolDecoder(dv, sv reflect.Value) {
	dv.SetBool(sv.Int() != 0)
}
func intAsIntDecoder(dv, sv reflect.Value) {
	dv.SetInt(sv.Int())
}
func intAsUintDecoder(dv, sv reflect.Value) {
	dv.SetUint(uint64(sv.Int()))
}
func intAsFloatDecoder(dv, sv reflect.Value) {
	dv.SetFloat(float64(sv.Int()))
}
func intAsStringDecoder(dv, sv reflect.Value) {
	dv.SetString(strconv.FormatInt(sv.Int(), 10))
}

// Uint decoders

func uintAsBoolDecoder(dv, sv reflect.Value) {
	dv.SetBool(sv.Uint() != 0)
}
func uintAsIntDecoder(dv, sv reflect.Value) {
	dv.SetInt(int64(sv.Uint()))
}
func uintAsUintDecoder(dv, sv reflect.Value) {
	dv.SetUint(sv.Uint())
}
func uintAsFloatDecoder(dv, sv reflect.Value) {
	dv.SetFloat(float64(sv.Uint()))
}
func uintAsStringDecoder(dv, sv reflect.Value) {
	dv.SetString(strconv.FormatUint(sv.Uint(), 10))
}

// Float decoders

func floatAsBoolDecoder(dv, sv reflect.Value) {
	dv.SetBool(sv.Float() != 0)
}
func floatAsIntDecoder(dv, sv reflect.Value) {
	dv.SetInt(int64(sv.Float()))
}
func floatAsUintDecoder(dv, sv reflect.Value) {
	dv.SetUint(uint64(sv.Float()))
}
func floatAsFloatDecoder(dv, sv reflect.Value) {
	dv.SetFloat(float64(sv.Float()))
}
func floatAsStringDecoder(dv, sv reflect.Value) {
	dv.SetString(strconv.FormatFloat(sv.Float(), 'f', -1, 64))
}

// String decoders

func stringAsBoolDecoder(dv, sv reflect.Value) {
	b, err := strconv.ParseBool(sv.String())
	if err == nil {
		dv.SetBool(b)
	} else if sv.String() == "" {
		dv.SetBool(false)
	} else {
		panic(&InvalidTypeError{dv.Type(), sv.Type(), err})
	}
}
func stringAsIntDecoder(dv, sv reflect.Value) {
	pp.Println(dv.Interface())
	i, err := strconv.ParseInt(sv.String(), 0, dv.Type().Bits())
	if err == nil {
		dv.SetInt(i)
	} else {
		panic(&InvalidTypeError{dv.Type(), sv.Type(), err})
	}
}
func stringAsUintDecoder(dv, sv reflect.Value) {
	i, err := strconv.ParseUint(sv.String(), 0, dv.Type().Bits())
	if err == nil {
		dv.SetUint(i)
	} else {
		panic(&InvalidTypeError{dv.Type(), sv.Type(), err})
	}
}
func stringAsFloatDecoder(dv, sv reflect.Value) {
	f, err := strconv.ParseFloat(sv.String(), dv.Type().Bits())
	if err == nil {
		dv.SetFloat(f)
	} else {
		panic(&InvalidTypeError{dv.Type(), sv.Type(), err})
	}
}
func stringAsStringDecoder(dv, sv reflect.Value) {
	dv.SetString(sv.String())
}

// Map decoder

type mapAsMapDecoder struct {
	keyDec, elemDec decoderFunc
}

func (d *mapAsMapDecoder) decode(dv, sv reflect.Value) {
	dt := sv.Type()

	mt := reflect.MapOf(dt.Key(), dt.Elem())
	m := reflect.MakeMap(mt)

	for _, k := range sv.MapKeys() {
		v := sv.MapIndex(k)
		ek := reflect.Indirect(reflect.New(dt.Key()))
		ev := reflect.Indirect(reflect.New(dt.Elem()))

		d.keyDec(ek, k)
		d.elemDec(ev, v)

		m.SetMapIndex(ek, ev)
	}

	dv.Set(m)
}

func newMapAsMapDecoder(dt, st reflect.Type) decoderFunc {
	d := &mapAsMapDecoder{typeDecoder(dt.Key(), st.Key()), typeDecoder(dt.Elem(), st.Elem())}
	return d.decode
}

// Struct decoder

// type structDecoder struct {
// 	fields    []field
// 	fieldEncs []decoderFunc
// }

// func (se *structDecoder) decode(dv, sv reflect.Value) {
// 	m := make(map[string]interface{})

// 	for i, f := range se.fields {
// 		fv := fieldByIndex(v, f.index)
// 		if !fv.IsValid() || f.omitEmpty && isEmptyValue(fv) {
// 			continue
// 		}

// 		m[f.name] = se.fieldEncs[i](fv)
// 	}

// 	return m
// }

// func newStructDecoder(t reflect.Type) decoderFunc {
// 	fields := cachedTypeFields(t)
// 	se := &structDecoder{
// 		fields:    fields,
// 		fieldEncs: make([]decoderFunc, len(fields)),
// 	}
// 	for i, f := range fields {
// 		se.fieldEncs[i] = typeDecoder(typeByIndex(t, f.index))
// 	}
// 	return se.decode
// }
