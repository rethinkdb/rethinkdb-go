package encoding

import "reflect"

// var (
// 	marshalerType     = reflect.TypeOf(new(Marshaler)).Elem()
// 	textMarshalerType = reflect.TypeOf(new(encoding.TextMarshaler)).Elem()

// 	timeType     = reflect.TypeOf(new(time.Time)).Elem()
// 	geometryType = reflect.TypeOf(new(types.Geometry)).Elem()
// )

// newTypeDecoder constructs an decoderFunc for a type.
// The returned decoder only checks CanAddr when allowAddr is true.
func newTypeDecoder(dt, st reflect.Type, allowAddr bool) decoderFunc {
	// if dt.Implements(marshalerType) {
	// 	return marshalerDecoder
	// }
	// if dt.Kind() != reflect.Ptr && allowAddr {
	// 	if reflect.PtrTo(dt).Implements(marshalerType) {
	// 		return newCondAddrDecoder(addrMarshalerDecoder, newTypeDecoder(dt, false))
	// 	}
	// }
	// Check for psuedo-types first
	// switch dt {
	// case timeType:
	// 	return timePseudoTypeDecoder
	// case geometryType:
	// 	return geometryPseudoTypeDecoder
	// }

	switch dt.Kind() {
	// case reflect.Bool:
	// 	return boolDecoder
	// case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	// 	return intDecoder
	// case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
	// 	return uintDecoder
	// case reflect.Float32:
	// 	return float32Decoder
	// case reflect.Float64:
	// 	return float64Decoder
	// case reflect.String:
	// 	return stringDecoder
	// case reflect.Interface:
	// 	return interfaceDecoder
	// case reflect.Struct:
	// 	return newStructDecoder(dt)
	// case reflect.Map:
	// 	return newMapDecoder(dt)
	// case reflect.Slice:
	// 	return newSliceDecoder(dt)
	// case reflect.Array:
	// 	return newArrayDecoder(dt)
	// case reflect.Ptr:
	// 	return newPtrDecoder(dt)
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
