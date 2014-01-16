package encoding

import (
	"image"
	"reflect"
	"testing"
	"time"
)

var encodeExpected = map[string]interface{}{
	"Level0":  1,
	"Level1b": 2,
	"Level1c": 3,
	"Level1a": 5,
	"LEVEL1B": 6,
	"e": map[string]interface{}{
		"Level1a": 8,
		"Level1b": 9,
		"Level1c": 10,
		"Level1d": 11,
		"x":       12,
	},
	"Loop1": 13,
	"Loop2": 14,
	"X":     15,
	"Y":     16,
	"Z":     17,
}

func TestEncode(t *testing.T) {
	// Top is defined in decoder_test.go
	var in Top = Top{
		Level0: 1,
		Embed0: Embed0{
			Level1b: 2,
			Level1c: 3,
		},
		Embed0a: &Embed0a{
			Level1a: 5,
			Level1b: 6,
		},
		Embed0b: &Embed0b{
			Level1a: 8,
			Level1b: 9,
			Level1c: 10,
			Level1d: 11,
			Level1e: 12,
		},
		Loop: Loop{
			Loop1: 13,
			Loop2: 14,
		},
		Embed0p: Embed0p{
			Point: image.Point{X: 15, Y: 16},
		},
		Embed0q: Embed0q{
			Point: Point{Z: 17},
		},
	}

	got, err := Encode(&in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, encodeExpected) {
		t.Errorf(" got: %v\nwant: %v\n", got, encodeExpected)
	}
}

type FieldMappable struct {
	Str string
	Int int
}

func (f FieldMappable) FieldMap() map[string]string {
	return map[string]string{
		"Str": "str",
		"Int": "int",
	}
}

func TestFieldMapper(t *testing.T) {
	var in = FieldMappable{"string", 123}
	var out = map[string]interface{}{
		"str": "string",
		"int": 123,
	}

	got, err := Encode(&in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, out) {
		t.Errorf(" got: %v\nwant: %v\n", got, out)
	}
}

type Optionals struct {
	Sr string `gorethink:"sr"`
	So string `gorethink:"so,omitempty"`
	Sw string `gorethink:"-"`

	Ir int `gorethink:"omitempty"` // actually named omitempty, not an option
	Io int `gorethink:"io,omitempty"`

	Slr []string `gorethink:"slr"`
	Slo []string `gorethink:"slo,omitempty"`

	Mr map[string]interface{} `gorethink:"mr"`
	Mo map[string]interface{} `gorethink:",omitempty"`

	Tr time.Time `gorethink:"tr"`
	To time.Time `gorethink:",omitempty"`
}

var optionalsExpected = map[string]interface{}{
	"sr":        "",
	"omitempty": 0,
	"slr":       []interface{}(nil),
	"mr":        map[string]interface{}{},
	"tr":        time.Time{},
}

func TestOmitEmpty(t *testing.T) {
	var o Optionals
	o.Sw = "something"
	o.Mr = map[string]interface{}{}
	o.Mo = map[string]interface{}{}
	o.Tr = time.Time{}

	got, err := Encode(&o)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, optionalsExpected) {
		t.Errorf(" got: %v\nwant: %v\n", got, optionalsExpected)
	}
}

type IntType int

type MyStruct struct {
	IntType
}

func TestAnonymousNonstruct(t *testing.T) {
	var i IntType = 11
	a := MyStruct{i}
	var want = map[string]interface{}{"IntType": IntType(11)}

	got, err := Encode(a)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

type BugA struct {
	S string
}

type BugB struct {
	BugA
	S string
}

type BugC struct {
	S string
}

// Legal Go: We never use the repeated embedded field (S).
type BugX struct {
	A int
	BugA
	BugB
}

// Issue 5245.
func TestEmbeddedBug(t *testing.T) {
	v := BugB{
		BugA{"A"},
		"B",
	}
	got, err := Encode(v)
	if err != nil {
		t.Fatal("Encode:", err)
	}
	want := map[string]interface{}{"S": "B"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Encode: got %v want %v", got, want)
	}
	// Now check that the duplicate field, S, does not appear.
	x := BugX{
		A: 23,
	}
	got, err = Encode(x)
	if err != nil {
		t.Fatal("Encode:", err)
	}
	want = map[string]interface{}{"A": 23}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Encode: got %v want %v", got, want)
	}
}

type BugD struct { // Same as BugA after tagging.
	XXX string `gorethink:"S"`
}

// BugD's tagged S field should dominate BugA's.
type BugY struct {
	BugA
	BugD
}

// Test that a field with a tag dominates untagged fields.
func TestTaggedFieldDominates(t *testing.T) {
	v := BugY{
		BugA{"BugA"},
		BugD{"BugD"},
	}
	got, err := Encode(v)
	if err != nil {
		t.Fatal("Encode:", err)
	}
	want := map[string]interface{}{"S": "BugD"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Encode: got %v want %v", got, want)
	}
}

// There are no tags here, so S should not appear.
type BugZ struct {
	BugA
	BugC
	BugY // Contains a tagged S field through BugD; should not dominate.
}

func TestDuplicatedFieldDisappears(t *testing.T) {
	v := BugZ{
		BugA{"BugA"},
		BugC{"BugC"},
		BugY{
			BugA{"nested BugA"},
			BugD{"nested BugD"},
		},
	}
	got, err := Encode(v)
	if err != nil {
		t.Fatal("Encode:", err)
	}
	want := map[string]interface{}{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Encode: got %v want %v", got, want)
	}
}
