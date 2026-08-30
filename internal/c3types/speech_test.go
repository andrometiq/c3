package c3types

import (
	"reflect"
	"testing"
)

func TestSpeechTypesArePlainUntaggedStructs(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "SpeechRequest", value: SpeechRequest{}},
		{name: "SpeechResult", value: SpeechResult{}},
	}
	for _, test := range tests {
		typeOf := reflect.TypeOf(test.value)
		if typeOf.Name() != test.name || typeOf.Kind() != reflect.Struct {
			t.Fatalf("%T is not the named struct %s", test.value, test.name)
		}
		for index := 0; index < typeOf.NumField(); index++ {
			if tag := typeOf.Field(index).Tag.Get("json"); tag != "" {
				t.Errorf("%s.%s json tag = %q, want none", typeOf.Name(), typeOf.Field(index).Name, tag)
			}
		}
	}
}
