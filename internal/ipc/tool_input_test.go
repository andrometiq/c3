package ipc

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCanonicalToolInput(t *testing.T) {
	cases := []struct {
		name, input, want string
	}{
		{"key order", ` {"z":2,"a":1} `, `{"a":1,"z":2}`},
		{"big integers", `{"n":9007199254740993,"m":123456789012345678901234567890}`,
			`{"m":123456789012345678901234567890,"n":9007199254740993}`},
		{"number lexemes", `[1e+3,1.00,-0]`, `[1e+3,1.00,-0]`},
		{"nested arrays", `{"z":[{"b":2,"a":1},[3,2,1]],"a":[]}`, `{"a":[],"z":[{"a":1,"b":2},[3,2,1]]}`},
		{"surrogate pair", `"\uD83D\uDE00"`, `"😀"`},
		{"escaped backslash before u", `"\\uD800"`, `"\\uD800"`},
		{"escaped quote before a pair", `"\"\uD83D\uDE00"`, `"\"😀"`},
		{"null", `null`, `null`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := CanonicalToolInput(json.RawMessage(test.input))
			if err != nil || string(got) != test.want {
				t.Fatalf("canonical = %s, %v; want %s", got, err, test.want)
			}
		})
	}
}

func TestCanonicalToolInputRejectsAmbiguity(t *testing.T) {
	cases := []struct {
		name, input string
	}{
		{"invalid UTF-8", string([]byte{'"', 0xff, '"'})},
		{"lone high surrogate", `"\uD800"`},
		{"lone low surrogate", `"\uDFFF"`},
		{"high then plain character", `"\uD800a"`},
		{"high then high", `"\uD800\uD800"`},
		{"surrogate in object key", `{"\uD800":1}`},
		{"duplicate key", `{"a":1,"a":2}`},
		{"duplicate key through an escape", `{"a":1,"\u0061":2}`},
		{"duplicate key spelled two ways", `{"command":"ls","\u0063ommand":"rm -rf ~"}`},
		{"nested duplicate key", `[{"x":{"a":1,"a":2}}]`},
		{"second value", `{} {}`},
		{"trailing junk", `{} junk`},
		{"empty", ``},
		{"invalid JSON", `{"a":}`},
		{"incomplete array", `[1`},
		{"incomplete object", `{"a":1`},
		{"invalid escape", `"\uQQQQ"`},
		{"incomplete escape", `"\u12"`},
		{"excessive nesting", strings.Repeat("[", 10002) + "0" + strings.Repeat("]", 10002)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if value, err := CanonicalToolInput(json.RawMessage(test.input)); err == nil {
				t.Fatalf("accepted ambiguous input as %s", value)
			}
			if hash, err := ToolInputHash(json.RawMessage(test.input)); err == nil || hash != "" {
				t.Fatalf("invalid input produced hash %q, %v", hash, err)
			}
		})
	}
}

func TestToolInputHash(t *testing.T) {
	hash, err := ToolInputHash(json.RawMessage(`{"z":2,"a":1}`))
	if err != nil || len(hash) != 64 || strings.ToLower(hash) != hash {
		t.Fatalf("hash = %q, %v", hash, err)
	}
	equivalent, err := ToolInputHash(json.RawMessage(` { "a": 1, "z": 2 } `))
	if err != nil || hash != equivalent {
		t.Fatalf("key order or whitespace changed the hash: %q != %q (%v)", hash, equivalent, err)
	}
	for _, changed := range []string{`{"a":2,"z":2}`, `{"a":1.0,"z":2}`, `{"a":1,"z":2,"y":null}`} {
		other, err := ToolInputHash(json.RawMessage(changed))
		if err != nil || hash == other {
			t.Fatalf("%s reused the hash (%v)", changed, err)
		}
	}
	first, _ := ToolInputHash(json.RawMessage(`[1,2]`))
	second, _ := ToolInputHash(json.RawMessage(`[2,1]`))
	if first == second {
		t.Fatal("array order lost")
	}
}

func nested(depth int) string {
	return strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
}

// The walk enforces its own depth limit, below encoding/json's, both for a
// tool input and for a hook envelope carrying one.
func TestStrictJSONDepthLimit(t *testing.T) {
	if _, err := CanonicalToolInput(json.RawMessage(nested(maxJSONDepth))); err != nil {
		t.Fatalf("depth %d rejected: %v", maxJSONDepth, err)
	}
	for _, depth := range []int{maxJSONDepth + 1, 200000} {
		if _, err := CanonicalToolInput(json.RawMessage(nested(depth))); err == nil || err.Error() != "json: nesting too deep" {
			t.Fatalf("depth %d: %v, want this package's depth error", depth, err)
		}
		envelope := `{"op":"auto_denied","cli":"claude","session_id":"s","cwd":"/w","tool_name":"Bash","tool_input":` +
			nested(depth) + `}`
		var request AutoDeniedReq
		if err := DecodeStrict([]byte(envelope), &request); err == nil || err.Error() != "json: nesting too deep" {
			t.Fatalf("envelope at depth %d: %v", depth, err)
		}
	}
}

func TestDecodeStrict(t *testing.T) {
	var request GrantCheckReq
	if err := DecodeStrict([]byte(`{"op":"grant_check","cli":"claude","session_id":"s"}`), &request); err != nil ||
		request.SessionID != "s" {
		t.Fatalf("valid frame: %+v, %v", request, err)
	}
	for _, frame := range []string{
		`{"op":"grant_check","session_id":"a","session_id":"b"}`,
		`{"op":"grant_check","cwd":"\uDC00"}`,
		`{"op":"grant_check"} {}`,
	} {
		if err := DecodeStrict([]byte(frame), &request); err == nil {
			t.Fatalf("accepted ambiguous frame %s", frame)
		}
	}
}
