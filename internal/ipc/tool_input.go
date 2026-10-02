package ipc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

// CanonicalToolInput returns the canonical form of a tool_input that an
// auto-mode grant is keyed on: object keys sorted at every depth, arrays in
// order, numbers kept as their exact lexemes, no insignificant whitespace.
//
// It first rejects anything that two different inputs could share a canonical
// form through (see parseStrictJSON), so one approval can never match a
// different call.
func CanonicalToolInput(raw json.RawMessage) ([]byte, error) {
	value, err := parseStrictJSON(raw)
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// ToolInputHash is the lowercase hex SHA-256 of CanonicalToolInput.
func ToolInputHash(raw json.RawMessage) (string, error) {
	canonical, err := CanonicalToolInput(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// DecodeStrict unmarshals raw into v after the same validation as
// CanonicalToolInput. encoding/json alone keeps the last of two duplicate keys
// and turns invalid UTF-8 or lone surrogates into U+FFFD, so two different
// frames could otherwise decode to the same request.
func DecodeStrict(raw []byte, v any) error {
	if _, err := parseStrictJSON(raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// parseStrictJSON decodes exactly one JSON value, with numbers as json.Number.
// It rejects invalid UTF-8, unpaired \uD800–\uDFFF escapes, duplicate object
// keys at any depth and any data after the value.
func parseStrictJSON(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("json: not valid UTF-8")
	}
	if err := rejectUnpairedSurrogates(raw); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := readStrictValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("json: data after the value")
	}
	return value, nil
}

// rejectUnpairedSurrogates scans string escapes in raw bytes. The standard
// decoder would silently replace an unpaired surrogate escape with U+FFFD.
func rejectUnpairedSurrogates(raw []byte) error {
	isInString := false
	for index := 0; index < len(raw); index++ {
		if raw[index] == '"' {
			isInString = !isInString
			continue
		}
		if !isInString || raw[index] != '\\' {
			continue
		}
		index++ // skip the escaped byte, so an escaped quote never toggles isInString
		if index >= len(raw) || raw[index] != 'u' {
			continue
		}
		code, ok := hexEscape(raw, index+1)
		if !ok {
			return errors.New("json: invalid unicode escape")
		}
		index += 4
		if code >= 0xDC00 && code <= 0xDFFF {
			return errors.New("json: unpaired surrogate escape")
		}
		if code < 0xD800 || code > 0xDBFF {
			continue
		}
		// A high surrogate must be followed immediately by a low one.
		if index+2 >= len(raw) || raw[index+1] != '\\' || raw[index+2] != 'u' {
			return errors.New("json: unpaired surrogate escape")
		}
		low, ok := hexEscape(raw, index+3)
		if !ok || low < 0xDC00 || low > 0xDFFF {
			return errors.New("json: unpaired surrogate escape")
		}
		index += 6
	}
	return nil
}

// hexEscape parses the four hex digits of a \u escape starting at start.
func hexEscape(raw []byte, start int) (uint64, bool) {
	if start+4 > len(raw) {
		return 0, false
	}
	code, err := strconv.ParseUint(string(raw[start:start+4]), 16, 16)
	return code, err == nil
}

func readStrictValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, isString := token.(string)
			if !isString {
				return nil, errors.New("json: object key is not a string")
			}
			if _, exists := object[key]; exists {
				return nil, errors.New("json: duplicate object key")
			}
			if object[key], err = readStrictValue(decoder); err != nil {
				return nil, err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return object, nil
	case '[':
		array := []any{}
		for decoder.More() {
			value, err := readStrictValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, errors.New("json: unexpected delimiter")
	}
}
