package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxStrictJSONDepth = 128
	maxStrictJSONNodes = 1_000_000
)

func decodeStrictJSON(data []byte, destination any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("input contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return err
	}
	expected := reflect.TypeOf(destination)
	if expected == nil || expected.Kind() != reflect.Pointer || expected.Elem().Kind() != reflect.Struct || reflect.ValueOf(destination).IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer to a struct")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	nodes := 0
	if err := consumeExactJSONValue(decoder, expected.Elem(), "document", 0, &nodes); err != nil {
		return err
	}
	if trailing, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode trailing data: %w", err)
		}
		return fmt.Errorf("input contains trailing token %v", trailing)
	}

	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("input contains trailing value")
		}
		return fmt.Errorf("decode trailing data: %w", err)
	}
	return nil
}

func consumeExactJSONValue(decoder *json.Decoder, expected reflect.Type, path string, depth int, nodes *int) error {
	if depth > maxStrictJSONDepth {
		return fmt.Errorf("JSON nesting exceeds limit")
	}
	*nodes++
	if *nodes > maxStrictJSONNodes {
		return fmt.Errorf("JSON token count exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch expected.Kind() {
	case reflect.Struct:
		delimiter, ok := token.(json.Delim)
		if !ok || delimiter != '{' {
			return fmt.Errorf("%s must be a non-null object", path)
		}
		fields := make(map[string]reflect.StructField, expected.NumField())
		required := make(map[string]bool, expected.NumField())
		for index := 0; index < expected.NumField(); index++ {
			field := expected.Field(index)
			if field.PkgPath != "" {
				continue
			}
			name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			fields[name] = field
			required[name] = !strings.Contains(","+options+",", ",omitempty,")
		}
		seen := make(map[string]struct{}, len(fields))
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("JSON object field name must be a string")
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("%s contains duplicate field %q", path, name)
			}
			seen[name] = struct{}{}
			field, known := fields[name]
			if !known {
				return fmt.Errorf("%s contains unknown or non-canonical field %q", path, name)
			}
			if err := consumeExactJSONValue(decoder, field.Type, path+"."+name, depth+1, nodes); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("JSON object is not closed")
		}
		for name, isRequired := range required {
			if _, present := seen[name]; isRequired && !present {
				return fmt.Errorf("%s is missing required field %q", path, name)
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		delimiter, ok := token.(json.Delim)
		if !ok || delimiter != '[' {
			return fmt.Errorf("%s must be a non-null array", path)
		}
		index := 0
		for decoder.More() {
			if err := consumeExactJSONValue(decoder, expected.Elem(), fmt.Sprintf("%s[%d]", path, index), depth+1, nodes); err != nil {
				return err
			}
			index++
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("JSON array is not closed")
		}
		return nil
	case reflect.String:
		if _, ok := token.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
		return nil
	case reflect.Bool:
		if _, ok := token.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number, ok := token.(json.Number)
		if !ok {
			return fmt.Errorf("%s must be an unsigned integer", path)
		}
		parsed, err := strconv.ParseUint(number.String(), 10, expected.Bits())
		if err != nil || strconv.FormatUint(parsed, 10) != number.String() {
			return fmt.Errorf("%s must be a canonical unsigned integer", path)
		}
		return nil
	default:
		return fmt.Errorf("%s has unsupported persisted JSON type %s", path, expected)
	}
}

func validateJSONUnicodeEscapes(data []byte) error {
	for index := 0; index < len(data); index++ {
		if data[index] != '"' {
			continue
		}
		closed := false
		for index++; index < len(data); index++ {
			switch data[index] {
			case '"':
				closed = true
			case '\\':
				if index+1 >= len(data) {
					return fmt.Errorf("unterminated JSON escape")
				}
				escape := data[index+1]
				if escape != 'u' {
					if !strings.ContainsRune(`"\\/bfnrt`, rune(escape)) {
						return fmt.Errorf("invalid JSON escape")
					}
					index++
					continue
				}
				codePoint, ok := decodeHexQuad(data, index+2)
				if !ok {
					return fmt.Errorf("invalid JSON Unicode escape")
				}
				switch {
				case codePoint >= 0xd800 && codePoint <= 0xdbff:
					low, lowOK := decodeFollowingLowSurrogate(data, index+6)
					if !lowOK || low < 0xdc00 || low > 0xdfff {
						return fmt.Errorf("isolated high surrogate in JSON string")
					}
					index += 11
				case codePoint >= 0xdc00 && codePoint <= 0xdfff:
					return fmt.Errorf("isolated low surrogate in JSON string")
				default:
					index += 5
				}
			case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f:
				return fmt.Errorf("unescaped control character in JSON string")
			}
			if closed {
				break
			}
		}
		if !closed {
			return fmt.Errorf("unterminated JSON string")
		}
	}
	return nil
}

func decodeHexQuad(data []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(data) {
		return 0, false
	}
	var value uint16
	for _, digit := range data[start : start+4] {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value |= uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value |= uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func decodeFollowingLowSurrogate(data []byte, start int) (uint16, bool) {
	if start+6 > len(data) || data[start] != '\\' || data[start+1] != 'u' {
		return 0, false
	}
	return decodeHexQuad(data, start+2)
}
