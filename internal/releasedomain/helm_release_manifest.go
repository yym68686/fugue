package releasedomain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"
)

// ExtractHelmReleaseManifest extracts only the rendered manifest and hook
// manifests from Helm's JSON release output. Volatile release metadata is
// validated where it binds the render (name, namespace, version) and otherwise
// excluded from canonical evidence.
func ExtractHelmReleaseManifest(
	data []byte,
	expectedName string,
	expectedNamespace string,
	expectedVersion uint64,
) ([]byte, error) {
	if len(data) > maxRenderedManifestBytes {
		return nil, fmt.Errorf("Helm release JSON bytes exceed limit %d", maxRenderedManifestBytes)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("Helm release JSON contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return nil, fmt.Errorf("Helm release JSON: %w", err)
	}
	if err := validateStrictJSON(data); err != nil {
		return nil, fmt.Errorf("Helm release JSON: %w", err)
	}
	if expectedName == "" || expectedNamespace == "" || expectedVersion == 0 {
		return nil, fmt.Errorf("expected Helm release identity and version are required")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("root must be an object")
	}
	seen := map[string]struct{}{}
	var name string
	var namespace string
	var version uint64
	var manifest string
	hooks := make([]string, 0)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		field, ok := nameToken.(string)
		if !ok {
			return nil, fmt.Errorf("field name must be a string")
		}
		if _, duplicate := seen[field]; duplicate {
			return nil, fmt.Errorf("duplicate top-level field %q", field)
		}
		seen[field] = struct{}{}
		switch field {
		case "name":
			name, err = decodeJSONStringToken(decoder, field)
		case "namespace":
			namespace, err = decodeJSONStringToken(decoder, field)
		case "version":
			version, err = decodeJSONUintToken(decoder, field)
		case "manifest":
			manifest, err = decodeJSONStringToken(decoder, field)
		case "hooks":
			hooks, err = decodeHelmHookManifests(decoder)
		case "apply_method", "chart", "config", "info":
			var ignored json.RawMessage
			err = decoder.Decode(&ignored)
		default:
			return nil, fmt.Errorf("unknown top-level field %q", field)
		}
		if err != nil {
			return nil, err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return nil, fmt.Errorf("root object is not closed")
	}
	for _, required := range []string{"name", "namespace", "version"} {
		if _, ok := seen[required]; !ok {
			return nil, fmt.Errorf("missing required top-level field %q", required)
		}
	}
	if name != expectedName {
		return nil, fmt.Errorf("Helm release name %q does not match expected %q", name, expectedName)
	}
	if namespace != expectedNamespace {
		return nil, fmt.Errorf("Helm release namespace %q does not match expected %q", namespace, expectedNamespace)
	}
	if version != expectedVersion {
		return nil, fmt.Errorf("Helm release version %d does not match expected %d", version, expectedVersion)
	}

	parts := append([]string{manifest}, hooks...)
	var output bytes.Buffer
	for index, part := range parts {
		if index != 0 {
			output.WriteString("\n---\n")
		}
		output.WriteString(part)
	}
	if output.Len() > maxRenderedManifestBytes {
		return nil, fmt.Errorf("extracted Helm manifest bytes exceed limit %d", maxRenderedManifestBytes)
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func decodeJSONStringToken(decoder *json.Decoder, field string) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	value, ok := token.(string)
	if !ok {
		return "", fmt.Errorf("field %q must be a string", field)
	}
	return value, nil
}

func decodeJSONUintToken(decoder *json.Decoder, field string) (uint64, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, err
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, fmt.Errorf("field %q must be an unsigned integer", field)
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q must be an unsigned integer", field)
	}
	return value, nil
}

func decodeHelmHookManifests(decoder *json.Decoder) ([]string, error) {
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '[' {
		return nil, fmt.Errorf("field %q must be an array", "hooks")
	}
	manifests := make([]string, 0)
	for decoder.More() {
		object, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if delimiter, ok := object.(json.Delim); !ok || delimiter != '{' {
			return nil, fmt.Errorf("hook %d must be an object", len(manifests))
		}
		seen := map[string]struct{}{}
		manifest := ""
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			field, ok := nameToken.(string)
			if !ok {
				return nil, fmt.Errorf("hook %d field name must be a string", len(manifests))
			}
			if _, duplicate := seen[field]; duplicate {
				return nil, fmt.Errorf("hook %d contains duplicate field %q", len(manifests), field)
			}
			seen[field] = struct{}{}
			switch field {
			case "manifest":
				manifest, err = decodeJSONStringToken(decoder, "hooks.manifest")
			case "events", "kind", "last_run", "name", "path", "weight", "delete_policies", "output_log_policies":
				var ignored json.RawMessage
				err = decoder.Decode(&ignored)
			default:
				return nil, fmt.Errorf("hook %d contains unknown field %q", len(manifests), field)
			}
			if err != nil {
				return nil, err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
			return nil, fmt.Errorf("hook %d object is not closed", len(manifests))
		}
		if _, ok := seen["manifest"]; !ok {
			return nil, fmt.Errorf("hook %d is missing manifest", len(manifests))
		}
		manifests = append(manifests, manifest)
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
		return nil, fmt.Errorf("hooks array is not closed")
	}
	return manifests, nil
}

func validateStrictJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	budget := &manifestDecodeBudget{}
	if err := validateStrictJSONValue(decoder, "$", 0, budget); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("trailing token %v", token)
	}
	return nil
}

func validateStrictJSONValue(decoder *json.Decoder, path string, depth int, budget *manifestDecodeBudget) error {
	if depth > maxRenderedManifestDepth {
		return fmt.Errorf("%s exceeds JSON nesting limit %d", path, maxRenderedManifestDepth)
	}
	budget.nodes++
	if budget.nodes > maxRenderedManifestNodes {
		return fmt.Errorf("JSON token count exceeds limit %d", maxRenderedManifestNodes)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("%s field name must be a string", path)
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("%s contains duplicate field %q", path, name)
			}
			seen[name] = struct{}{}
			childPath := boundedManifestDiagnosticPath(path, escapeJSONPointerToken(name))
			if err := validateStrictJSONValue(decoder, childPath, depth+1, budget); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("%s object is not closed", path)
		}
		return nil
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := validateStrictJSONValue(decoder, boundedManifestDiagnosticPath(path, strconv.Itoa(index)), depth+1, budget); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("%s array is not closed", path)
		}
		return nil
	default:
		return fmt.Errorf("%s has unexpected delimiter %q", path, delimiter)
	}
}
