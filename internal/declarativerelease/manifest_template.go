package declarativerelease

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

// MaterializeManifestTemplate replaces only reviewed registry variables in a
// shared JSON resource template. Values are validated by Component.Validate;
// unresolved or unused variables fail closed.
func MaterializeManifestTemplate(raw []byte, variables map[string]string) ([]byte, error) {
	return materializeManifestTemplate(raw, variables, true)
}

// MaterializePredecessorManifestTemplate renders a historical template with
// the current reviewed registry. Newly introduced variables may be absent from
// the predecessor template; every placeholder that does exist must still be
// resolved, and the current forward template remains strict about unused data.
func MaterializePredecessorManifestTemplate(raw []byte, variables map[string]string) ([]byte, error) {
	return materializeManifestTemplate(raw, variables, false)
}

func materializeManifestTemplate(raw []byte, variables map[string]string, rejectUnused bool) ([]byte, error) {
	if len(raw) == 0 || len(raw) > maxWorkloadManifestBytes {
		return nil, errors.New("component manifest template size is invalid")
	}
	result := append([]byte(nil), raw...)
	keys := make([]string, 0, len(variables))
	for key := range variables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !manifestVariableNamePattern.MatchString(key) || !manifestVariableValuePattern.MatchString(variables[key]) {
			return nil, fmt.Errorf("manifest variable %q is invalid", key)
		}
		placeholder := []byte("@@" + key + "@@")
		if !bytes.Contains(result, placeholder) && rejectUnused {
			return nil, fmt.Errorf("manifest variable %q is unused", key)
		}
		result = bytes.ReplaceAll(result, placeholder, []byte(variables[key]))
	}
	if bytes.Contains(result, []byte("@@")) {
		return nil, errors.New("component manifest contains an unresolved variable")
	}
	set, err := DecodeResourceSet(bytes.NewReader(result))
	if err != nil {
		return nil, fmt.Errorf("decode materialized component manifest: %w", err)
	}
	return CanonicalJSON(set)
}
