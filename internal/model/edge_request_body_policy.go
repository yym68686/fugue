package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
)

const AppEdgeRequestBodyPoliciesEnv = "FUGUE_EDGE_REQUEST_BODY_POLICIES"

const (
	maxEdgeRequestBodyPolicies                = 16
	maxEdgeRequestBodyPolicyPaths             = 64
	maxEdgeRequestBodyPolicyMethods           = 16
	maxEdgeRequestBodyPolicyBytes       int64 = 1 << 40
	maxEdgeRequestBodyTimeoutSeconds          = 24 * 60 * 60
	maxEdgeRequestBodyConcurrency             = 1024
	maxEdgeRequestBodyRetryAfterSeconds       = 60 * 60
)

// EdgeRequestBodyPolicy is an app-scoped, exact-path ingress policy carried in
// the signed edge route bundle. Policies are deliberately opt-in: routes with
// no policy retain the existing streaming proxy behavior.
type EdgeRequestBodyPolicy struct {
	Name              string   `json:"name"`
	Methods           []string `json:"methods"`
	Paths             []string `json:"paths"`
	MaxBytes          int64    `json:"max_bytes"`
	TimeoutSeconds    int      `json:"timeout_seconds"`
	MaxConcurrent     int      `json:"max_concurrent"`
	RetryAfterSeconds int      `json:"retry_after_seconds,omitempty"`
}

// ParseEdgeRequestBodyPolicies parses the app-owned ingress policy metadata.
// Invalid opt-in metadata is an error so callers can fail the route closed
// instead of silently dropping a requested safety boundary.
func ParseEdgeRequestBodyPolicies(raw string) ([]EdgeRequestBodyPolicy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var policies []EdgeRequestBodyPolicy
	if err := decoder.Decode(&policies); err != nil {
		return nil, fmt.Errorf("decode %s: %w", AppEdgeRequestBodyPoliciesEnv, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode %s: %w", AppEdgeRequestBodyPoliciesEnv, err)
	}
	if len(policies) == 0 {
		return nil, fmt.Errorf("%s must contain at least one policy", AppEdgeRequestBodyPoliciesEnv)
	}
	if len(policies) > maxEdgeRequestBodyPolicies {
		return nil, fmt.Errorf("%s supports at most %d policies", AppEdgeRequestBodyPoliciesEnv, maxEdgeRequestBodyPolicies)
	}

	names := make(map[string]struct{}, len(policies))
	matches := make(map[string]string)
	for index := range policies {
		policy := &policies[index]
		policy.Name = strings.TrimSpace(policy.Name)
		if !validEdgeRequestBodyPolicyName(policy.Name) {
			return nil, fmt.Errorf("%s policy %d has an invalid name", AppEdgeRequestBodyPoliciesEnv, index)
		}
		if _, exists := names[policy.Name]; exists {
			return nil, fmt.Errorf("%s policy name %q is duplicated", AppEdgeRequestBodyPoliciesEnv, policy.Name)
		}
		names[policy.Name] = struct{}{}

		if policy.MaxBytes <= 0 || policy.MaxBytes > maxEdgeRequestBodyPolicyBytes {
			return nil, fmt.Errorf("%s policy %q max_bytes must be between 1 and %d", AppEdgeRequestBodyPoliciesEnv, policy.Name, maxEdgeRequestBodyPolicyBytes)
		}
		if policy.TimeoutSeconds <= 0 || policy.TimeoutSeconds > maxEdgeRequestBodyTimeoutSeconds {
			return nil, fmt.Errorf("%s policy %q timeout_seconds must be between 1 and %d", AppEdgeRequestBodyPoliciesEnv, policy.Name, maxEdgeRequestBodyTimeoutSeconds)
		}
		if policy.MaxConcurrent <= 0 || policy.MaxConcurrent > maxEdgeRequestBodyConcurrency {
			return nil, fmt.Errorf("%s policy %q max_concurrent must be between 1 and %d", AppEdgeRequestBodyPoliciesEnv, policy.Name, maxEdgeRequestBodyConcurrency)
		}
		if policy.RetryAfterSeconds == 0 {
			policy.RetryAfterSeconds = 5
		}
		if policy.RetryAfterSeconds < 1 || policy.RetryAfterSeconds > maxEdgeRequestBodyRetryAfterSeconds {
			return nil, fmt.Errorf("%s policy %q retry_after_seconds must be between 1 and %d", AppEdgeRequestBodyPoliciesEnv, policy.Name, maxEdgeRequestBodyRetryAfterSeconds)
		}

		methods, err := normalizeEdgeRequestBodyPolicyMethods(policy.Methods)
		if err != nil {
			return nil, fmt.Errorf("%s policy %q: %w", AppEdgeRequestBodyPoliciesEnv, policy.Name, err)
		}
		paths, err := normalizeEdgeRequestBodyPolicyPaths(policy.Paths)
		if err != nil {
			return nil, fmt.Errorf("%s policy %q: %w", AppEdgeRequestBodyPoliciesEnv, policy.Name, err)
		}
		policy.Methods = methods
		policy.Paths = paths

		for _, method := range methods {
			for _, requestPath := range paths {
				key := method + " " + requestPath
				if previous, exists := matches[key]; exists {
					return nil, fmt.Errorf("%s policies %q and %q overlap at %s", AppEdgeRequestBodyPoliciesEnv, previous, policy.Name, key)
				}
				matches[key] = policy.Name
			}
		}
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Name < policies[j].Name
	})
	return policies, nil
}

func MatchEdgeRequestBodyPolicy(policies []EdgeRequestBodyPolicy, method, requestPath string) (EdgeRequestBodyPolicy, bool) {
	method = strings.ToUpper(strings.TrimSpace(method))
	if requestPath == "" {
		requestPath = "/"
	}
	for _, policy := range policies {
		if !sortedStringSliceContains(policy.Methods, method) || !sortedStringSliceContains(policy.Paths, requestPath) {
			continue
		}
		return policy, true
	}
	return EdgeRequestBodyPolicy{}, false
}

func CloneEdgeRequestBodyPolicies(policies []EdgeRequestBodyPolicy) []EdgeRequestBodyPolicy {
	if len(policies) == 0 {
		return nil
	}
	cloned := make([]EdgeRequestBodyPolicy, len(policies))
	for index, policy := range policies {
		cloned[index] = policy
		cloned[index].Methods = append([]string(nil), policy.Methods...)
		cloned[index].Paths = append([]string(nil), policy.Paths...)
	}
	return cloned
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("unexpected trailing JSON value")
}

func validEdgeRequestBodyPolicyName(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case character == '-', character == '_', character == '.':
		default:
			return false
		}
	}
	return true
}

func normalizeEdgeRequestBodyPolicyMethods(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > maxEdgeRequestBodyPolicyMethods {
		return nil, fmt.Errorf("methods must contain between 1 and %d entries", maxEdgeRequestBodyPolicyMethods)
	}
	seen := make(map[string]struct{}, len(values))
	methods := make([]string, 0, len(values))
	for _, value := range values {
		method := strings.ToUpper(strings.TrimSpace(value))
		if !validHTTPToken(method) {
			return nil, fmt.Errorf("method %q is invalid", value)
		}
		if _, exists := seen[method]; exists {
			continue
		}
		seen[method] = struct{}{}
		methods = append(methods, method)
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("methods must not be empty")
	}
	sort.Strings(methods)
	return methods, nil
}

func normalizeEdgeRequestBodyPolicyPaths(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > maxEdgeRequestBodyPolicyPaths {
		return nil, fmt.Errorf("paths must contain between 1 and %d entries", maxEdgeRequestBodyPolicyPaths)
	}
	seen := make(map[string]struct{}, len(values))
	paths := make([]string, 0, len(values))
	for _, value := range values {
		requestPath := strings.TrimSpace(value)
		if requestPath == "" || !strings.HasPrefix(requestPath, "/") || strings.ContainsAny(requestPath, "?#") || path.Clean(requestPath) != requestPath {
			return nil, fmt.Errorf("path %q must be an absolute, normalized exact path without a query or fragment", value)
		}
		if _, exists := seen[requestPath]; exists {
			continue
		}
		seen[requestPath] = struct{}{}
		paths = append(paths, requestPath)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("paths must not be empty")
	}
	sort.Strings(paths)
	return paths, nil
}

func validHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character >= 0x7f || strings.ContainsRune("()<>@,;:\\\"/[]?={}\t", character) {
			return false
		}
	}
	return true
}

func sortedStringSliceContains(values []string, value string) bool {
	index := sort.SearchStrings(values, value)
	return index < len(values) && values[index] == value
}
