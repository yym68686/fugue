package releasedomain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"unicode/utf8"
)

// DecodeChangedFilesJSON reads the enriched, side-effect-free file evidence
// format consumed by the Boundary A CLI.
func DecodeChangedFilesJSON(reader io.Reader) ([]ChangedFile, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read changed files: %w", err)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("decode changed files: input contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return nil, fmt.Errorf("decode changed files: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	root, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("decode changed files: %w", err)
	}
	if delimiter, ok := root.(json.Delim); !ok || delimiter != '[' {
		return nil, fmt.Errorf("decode changed files: root must be a non-null array")
	}

	changes := make([]ChangedFile, 0)
	for decoder.More() {
		change, err := decodeChangedFileObject(decoder, len(changes))
		if err != nil {
			return nil, fmt.Errorf("decode changed files: %w", err)
		}
		changes = append(changes, change)
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("decode changed files: close root array: %w", err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
		return nil, fmt.Errorf("decode changed files: root array is not closed")
	}
	if trailing, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("decode trailing changed files data: %w", err)
		}
		return nil, fmt.Errorf("changed files must contain one JSON value; found trailing token %v", trailing)
	}
	return changes, nil
}

func decodeChangedFileObject(decoder *json.Decoder, index int) (ChangedFile, error) {
	opening, err := decoder.Token()
	if err != nil {
		return ChangedFile{}, fmt.Errorf("entry %d: %w", index, err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return ChangedFile{}, fmt.Errorf("entry %d must be a non-null object", index)
	}

	var change ChangedFile
	seen := make(map[string]struct{}, 6)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return ChangedFile{}, fmt.Errorf("entry %d field name: %w", index, err)
		}
		name, ok := nameToken.(string)
		if !ok {
			return ChangedFile{}, fmt.Errorf("entry %d field name must be a string", index)
		}
		if _, duplicate := seen[name]; duplicate {
			return ChangedFile{}, fmt.Errorf("entry %d contains duplicate field %q", index, name)
		}
		seen[name] = struct{}{}

		switch name {
		case "status":
			value, err := decodeChangedFileString(decoder, index, name)
			if err != nil {
				return ChangedFile{}, err
			}
			status, err := parseChangeStatus(value)
			if err != nil {
				return ChangedFile{}, fmt.Errorf("entry %d field %q: %w", index, name, err)
			}
			change.Status = status
		case "path":
			change.Path, err = decodeChangedFileString(decoder, index, name)
			if err != nil {
				return ChangedFile{}, err
			}
		case "valuePointers":
			change.ValuePointers, err = decodeChangedFileStringArray(decoder, index, name)
			if err != nil {
				return ChangedFile{}, err
			}
		case "consumerDomains":
			values, err := decodeChangedFileStringArray(decoder, index, name)
			if err != nil {
				return ChangedFile{}, err
			}
			change.ConsumerDomains = make([]Domain, len(values))
			for valueIndex, value := range values {
				change.ConsumerDomains[valueIndex] = Domain(value)
			}
		case "semanticDomains":
			values, err := decodeChangedFileStringArray(decoder, index, name)
			if err != nil {
				return ChangedFile{}, err
			}
			change.SemanticDomains = make([]Domain, len(values))
			for valueIndex, value := range values {
				change.SemanticDomains[valueIndex] = Domain(value)
			}
		case "outsideConsumers":
			change.OutsideConsumers, err = decodeChangedFileStringArray(decoder, index, name)
			if err != nil {
				return ChangedFile{}, err
			}
		default:
			return ChangedFile{}, fmt.Errorf("entry %d contains unknown field %q", index, name)
		}
	}

	closing, err := decoder.Token()
	if err != nil {
		return ChangedFile{}, fmt.Errorf("entry %d: close object: %w", index, err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return ChangedFile{}, fmt.Errorf("entry %d object is not closed", index)
	}
	for _, required := range []string{"status", "path"} {
		if _, present := seen[required]; !present {
			return ChangedFile{}, fmt.Errorf("entry %d is missing required field %q", index, required)
		}
	}
	return change, nil
}

func decodeChangedFileString(decoder *json.Decoder, index int, name string) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("entry %d field %q: %w", index, name, err)
	}
	value, ok := token.(string)
	if !ok {
		return "", fmt.Errorf("entry %d field %q must be a string", index, name)
	}
	return value, nil
}

func decodeChangedFileStringArray(decoder *json.Decoder, index int, name string) ([]string, error) {
	opening, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("entry %d field %q: %w", index, name, err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '[' {
		return nil, fmt.Errorf("entry %d field %q must be a non-null array of strings", index, name)
	}
	values := make([]string, 0)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("entry %d field %q: %w", index, name, err)
		}
		value, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("entry %d field %q must contain only strings", index, name)
		}
		values = append(values, value)
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("entry %d field %q: close array: %w", index, name, err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
		return nil, fmt.Errorf("entry %d field %q array is not closed", index, name)
	}
	return values, nil
}

// encoding/json deliberately replaces malformed UTF-16 surrogate escapes with
// U+FFFD. Evidence must instead fail closed, so validate every JSON string's
// escapes before the decoder has a chance to normalize them.
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
						return fmt.Errorf("invalid JSON escape \\%c", escape)
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

// ParseNameStatusZ parses `git diff --no-renames --name-status -z`. Runtime Go
// files and versioned values parsed this way intentionally remain unknown until
// their dependency or leaf-pointer evidence is supplied in enriched JSON.
func ParseNameStatusZ(reader io.Reader) ([]ChangedFile, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read name-status: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	if data[len(data)-1] != 0 {
		return nil, fmt.Errorf("name-status input must be NUL terminated")
	}
	parts := bytes.Split(data[:len(data)-1], []byte{0})
	changes := make([]ChangedFile, 0, len(parts)/2)
	for index := 0; index < len(parts); {
		if !utf8.Valid(parts[index]) {
			return nil, fmt.Errorf("name-status contains invalid UTF-8 status")
		}
		token := string(parts[index])
		index++
		statusToken := token
		pathToken := ""
		if before, after, found := strings.Cut(token, "\t"); found {
			statusToken = before
			pathToken = after
		} else {
			if index >= len(parts) {
				return nil, fmt.Errorf("name-status entry %q is missing a path", statusToken)
			}
			if !utf8.Valid(parts[index]) {
				return nil, fmt.Errorf("name-status contains invalid UTF-8 path")
			}
			pathToken = string(parts[index])
			index++
		}
		status, err := parseChangeStatus(statusToken)
		if err != nil {
			return nil, err
		}
		changes = append(changes, ChangedFile{Status: status, Path: pathToken})
	}
	return changes, nil
}

func parseChangeStatus(value string) (ChangeStatus, error) {
	switch ChangeStatus(value) {
	case ChangeAdded, ChangeModified, ChangeDeleted:
		return ChangeStatus(value), nil
	default:
		return "", fmt.Errorf("unsupported name-status %q; rename/copy/type changes are fail-closed", value)
	}
}

// These authority-defining paths are intentionally outside the ownership
// document they protect. A policy edit, planner edit, or activation-wiring edit
// must never make itself eligible by changing a YAML rule in the same release.
//
// The planner implementation is permanently reserved as well. The initial
// additive foundation is introduced through the legacy, pre-activation release
// lane; once present, planner code must never be able to authorize changes to
// itself through a policy edit in the same release.
var reservedBootstrapExactPaths = map[string]string{
	"Makefile": "repository release orchestration is reserved bootstrap wiring",
	"scripts/lib/control_plane_release_domains.sh":    "release-domain adapter wiring cannot authorize itself",
	"scripts/test_release_domain_safety.sh":           "release-domain safety gate wiring cannot authorize itself",
	"scripts/test_single_domain_release.sh":           "release-domain activation gate wiring cannot authorize itself",
	"scripts/upgrade_fugue_control_plane.sh":          "release entrypoint wiring cannot authorize itself",
	".github/workflows/deploy-control-plane.yml":      "control-plane release workflow wiring cannot authorize itself",
	".github/workflows/release-public-data-plane.yml": "public data-plane release workflow wiring cannot authorize itself",
}

var reservedBootstrapPrefixes = []struct {
	prefix string
	reason string
}{
	{prefix: "deploy/release-domains/", reason: "release-domain ownership policy cannot authorize itself"},
	{prefix: "cmd/fugue-release-domain-plan/", reason: "release-domain planner command cannot authorize itself"},
	{prefix: "cmd/fugue-release-domain-evidence/", reason: "release-domain evidence producer cannot authorize itself"},
	{prefix: "internal/releasedomain/", reason: "release-domain planner implementation cannot authorize itself"},
	{prefix: "scripts/release-domains/", reason: "future release-domain activation wiring cannot authorize itself"},
}

func reservedBootstrapPath(repositoryPath string) (string, bool) {
	if reason, reserved := reservedBootstrapExactPaths[repositoryPath]; reserved {
		return reason, true
	}
	for _, reservation := range reservedBootstrapPrefixes {
		if strings.HasPrefix(repositoryPath, reservation.prefix) {
			return reservation.reason, true
		}
	}
	return "", false
}

// ClassifyFiles applies declarative path rules and supplied dependency/values
// evidence. Anything not positively classified is unknown.
func ClassifyFiles(changes []ChangedFile, spec *OwnershipSpec) FileClassification {
	classification := FileClassification{AllNonRuntime: true}
	if spec == nil {
		classification.AllNonRuntime = false
		classification.Unknown = []Evidence{{
			Source:  "changed-file",
			Subject: "ownership",
			Reason:  "ownership is nil",
		}}
		return classification
	}

	domainSet := map[Domain]struct{}{}
	seenPaths := map[string]struct{}{}
	for _, change := range changes {
		status := string(change.Status)
		if _, err := parseChangeStatus(status); err != nil {
			classification.AllNonRuntime = false
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Reason: err.Error(),
			})
			continue
		}
		if !validRepositoryPath(change.Path) {
			classification.AllNonRuntime = false
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Reason: "invalid repository-relative UTF-8 path",
			})
			continue
		}
		if _, exists := seenPaths[change.Path]; exists {
			classification.AllNonRuntime = false
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Reason: "duplicate changed path",
			})
			continue
		}
		seenPaths[change.Path] = struct{}{}

		if reason, reserved := reservedBootstrapPath(change.Path); reserved {
			classification.AllNonRuntime = false
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Reason: reason,
			})
			continue
		}

		if hasValueRulesForPath(change.Path, spec.ValueRules) {
			classification.AllNonRuntime = false
			classifyValueFile(change, spec.ValueRules, domainSet, &classification)
			continue
		}

		matchedRules := make([]FileRule, 0)
		for _, rule := range spec.FileRules {
			if pathSelectorMatches(change.Path, rule.Exact, rule.Prefix, rule.Suffix, rule.Glob) {
				matchedRules = append(matchedRules, rule)
			}
		}
		if len(matchedRules) > 0 {
			classifyPathRules(change, matchedRules, domainSet, &classification)
			continue
		}

		if strings.HasSuffix(change.Path, ".go") {
			classification.AllNonRuntime = false
			classifyGoFile(change, domainSet, &classification)
			continue
		}

		classification.AllNonRuntime = false
		classification.Unknown = append(classification.Unknown, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status, Reason: "no ownership rule matched",
		})
	}

	classification.Domains = domainsFromSet(domainSet)
	classification.Evidence = canonicalEvidence(classification.Evidence)
	classification.Unknown = canonicalEvidence(classification.Unknown)
	return classification
}

func classifyPathRules(change ChangedFile, rules []FileRule, domainSet map[Domain]struct{}, classification *FileClassification) {
	status := string(change.Status)
	matchedDomains := map[Domain]struct{}{}
	nonRuntime := false
	hasUnknown := false
	for _, rule := range rules {
		switch {
		case rule.UnknownReason != "":
			hasUnknown = true
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, RuleID: rule.ID, Reason: rule.UnknownReason,
			})
		case rule.NonRuntime:
			nonRuntime = true
		case len(rule.Domains) > 0:
			for _, domain := range rule.Domains {
				matchedDomains[domain] = struct{}{}
			}
		}
	}
	if hasUnknown {
		classification.AllNonRuntime = false
		return
	}
	if nonRuntime && len(matchedDomains) > 0 {
		classification.AllNonRuntime = false
		classification.Unknown = append(classification.Unknown, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status, Reason: "overlapping rules classify the path as both runtime and non-runtime",
		})
		return
	}
	if nonRuntime {
		classification.Evidence = append(classification.Evidence, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status, Reason: "proven non-runtime",
		})
		return
	}
	classification.AllNonRuntime = false
	domains := domainsFromSet(matchedDomains)
	for _, domain := range domains {
		domainSet[domain] = struct{}{}
	}
	classification.Evidence = append(classification.Evidence, Evidence{
		Source: "changed-file", Subject: change.Path, Status: status, Domains: domains,
	})
}

func classifyGoFile(change ChangedFile, domainSet map[Domain]struct{}, classification *FileClassification) {
	status := string(change.Status)
	invalid := make([]string, 0)
	localDomains := map[Domain]struct{}{}
	for _, domain := range append(append([]Domain(nil), change.ConsumerDomains...), change.SemanticDomains...) {
		if _, err := ParseDomain(string(domain)); err != nil {
			invalid = append(invalid, string(domain))
			continue
		}
		localDomains[domain] = struct{}{}
	}
	if len(invalid) > 0 {
		sort.Strings(invalid)
		classification.Unknown = append(classification.Unknown, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status, Reason: "invalid Go consumer domains: " + strings.Join(invalid, ", "),
		})
		return
	}
	if len(change.ConsumerDomains) == 0 {
		classification.Unknown = append(classification.Unknown, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status, Reason: "runtime Go file requires package-to-binary consumer evidence",
		})
		return
	}
	if len(change.OutsideConsumers) > 0 {
		outside := append([]string(nil), change.OutsideConsumers...)
		sort.Strings(outside)
		classification.Unknown = append(classification.Unknown, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status,
			Domains: domainsFromSet(localDomains), Reason: "Go package reaches out-of-domain consumers: " + strings.Join(outside, ", "),
		})
		return
	}
	domains := domainsFromSet(localDomains)
	for _, domain := range domains {
		domainSet[domain] = struct{}{}
	}
	classification.Evidence = append(classification.Evidence, Evidence{
		Source: "changed-file", Subject: change.Path, Status: status, Domains: domains,
		Reason: "dependency and semantic consumer closure",
	})
}

func hasValueRulesForPath(repositoryPath string, rules []ValueRule) bool {
	for _, rule := range rules {
		if pathSelectorMatches(repositoryPath, rule.Exact, rule.Prefix, rule.Suffix, rule.Glob) {
			return true
		}
	}
	return false
}

func classifyValueFile(change ChangedFile, rules []ValueRule, domainSet map[Domain]struct{}, classification *FileClassification) {
	status := string(change.Status)
	if len(change.ValuePointers) == 0 {
		classification.Unknown = append(classification.Unknown, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status, Reason: "versioned values file requires changed leaf JSON Pointer evidence",
		})
		return
	}
	seenPointers := map[string]struct{}{}
	for _, pointer := range change.ValuePointers {
		if _, exists := seenPointers[pointer]; exists {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Paths: []string{pointer}, Reason: "duplicate values pointer",
			})
			continue
		}
		seenPointers[pointer] = struct{}{}
		if err := validateJSONPointer(pointer); err != nil {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Paths: []string{pointer}, Reason: err.Error(),
			})
			continue
		}
		longest := -1
		matches := make([]ValueRule, 0)
		for _, rule := range rules {
			if !pathSelectorMatches(change.Path, rule.Exact, rule.Prefix, rule.Suffix, rule.Glob) || !pointerHasPrefix(pointer, rule.Pointer) {
				continue
			}
			depth := pointerDepth(rule.Pointer)
			if depth > longest {
				longest = depth
				matches = matches[:0]
			}
			if depth == longest {
				matches = append(matches, rule)
			}
		}
		if len(matches) == 0 {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Paths: []string{pointer}, Reason: "values leaf has no owner",
			})
			continue
		}
		first := matches[0]
		conflict := false
		for _, candidate := range matches[1:] {
			if candidate.Domain != first.Domain || candidate.UnknownReason != first.UnknownReason {
				conflict = true
				break
			}
		}
		if conflict {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Paths: []string{pointer}, Reason: "values ownership rules conflict at equal specificity",
			})
			continue
		}
		if first.UnknownReason != "" {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "changed-file", Subject: change.Path, Status: status, Paths: []string{pointer}, RuleID: first.ID, Reason: first.UnknownReason,
			})
			continue
		}
		domainSet[first.Domain] = struct{}{}
		classification.Evidence = append(classification.Evidence, Evidence{
			Source: "changed-file", Subject: change.Path, Status: status, Paths: []string{pointer}, Domains: []Domain{first.Domain}, RuleID: first.ID,
		})
	}
}

func validRepositoryPath(value string) bool {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) || strings.Contains(value, "\\") || path.IsAbs(value) {
		return false
	}
	cleaned := path.Clean(value)
	return cleaned == value && cleaned != "." && cleaned != ".." && !strings.HasPrefix(cleaned, "../")
}

func domainsFromSet(values map[Domain]struct{}) []Domain {
	domains := make([]Domain, 0, len(values))
	for domain := range values {
		domains = append(domains, domain)
	}
	return canonicalDomains(domains)
}
