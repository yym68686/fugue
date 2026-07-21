package releasecontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MultiDomainContractAPIVersion = "release-domain.fugue.dev/v2"
	CompositeReleasePlanKind      = "CompositeReleasePlan"
	CompositeReleasePlanPolicy    = "evidence-derived-composite-saga-v1"
	maxCompositePlanBytes         = 8 << 20
)

// CompositeReleasePlan is a dormant, evidence-only contract. It does not
// authorize or execute a mutation. A TransactionEnvelope v2 consumer must
// independently bind ImageActivationPlanDigest before the first write.
type CompositeReleasePlan struct {
	APIVersion                string                 `json:"apiVersion"`
	Kind                      string                 `json:"kind"`
	Policy                    string                 `json:"policy"`
	BaseCommit                string                 `json:"baseCommit"`
	TargetCommit              string                 `json:"targetCommit"`
	ImageActivationPlanDigest string                 `json:"imageActivationPlanDigest"`
	Generation                string                 `json:"generation"`
	FencingEpoch              string                 `json:"fencingEpoch"`
	BaseVersions              []DomainVersion        `json:"baseVersions"`
	TargetVersions            []DomainVersion        `json:"targetVersions"`
	Steps                     []CompositeReleaseStep `json:"steps"`
	Digest                    string                 `json:"digest"`
}

type DomainVersion struct {
	Domain  Domain `json:"domain"`
	Version string `json:"version"`
}

type CompositeReleaseStep struct {
	ID                    string                     `json:"id"`
	Domain                Domain                     `json:"domain"`
	Adapter               string                     `json:"adapter"`
	DependsOn             []string                   `json:"dependsOn"`
	ActivationIDs         []string                   `json:"activationIds"`
	BaseVersion           string                     `json:"baseVersion"`
	TargetVersion         string                     `json:"targetVersion"`
	ForwardRenderedDigest string                     `json:"forwardRenderedDigest"`
	ReverseRenderedDigest string                     `json:"reverseRenderedDigest"`
	Observation           CompositeObservationPolicy `json:"observation"`
	RollbackBudgetSeconds string                     `json:"rollbackBudgetSeconds"`
}

type CompositeObservationPolicy struct {
	HealthEvidenceDigest string `json:"healthEvidenceDigest"`
	MinimumSamples       string `json:"minimumSamples"`
	WindowSeconds        string `json:"windowSeconds"`
}

func NewCompositeReleasePlan(plan CompositeReleasePlan) (CompositeReleasePlan, error) {
	plan.APIVersion = MultiDomainContractAPIVersion
	plan.Kind = CompositeReleasePlanKind
	plan.Policy = CompositeReleasePlanPolicy
	plan.BaseVersions = canonicalDomainVersions(plan.BaseVersions)
	plan.TargetVersions = canonicalDomainVersions(plan.TargetVersions)
	plan.Steps = CloneCompositeSteps(plan.Steps)
	for index := range plan.Steps {
		dependencies := canonicalStrings(plan.Steps[index].DependsOn)
		activations := canonicalStrings(plan.Steps[index].ActivationIDs)
		if len(dependencies) != len(plan.Steps[index].DependsOn) || len(activations) != len(plan.Steps[index].ActivationIDs) {
			return CompositeReleasePlan{}, fmt.Errorf("composite step dependencies or activations are duplicated")
		}
		plan.Steps[index].DependsOn = dependencies
		plan.Steps[index].ActivationIDs = activations
	}
	plan.Digest = DigestCompositeReleasePlan(plan)
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		return CompositeReleasePlan{}, err
	}
	return plan, nil
}

func VerifyCompositeReleasePlan(plan CompositeReleasePlan) error {
	if plan.APIVersion != MultiDomainContractAPIVersion || plan.Kind != CompositeReleasePlanKind || plan.Policy != CompositeReleasePlanPolicy {
		return fmt.Errorf("composite release plan identity is unsupported")
	}
	if err := validateRevisionPair(plan.BaseCommit, plan.TargetCommit); err != nil {
		return err
	}
	if err := validateDigest(plan.ImageActivationPlanDigest, "composite image activation plan digest"); err != nil {
		return err
	}
	if !validPositiveDecimal(plan.Generation) || !validPositiveDecimal(plan.FencingEpoch) {
		return fmt.Errorf("composite generation and fencing epoch must be positive")
	}
	baseDomains, err := verifyDomainVersionVector(plan.BaseVersions, "base")
	if err != nil {
		return err
	}
	targetDomains, err := verifyDomainVersionVector(plan.TargetVersions, "target")
	if err != nil {
		return err
	}
	if len(baseDomains) < 2 || !equalDomains(baseDomains, targetDomains) {
		return fmt.Errorf("composite version vectors must contain the same two or more domains")
	}
	if len(plan.Steps) < 2 {
		return fmt.Errorf("composite release plan requires at least two ordered steps")
	}
	knownIDs := map[string]struct{}{}
	activationIDs := map[string]struct{}{}
	stepDomains := map[Domain]struct{}{}
	vectorDomains := map[Domain]struct{}{}
	baseVersionByDomain := map[Domain]string{}
	targetVersionByDomain := map[Domain]string{}
	for index, domain := range baseDomains {
		vectorDomains[domain] = struct{}{}
		baseVersionByDomain[domain] = plan.BaseVersions[index].Version
		targetVersionByDomain[domain] = plan.TargetVersions[index].Version
	}
	for _, step := range plan.Steps {
		if !validIdentifier(step.ID) {
			return fmt.Errorf("composite step identity is invalid")
		}
		if _, duplicate := knownIDs[step.ID]; duplicate {
			return fmt.Errorf("composite step identity is duplicated")
		}
		if _, ok := vectorDomains[step.Domain]; !ok {
			return fmt.Errorf("composite step domain is absent from the version vector")
		}
		if expected, ok := AdapterForDomain(step.Domain); !ok || step.Adapter != expected {
			return fmt.Errorf("composite step adapter does not match its fixed domain")
		}
		if !reflect.DeepEqual(step.DependsOn, canonicalStrings(step.DependsOn)) ||
			!reflect.DeepEqual(step.ActivationIDs, canonicalStrings(step.ActivationIDs)) {
			return fmt.Errorf("composite step dependencies or activations are not canonical")
		}
		for _, dependency := range step.DependsOn {
			if _, ok := knownIDs[dependency]; !ok {
				return fmt.Errorf("composite step dependency must reference an earlier step")
			}
		}
		for _, activationID := range step.ActivationIDs {
			if !validIdentifier(activationID) {
				return fmt.Errorf("composite activation identity is invalid")
			}
			if _, duplicate := activationIDs[activationID]; duplicate {
				return fmt.Errorf("composite activation identity is assigned more than once")
			}
			activationIDs[activationID] = struct{}{}
		}
		for label, digest := range map[string]string{
			"base version":     step.BaseVersion,
			"target version":   step.TargetVersion,
			"forward rendered": step.ForwardRenderedDigest,
			"reverse rendered": step.ReverseRenderedDigest,
			"health evidence":  step.Observation.HealthEvidenceDigest,
		} {
			if err := validateDigest(digest, "composite step "+label+" digest"); err != nil {
				return err
			}
		}
		if step.BaseVersion != baseVersionByDomain[step.Domain] || step.TargetVersion != targetVersionByDomain[step.Domain] {
			return fmt.Errorf("composite step versions do not match the domain-version vectors")
		}
		if !validPositiveDecimal(step.Observation.MinimumSamples) || !validPositiveDecimal(step.Observation.WindowSeconds) ||
			!validPositiveDecimal(step.RollbackBudgetSeconds) {
			return fmt.Errorf("composite observation and rollback budgets must be positive")
		}
		knownIDs[step.ID] = struct{}{}
		stepDomains[step.Domain] = struct{}{}
	}
	if len(stepDomains) < 2 {
		return fmt.Errorf("composite steps must cover at least two domains")
	}
	for domain := range vectorDomains {
		if _, ok := stepDomains[domain]; !ok {
			return fmt.Errorf("composite version-vector domain has no step")
		}
	}
	if err := validateDigest(plan.Digest, "composite release plan digest"); err != nil {
		return err
	}
	if plan.Digest != DigestCompositeReleasePlan(plan) {
		return fmt.Errorf("composite release plan digest mismatch")
	}
	return nil
}

func MarshalCompositeReleasePlan(plan CompositeReleasePlan) ([]byte, error) {
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		return nil, err
	}
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(encoded)+1 > maxCompositePlanBytes {
		return nil, fmt.Errorf("composite release plan exceeds %d-byte limit", maxCompositePlanBytes)
	}
	return append(encoded, '\n'), nil
}

func DecodeAndVerifyCompositeReleasePlan(reader io.Reader, expectedDigest string) (CompositeReleasePlan, error) {
	if err := validateDigest(expectedDigest, "expected composite release plan digest"); err != nil {
		return CompositeReleasePlan{}, err
	}
	data, err := readCompositePlan(reader)
	if err != nil {
		return CompositeReleasePlan{}, err
	}
	if err := validateStrictCompositePlanJSON(data); err != nil {
		return CompositeReleasePlan{}, fmt.Errorf("decode composite release plan: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var plan CompositeReleasePlan
	if err := decoder.Decode(&plan); err != nil {
		return CompositeReleasePlan{}, fmt.Errorf("decode composite release plan: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return CompositeReleasePlan{}, err
	}
	if plan.Digest != expectedDigest {
		return CompositeReleasePlan{}, fmt.Errorf("composite release plan external digest mismatch")
	}
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		return CompositeReleasePlan{}, err
	}
	return plan, nil
}

// CloneCompositeSteps copies all slice-owned step fields.
func CloneCompositeSteps(values []CompositeReleaseStep) []CompositeReleaseStep {
	result := append([]CompositeReleaseStep(nil), values...)
	for index := range result {
		result[index].DependsOn = cloneStringsPreservingNil(result[index].DependsOn)
		result[index].ActivationIDs = cloneStringsPreservingNil(result[index].ActivationIDs)
	}
	if result == nil {
		return []CompositeReleaseStep{}
	}
	return result
}

func cloneStringsPreservingNil(values []string) []string {
	if values == nil {
		return nil
	}
	result := make([]string, len(values))
	copy(result, values)
	return result
}

// DigestCompositeReleasePlan returns the canonical sealed digest.
func DigestCompositeReleasePlan(plan CompositeReleasePlan) string {
	plan.Digest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal composite release plan: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func canonicalDomainVersions(values []DomainVersion) []DomainVersion {
	result := append([]DomainVersion(nil), values...)
	sort.Slice(result, func(left, right int) bool {
		leftRank, _ := DomainRank(result[left].Domain)
		rightRank, _ := DomainRank(result[right].Domain)
		return leftRank < rightRank
	})
	if result == nil {
		return []DomainVersion{}
	}
	return result
}

func verifyDomainVersionVector(values []DomainVersion, label string) ([]Domain, error) {
	if !reflect.DeepEqual(values, canonicalDomainVersions(values)) {
		return nil, fmt.Errorf("composite %s version vector is not canonical", label)
	}
	domains := make([]Domain, 0, len(values))
	for index, value := range values {
		if _, err := ParseDomain(string(value.Domain)); err != nil {
			return nil, fmt.Errorf("composite %s version domain: %w", label, err)
		}
		if index > 0 && values[index-1].Domain == value.Domain {
			return nil, fmt.Errorf("composite %s version domain is duplicated", label)
		}
		if err := validateDigest(value.Version, "composite "+label+" version digest"); err != nil {
			return nil, err
		}
		domains = append(domains, value.Domain)
	}
	return domains, nil
}

func equalDomains(left, right []Domain) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func canonicalStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	compacted := result[:0]
	for _, value := range result {
		if len(compacted) == 0 || compacted[len(compacted)-1] != value {
			compacted = append(compacted, value)
		}
	}
	if compacted == nil {
		return []string{}
	}
	return compacted
}

func validateRevisionPair(baseCommit, targetCommit string) error {
	if err := validateGitCommit(baseCommit, "composite release plan base commit"); err != nil {
		return err
	}
	if err := validateGitCommit(targetCommit, "composite release plan target commit"); err != nil {
		return err
	}
	if baseCommit == targetCommit {
		return fmt.Errorf("composite release plan base and target commits must differ")
	}
	return nil
}

func validateGitCommit(value, label string) error {
	if !utf8.ValidString(value) || len(value) != 40 {
		return fmt.Errorf("%s must be exact lowercase 40-hex", label)
	}
	for _, digit := range value {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return fmt.Errorf("%s must be exact lowercase 40-hex", label)
		}
	}
	return nil
}

func validateDigest(value, label string) error {
	if !utf8.ValidString(value) || len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return fmt.Errorf("%s must be lowercase sha256:<64-hex>", label)
	}
	for _, digit := range value[len("sha256:"):] {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return fmt.Errorf("%s must be lowercase sha256:<64-hex>", label)
		}
	}
	return nil
}

func validIdentifier(value string) bool {
	if !validText(value, 128) {
		return false
	}
	for index, char := range value {
		if index == 0 && (char < 'a' || char > 'z') {
			return false
		}
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func validText(value string, maximum int) bool {
	if !utf8.ValidString(value) || value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	return strings.IndexFunc(value, unicode.IsSpace) == -1
}

func validPositiveDecimal(value string) bool {
	if len(value) == 0 || len(value) > 20 || value[0] < '1' || value[0] > '9' {
		return false
	}
	for _, digit := range value[1:] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed > 0
}

func readCompositePlan(reader io.Reader) ([]byte, error) {
	if isNilReader(reader) {
		return nil, fmt.Errorf("composite release plan reader is nil")
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxCompositePlanBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read composite release plan: %w", err)
	}
	if len(data) > maxCompositePlanBytes {
		return nil, fmt.Errorf("composite release plan exceeds %d-byte limit", maxCompositePlanBytes)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("composite release plan contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return nil, fmt.Errorf("decode composite release plan: %w", err)
	}
	return data, nil
}

func isNilReader(reader io.Reader) bool {
	if reader == nil {
		return true
	}
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validateStrictCompositePlanJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValueForType(decoder, reflect.TypeOf(CompositeReleasePlan{}), "composite release plan"); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("JSON has trailing content")
	}
	return nil
}

func validateJSONValueForType(decoder *json.Decoder, expected reflect.Type, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	switch expected.Kind() {
	case reflect.Struct:
		opening, ok := token.(json.Delim)
		if !ok || opening != '{' {
			return fmt.Errorf("%s must be a non-null object", path)
		}
		fields := make(map[string]reflect.StructField, expected.NumField())
		required := make(map[string]bool, expected.NumField())
		for index := 0; index < expected.NumField(); index++ {
			field := expected.Field(index)
			name := jsonFieldName(field)
			if field.PkgPath != "" || name == "-" {
				continue
			}
			fields[name] = field
			_, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			required[name] = !strings.Contains(","+options+",", ",omitempty,")
		}
		seen := make(map[string]struct{}, len(fields))
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("%s field name: %w", path, err)
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("%s field name must be a string", path)
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("%s contains duplicate field %q", path, name)
			}
			seen[name] = struct{}{}
			field, known := fields[name]
			if !known {
				return fmt.Errorf("%s contains unknown field %q", path, name)
			}
			if err := validateJSONValueForType(decoder, field.Type, joinJSONPath(path, name)); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%s: close object: %w", path, err)
		}
		if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
			return fmt.Errorf("%s object is not closed", path)
		}
		for name, isRequired := range required {
			if _, present := seen[name]; isRequired && !present {
				return fmt.Errorf("%s is missing required field %q", path, name)
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		opening, ok := token.(json.Delim)
		if !ok || opening != '[' {
			return fmt.Errorf("%s must be an array", path)
		}
		index := 0
		for decoder.More() {
			if err := validateJSONValueForType(decoder, expected.Elem(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
			index++
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%s: close array: %w", path, err)
		}
		if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
			return fmt.Errorf("%s array is not closed", path)
		}
		return nil
	case reflect.String:
		if _, ok := token.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
		return nil
	default:
		return fmt.Errorf("%s has unsupported persisted JSON type %s", path, expected)
	}
}

func jsonFieldName(field reflect.StructField) string {
	name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
	if name == "" {
		return field.Name
	}
	return name
}

func joinJSONPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("composite release plan must contain one JSON value")
	}
	return nil
}

// encoding/json replaces malformed UTF-16 surrogate escapes with U+FFFD.
// Release evidence instead rejects them before decoding.
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
	if start < 0 || start+6 > len(data) || data[start] != '\\' || data[start+1] != 'u' {
		return 0, false
	}
	return decodeHexQuad(data, start+2)
}
