package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	PlanAPIVersion = "release-domain-plan.fugue.dev/v1"
	PlanKind       = "ReleaseDomainPlan"
)

// BuildPlan applies the dual-evidence conjunction. It never guesses or unions
// mismatched evidence into an executable domain.
func BuildPlan(input PlanInput) Plan {
	persistedStringErrors := invalidPersistedStringEvidence(input)
	domainErrors := append(
		invalidClassificationDomains("changed-file", input.Files.Domains),
		invalidClassificationDomains("rendered-object", input.Rendered.Domains)...,
	)
	domainErrors = append(domainErrors, classificationEvidenceErrors("changed-file", input.Files.Domains, input.Files.Evidence)...)
	domainErrors = append(domainErrors, classificationEvidenceErrors("rendered-object", input.Rendered.Domains, input.Rendered.Evidence)...)
	domainErrors = append(domainErrors, persistedStringErrors...)
	input.Files.Domains = canonicalDomains(input.Files.Domains)
	input.Files.Evidence = canonicalEvidence(input.Files.Evidence)
	input.Files.Unknown = canonicalEvidence(input.Files.Unknown)
	input.Rendered.Domains = canonicalDomains(input.Rendered.Domains)
	input.Rendered.Evidence = canonicalEvidence(input.Rendered.Evidence)
	input.Rendered.Unknown = canonicalEvidence(input.Rendered.Unknown)
	bindings := make([]ClassificationBinding, len(input.Digests.ClassificationContext.Bindings))
	copy(bindings, input.Digests.ClassificationContext.Bindings)
	input.Digests.ClassificationContext.Bindings = bindings

	plan := Plan{
		APIVersion: PlanAPIVersion,
		Kind:       PlanKind,
		Result:     OutcomeUnknown,
		Domains:    unionDomains(input.Files.Domains, input.Rendered.Domains),
		Digests:    input.Digests,
		Files:      input.Files,
		Rendered:   input.Rendered,
	}
	plan.Unknown = append(plan.Unknown, input.Files.Unknown...)
	plan.Unknown = append(plan.Unknown, input.Rendered.Unknown...)
	plan.Unknown = append(plan.Unknown, domainErrors...)
	plan.Unknown = append(plan.Unknown, validateDigestEvidence(input.Digests)...)

	if len(plan.Unknown) == 0 && !equalDomains(input.Files.Domains, input.Rendered.Domains) {
		plan.Unknown = append(plan.Unknown, Evidence{
			Source: "planner", Subject: "dual-evidence", Domains: plan.Domains,
			Reason: fmt.Sprintf("changed-file domains %v do not equal rendered-object domains %v", input.Files.Domains, input.Rendered.Domains),
		})
	}

	if len(plan.Unknown) == 0 {
		switch len(input.Files.Domains) {
		case 0:
			if !input.Files.AllNonRuntime {
				plan.Unknown = append(plan.Unknown, Evidence{
					Source: "planner", Subject: "zero-domain", Reason: "zero-domain requires every changed file to be proven non-runtime",
				})
			} else {
				plan.Result = OutcomeZero
				plan.Domains = []Domain{}
			}
		case 1:
			plan.Result = OutcomeSingle
			plan.SelectedDomain = input.Files.Domains[0]
			plan.Domains = append([]Domain(nil), input.Files.Domains...)
		default:
			plan.Result = OutcomeMultiple
			plan.Domains = append([]Domain(nil), input.Files.Domains...)
		}
	}
	if len(plan.Unknown) > 0 {
		plan.Result = OutcomeUnknown
		plan.SelectedDomain = ""
		// encoding/json replaces malformed UTF-8 with U+FFFD. Clear any
		// malformed external value only after it has forced an unknown result,
		// so the persisted blocked plan is byte-safe and never normalizes bad
		// evidence into a different apparent value.
		sanitizeInvalidUTF8Strings(reflect.ValueOf(&plan))
		plan.Unknown = canonicalEvidence(plan.Unknown)
	}
	plan.PlanDigest = computePlanDigest(plan)
	return plan
}

func invalidPersistedStringEvidence(input PlanInput) []Evidence {
	invalid := invalidUTF8Paths(reflect.ValueOf(input), "")
	if len(invalid) == 0 {
		return nil
	}
	sort.Strings(invalid)
	return []Evidence{{
		Source:  "planner",
		Subject: "persisted strings",
		Reason:  "persisted values are not valid UTF-8: " + strings.Join(invalid, ", "),
	}}
}

func classificationEvidenceErrors(source string, declared []Domain, evidence []Evidence) []Evidence {
	evidenceDomains := make([]Domain, 0)
	invalid := make([]string, 0)
	invalidUTF8 := make([]string, 0)
	for evidenceIndex, item := range evidence {
		if item.Ignored {
			continue
		}
		for domainIndex, domain := range item.Domains {
			if !utf8.ValidString(string(domain)) {
				invalidUTF8 = append(invalidUTF8, fmt.Sprintf("evidence[%d].domains[%d]", evidenceIndex, domainIndex))
				continue
			}
			if _, ok := domainRank[domain]; !ok {
				invalid = append(invalid, string(domain))
				continue
			}
			evidenceDomains = append(evidenceDomains, domain)
		}
	}
	if len(invalidUTF8) > 0 {
		sort.Strings(invalidUTF8)
		return []Evidence{{
			Source: "planner", Subject: source + " evidence", Reason: "evidence contains domains that are not valid UTF-8: " + strings.Join(invalidUTF8, ", "),
		}}
	}
	if len(invalid) > 0 {
		sort.Strings(invalid)
		return []Evidence{{
			Source: "planner", Subject: source + " evidence", Reason: "evidence contains unknown domains: " + strings.Join(invalid, ", "),
		}}
	}
	if equalDomains(declared, evidenceDomains) {
		return nil
	}
	return []Evidence{{
		Source: "planner", Subject: source + " evidence",
		Reason: fmt.Sprintf("declared domains %v do not equal evidence domains %v", canonicalDomains(declared), canonicalDomains(evidenceDomains)),
	}}
}

func invalidClassificationDomains(source string, domains []Domain) []Evidence {
	invalid := make([]string, 0)
	invalidUTF8 := make([]string, 0)
	for index, domain := range domains {
		if !utf8.ValidString(string(domain)) {
			invalidUTF8 = append(invalidUTF8, fmt.Sprintf("domains[%d]", index))
			continue
		}
		if _, ok := domainRank[domain]; !ok {
			invalid = append(invalid, string(domain))
		}
	}
	if len(invalidUTF8) > 0 {
		sort.Strings(invalidUTF8)
		return []Evidence{{
			Source: "planner", Subject: source + " domains", Reason: "classification contains domains that are not valid UTF-8: " + strings.Join(invalidUTF8, ", "),
		}}
	}
	if len(invalid) == 0 {
		return nil
	}
	sort.Strings(invalid)
	return []Evidence{{
		Source: "planner", Subject: source + " domains", Reason: "classification contains unknown domains: " + strings.Join(invalid, ", "),
	}}
}

func validateDigestEvidence(digests DigestEvidence) []Evidence {
	fields := []struct {
		name  string
		value string
	}{
		{"base", digests.Base},
		{"target", digests.Target},
		{"live", digests.Live},
		{"baseManifest", digests.BaseManifest},
		{"targetManifest", digests.TargetManifest},
		{"repeatedTargetManifest", digests.RepeatedTargetManifest},
		{"ownership", digests.Ownership},
		{"changedFiles", digests.ChangedFiles},
	}
	missing := make([]string, 0)
	invalidUTF8 := make([]string, 0)
	for _, field := range fields {
		if strings.TrimSpace(field.value) == "" {
			missing = append(missing, field.name)
		}
		if !utf8.ValidString(field.value) {
			invalidUTF8 = append(invalidUTF8, field.name)
		}
	}
	unknown := make([]Evidence, 0, 4)
	if len(missing) > 0 {
		sort.Strings(missing)
		unknown = append(unknown, Evidence{
			Source: "planner", Subject: "digests", Reason: "missing digest evidence: " + strings.Join(missing, ", "),
		})
	}
	if len(invalidUTF8) > 0 {
		sort.Strings(invalidUTF8)
		unknown = append(unknown, Evidence{
			Source: "planner", Subject: "digests", Reason: "digest evidence is not valid UTF-8: " + strings.Join(invalidUTF8, ", "),
		})
	}
	if digests.Base != "" && digests.Live != "" && digests.Base != digests.Live {
		unknown = append(unknown, Evidence{
			Source: "planner", Subject: "base/live", Reason: "opaque base and live digests differ",
		})
	}
	if digests.TargetManifest != "" && digests.RepeatedTargetManifest != "" && digests.TargetManifest != digests.RepeatedTargetManifest {
		unknown = append(unknown, Evidence{
			Source: "planner", Subject: "target-render", Reason: "repeated target render digest differs (lookup/render drift)",
		})
	}
	if err := VerifyClassificationContextEvidence(digests.ClassificationContext); err != nil {
		unknown = append(unknown, Evidence{
			Source: "planner", Subject: "classification-context", Reason: err.Error(),
		})
	}
	return unknown
}

func unionDomains(groups ...[]Domain) []Domain {
	set := map[Domain]struct{}{}
	for _, group := range groups {
		for _, domain := range group {
			if _, ok := domainRank[domain]; ok {
				set[domain] = struct{}{}
			}
		}
	}
	return domainsFromSet(set)
}

func invalidUTF8Paths(value reflect.Value, path string) []string {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return nil
	}

	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			if path == "" {
				path = "$"
			}
			return []string{path}
		}
	case reflect.Struct:
		invalid := make([]string, 0)
		typeOfValue := value.Type()
		for index := 0; index < value.NumField(); index++ {
			field := typeOfValue.Field(index)
			if field.PkgPath != "" || jsonFieldName(field) == "-" {
				continue
			}
			invalid = append(invalid, invalidUTF8Paths(value.Field(index), joinJSONPath(path, jsonFieldName(field)))...)
		}
		return invalid
	case reflect.Slice, reflect.Array:
		invalid := make([]string, 0)
		for index := 0; index < value.Len(); index++ {
			invalid = append(invalid, invalidUTF8Paths(value.Index(index), fmt.Sprintf("%s[%d]", path, index))...)
		}
		return invalid
	}
	return nil
}

func sanitizeInvalidUTF8Strings(value reflect.Value) {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return
	}

	switch value.Kind() {
	case reflect.String:
		if value.CanSet() && !utf8.ValidString(value.String()) {
			value.SetString("")
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			sanitizeInvalidUTF8Strings(value.Field(index))
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			sanitizeInvalidUTF8Strings(value.Index(index))
		}
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

func computePlanDigest(plan Plan) string {
	plan.PlanDigest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal release domain plan: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// VerifyPlanDigest detects any mutation of a plan after classification.
func VerifyPlanDigest(plan Plan) error {
	if invalid := invalidUTF8Paths(reflect.ValueOf(plan), ""); len(invalid) > 0 {
		sort.Strings(invalid)
		return fmt.Errorf("plan contains strings that are not valid UTF-8: %s", strings.Join(invalid, ", "))
	}
	if plan.PlanDigest == "" {
		return fmt.Errorf("plan digest is empty")
	}
	expected := computePlanDigest(plan)
	if plan.PlanDigest != expected {
		return fmt.Errorf("plan digest mismatch: got %s, want %s", plan.PlanDigest, expected)
	}
	return nil
}

// DecodeAndVerifyPlan strictly decodes a persisted plan and binds it to an
// independently supplied expected plan digest. The external digest is
// mandatory: a plan's embedded digest alone is not an authorization token.
// Unknown or duplicate JSON fields, non-canonical field-name casing,
// malformed Unicode, trailing values, and an unexpected API identity are all
// rejected before a caller can inspect an executable outcome.
func DecodeAndVerifyPlan(reader io.Reader, expectedPlanDigest string) (Plan, error) {
	if !utf8.ValidString(expectedPlanDigest) {
		return Plan{}, fmt.Errorf("expected plan digest is not valid UTF-8")
	}
	if strings.TrimSpace(expectedPlanDigest) == "" {
		return Plan{}, fmt.Errorf("expected plan digest is empty")
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return Plan{}, fmt.Errorf("read release domain plan: %w", err)
	}
	if !utf8.Valid(data) {
		return Plan{}, fmt.Errorf("decode release domain plan: input contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return Plan{}, fmt.Errorf("decode release domain plan: %w", err)
	}
	if err := validateStrictPlanJSON(data); err != nil {
		return Plan{}, fmt.Errorf("decode release domain plan: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var plan Plan
	if err := decoder.Decode(&plan); err != nil {
		return Plan{}, fmt.Errorf("decode release domain plan: %w", err)
	}
	if plan.APIVersion != PlanAPIVersion {
		return Plan{}, fmt.Errorf("release domain plan apiVersion %q is unsupported", plan.APIVersion)
	}
	if plan.Kind != PlanKind {
		return Plan{}, fmt.Errorf("release domain plan kind %q is unsupported", plan.Kind)
	}
	if plan.PlanDigest != expectedPlanDigest {
		return Plan{}, fmt.Errorf("external plan digest mismatch: got %s, want %s", plan.PlanDigest, expectedPlanDigest)
	}
	if err := VerifyPlanDigest(plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func validateStrictPlanJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValueForType(decoder, reflect.TypeOf(Plan{}), "plan"); err != nil {
		return err
	}
	if trailing, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode trailing data: %w", err)
		}
		return fmt.Errorf("plan must contain one JSON value; found trailing token %v", trailing)
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

	case reflect.Bool:
		if _, ok := token.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
		return nil
	default:
		return fmt.Errorf("%s has unsupported persisted JSON type %s", path, expected)
	}
}

// NoWriteRequired identifies the only successful outcome that must not
// dispatch an adapter or perform any mutation.
func (plan Plan) NoWriteRequired() bool {
	return plan.Result == OutcomeZero
}

// SingleDomainDispatchAllowed identifies the only outcome that may dispatch a
// mutation adapter after the integration boundary revalidates the plan digest.
func (plan Plan) SingleDomainDispatchAllowed() bool {
	return plan.Result == OutcomeSingle
}
