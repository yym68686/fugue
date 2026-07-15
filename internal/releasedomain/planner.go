package releasedomain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	domainErrors := append(
		invalidClassificationDomains("changed-file", input.Files.Domains),
		invalidClassificationDomains("rendered-object", input.Rendered.Domains)...,
	)
	domainErrors = append(domainErrors, classificationEvidenceErrors("changed-file", input.Files.Domains, input.Files.Evidence)...)
	domainErrors = append(domainErrors, classificationEvidenceErrors("rendered-object", input.Rendered.Domains, input.Rendered.Evidence)...)
	domainErrors = append(domainErrors, invalidEvidenceUTF8("changed-file", input.Files.Evidence)...)
	domainErrors = append(domainErrors, invalidEvidenceUTF8("rendered-object", input.Rendered.Evidence)...)
	input.Files.Domains = canonicalDomains(input.Files.Domains)
	input.Files.Evidence = canonicalEvidence(input.Files.Evidence)
	input.Files.Unknown = canonicalEvidence(input.Files.Unknown)
	input.Rendered.Domains = canonicalDomains(input.Rendered.Domains)
	input.Rendered.Evidence = canonicalEvidence(input.Rendered.Evidence)
	input.Rendered.Unknown = canonicalEvidence(input.Rendered.Unknown)

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
		plan.Unknown = canonicalEvidence(plan.Unknown)
	}
	plan.PlanDigest = computePlanDigest(plan)
	return plan
}

func invalidEvidenceUTF8(source string, evidence []Evidence) []Evidence {
	invalid := make([]string, 0)
	for index, item := range evidence {
		fields := []struct {
			name  string
			value string
		}{
			{"source", item.Source},
			{"subject", item.Subject},
			{"status", item.Status},
			{"reason", item.Reason},
			{"ruleId", item.RuleID},
		}
		for _, field := range fields {
			if !utf8.ValidString(field.value) {
				invalid = append(invalid, fmt.Sprintf("evidence[%d].%s", index, field.name))
			}
		}
		for pathIndex, path := range item.Paths {
			if !utf8.ValidString(path) {
				invalid = append(invalid, fmt.Sprintf("evidence[%d].paths[%d]", index, pathIndex))
			}
		}
	}
	if len(invalid) == 0 {
		return nil
	}
	sort.Strings(invalid)
	return []Evidence{{
		Source: "planner", Subject: source + " evidence", Reason: "evidence is not valid UTF-8: " + strings.Join(invalid, ", "),
	}}
}

func classificationEvidenceErrors(source string, declared []Domain, evidence []Evidence) []Evidence {
	evidenceDomains := make([]Domain, 0)
	invalid := make([]string, 0)
	for _, item := range evidence {
		if item.Ignored {
			continue
		}
		for _, domain := range item.Domains {
			if _, ok := domainRank[domain]; !ok {
				invalid = append(invalid, string(domain))
				continue
			}
			evidenceDomains = append(evidenceDomains, domain)
		}
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
	for _, domain := range domains {
		if _, ok := domainRank[domain]; !ok {
			invalid = append(invalid, string(domain))
		}
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
	if plan.PlanDigest == "" {
		return fmt.Errorf("plan digest is empty")
	}
	expected := computePlanDigest(plan)
	if plan.PlanDigest != expected {
		return fmt.Errorf("plan digest mismatch: got %s, want %s", plan.PlanDigest, expected)
	}
	return nil
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
