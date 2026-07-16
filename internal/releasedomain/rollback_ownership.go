package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// HooksPolicy is the Helm hook execution policy frozen into an execution
// authorization. B3 deliberately supports only no-hooks: a release whose
// rendered inputs contain hooks cannot enter the write boundary.
type HooksPolicy string

const HooksPolicyNoHooks HooksPolicy = "no-hooks"

// ExecutionBinding identifies the exact Helm release and immutable revision
// pair for which rollback ownership was proven. These values are supplied by
// the trusted release entrypoint and are sealed into ExecutionAuthorization.
type ExecutionBinding struct {
	ReleaseName       string      `json:"releaseName"`
	ReleaseNamespace  string      `json:"releaseNamespace"`
	BaseRevision      string      `json:"baseRevision"`
	TargetRevision    string      `json:"targetRevision"`
	UpgradeArgvDigest string      `json:"upgradeArgvDigest"`
	HooksPolicy       HooksPolicy `json:"hooksPolicy"`
}

// RollbackOwnershipInput contains the private, pre-write inputs needed to
// prove both the forward and reverse rendered ownership boundaries. Callers
// must discard the raw manifests after this function returns; they are never
// retained by the resulting authorization.
type RollbackOwnershipInput struct {
	Transaction            TransactionAuthorization
	Binding                ExecutionBinding
	Ownership              []byte
	BaseManifest           []byte
	TargetManifest         []byte
	RepeatedTargetManifest []byte
}

// RollbackOwnershipEvidence is a secret-free digest summary of the proof
// sealed into an ExecutionAuthorization. It contains no manifest, ownership,
// binding value, object identity, JSON Pointer, or classifier reason.
type RollbackOwnershipEvidence struct {
	Domain                       Domain `json:"domain"`
	OwnershipDigest              string `json:"ownershipDigest"`
	ClassificationContextDigest  string `json:"classificationContextDigest"`
	BaseManifestDigest           string `json:"baseManifestDigest"`
	TargetManifestDigest         string `json:"targetManifestDigest"`
	RepeatedTargetManifestDigest string `json:"repeatedTargetManifestDigest"`
	ForwardEvidenceDigest        string `json:"forwardEvidenceDigest"`
	ReverseEvidenceDigest        string `json:"reverseEvidenceDigest"`
}

// ExecutionAuthorization is the opaque, immutable result of the final
// pre-write gate. The seal covers the transaction identity, release binding,
// three canonical manifest digests, ownership/context identities, and both
// directions of the rendered ownership proof. It intentionally retains no
// raw Plan, manifest, ownership document, or detailed classifier evidence.
type ExecutionAuthorization struct {
	domain     Domain
	planDigest string
	binding    ExecutionBinding
	evidence   RollbackOwnershipEvidence
	seal       [sha256.Size]byte
}

// Domain returns the one release domain authorized to cross the write
// boundary.
func (authorization ExecutionAuthorization) Domain() Domain {
	return authorization.domain
}

// PlanDigest returns the independently verified transaction plan digest.
func (authorization ExecutionAuthorization) PlanDigest() string {
	return authorization.planDigest
}

// Binding returns a value copy of the frozen release binding.
func (authorization ExecutionAuthorization) Binding() ExecutionBinding {
	return authorization.binding
}

// Evidence returns the bounded, secret-free digest summary sealed into the
// authorization.
func (authorization ExecutionAuthorization) Evidence() RollbackOwnershipEvidence {
	return authorization.evidence
}

// Verify checks only the already-frozen authorization seal and its bounded
// public identities. It does not reopen artifacts or rerun ownership after
// Apply, so the transaction has one pre-write ownership decision.
func (authorization ExecutionAuthorization) Verify() error {
	if err := validateCanonicalPlanDigest(authorization.planDigest, "execution authorization plan digest"); err != nil {
		return err
	}
	if _, err := ParseDomain(string(authorization.domain)); err != nil {
		return fmt.Errorf("execution authorization domain: %w", err)
	}
	if err := validateExecutionBinding(authorization.binding); err != nil {
		return fmt.Errorf("execution authorization binding: %w", err)
	}
	if err := validateRollbackOwnershipEvidence(authorization.evidence, authorization.domain); err != nil {
		return fmt.Errorf("execution authorization evidence: %w", err)
	}
	expected := executionAuthorizationSeal(
		authorization.planDigest,
		authorization.domain,
		authorization.binding,
		authorization.evidence,
	)
	if subtle.ConstantTimeCompare(authorization.seal[:], expected[:]) != 1 {
		return fmt.Errorf("execution authorization seal mismatch")
	}
	return nil
}

// VerifyRollbackOwnership performs the complete rollback proof before the
// first possible Lease, Helm, Kubernetes, or host write. It verifies that the
// same ownership bytes and classification context reproduce the authorized
// forward evidence, that the exact reverse diff produces the same single
// domain and evidence, and that all three manifests are canonical and bound
// to their transaction digests.
func VerifyRollbackOwnership(input RollbackOwnershipInput) (ExecutionAuthorization, error) {
	if err := input.Transaction.Verify(); err != nil {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership transaction: %w", err)
	}
	if err := validateExecutionBinding(input.Binding); err != nil {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership binding: %w", err)
	}

	// Detach every caller-owned byte slice before parsing or hashing. The
	// authorization itself retains only fixed-size digests.
	ownershipData := append([]byte(nil), input.Ownership...)
	baseManifest := append([]byte(nil), input.BaseManifest...)
	targetManifest := append([]byte(nil), input.TargetManifest...)
	repeatedTargetManifest := append([]byte(nil), input.RepeatedTargetManifest...)

	plan := input.Transaction.plan
	context := plan.Digests.ClassificationContext
	if err := VerifyClassificationContextEvidence(context); err != nil {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership classification context: %w", err)
	}
	if context.IgnoreHelmTestHooks {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership requires a no-hooks classification context")
	}
	if input.Binding.ReleaseNamespace != context.DefaultNamespace {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership release namespace differs from classification context")
	}
	bindings := context.BindingMap()
	if bindings["releaseNamespace"] != input.Binding.ReleaseNamespace {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership release namespace binding mismatch")
	}
	if bindings["releaseName"] != input.Binding.ReleaseName {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership release name binding mismatch")
	}
	if input.Binding.BaseRevision != plan.Digests.Base || input.Binding.TargetRevision != plan.Digests.Target {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership base/target revision binding mismatch")
	}

	spec, err := LoadOwnership(bytes.NewReader(ownershipData))
	if err != nil {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership document: %w", err)
	}
	if err := spec.ValidateBindings(bindings); err != nil {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership bindings: %w", err)
	}
	ownershipDigest := rollbackDigestBytes(ownershipData)
	if ownershipDigest != plan.Digests.Ownership {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership digest mismatch")
	}

	manifestInputs := []struct {
		name     string
		data     []byte
		expected string
	}{
		{name: "base", data: baseManifest, expected: plan.Digests.BaseManifest},
		{name: "target", data: targetManifest, expected: plan.Digests.TargetManifest},
		{name: "repeated target", data: repeatedTargetManifest, expected: plan.Digests.RepeatedTargetManifest},
	}
	for _, manifest := range manifestInputs {
		if err := verifyCanonicalNoHooksManifest(manifest.name, manifest.data, manifest.expected, spec, context.DefaultNamespace); err != nil {
			return ExecutionAuthorization{}, err
		}
	}
	if !bytes.Equal(targetManifest, repeatedTargetManifest) {
		return ExecutionAuthorization{}, fmt.Errorf("rollback ownership repeated target canonical manifest differs from target")
	}

	options := RenderedOptions{
		DefaultNamespace:    context.DefaultNamespace,
		Bindings:            bindings,
		IgnoreHelmTestHooks: false,
	}
	forward := ClassifyRendered(baseManifest, targetManifest, spec, options)
	reverse := ClassifyRendered(targetManifest, baseManifest, spec, options)
	if err := verifyExactRollbackOwnershipPair(plan.Rendered, forward, reverse, input.Transaction.domain); err != nil {
		return ExecutionAuthorization{}, err
	}

	evidence := RollbackOwnershipEvidence{
		Domain:                       input.Transaction.domain,
		OwnershipDigest:              ownershipDigest,
		ClassificationContextDigest:  context.Digest,
		BaseManifestDigest:           rollbackDigestBytes(baseManifest),
		TargetManifestDigest:         rollbackDigestBytes(targetManifest),
		RepeatedTargetManifestDigest: rollbackDigestBytes(repeatedTargetManifest),
		ForwardEvidenceDigest:        rollbackRenderedEvidenceDigest(forward),
		ReverseEvidenceDigest:        rollbackRenderedEvidenceDigest(reverse),
	}
	authorization := ExecutionAuthorization{
		domain:     input.Transaction.domain,
		planDigest: input.Transaction.planDigest,
		binding:    input.Binding,
		evidence:   evidence,
	}
	authorization.seal = executionAuthorizationSeal(
		authorization.planDigest,
		authorization.domain,
		authorization.binding,
		authorization.evidence,
	)
	if err := authorization.Verify(); err != nil {
		return ExecutionAuthorization{}, err
	}
	return authorization, nil
}

func validateExecutionBinding(binding ExecutionBinding) error {
	fields := []struct {
		name  string
		value string
	}{
		{name: "release name", value: binding.ReleaseName},
		{name: "release namespace", value: binding.ReleaseNamespace},
	}
	for _, field := range fields {
		if !utf8.ValidString(field.value) {
			return fmt.Errorf("%s is not valid UTF-8", field.name)
		}
		if field.value == "" || strings.TrimSpace(field.value) != field.value {
			return fmt.Errorf("%s must be non-empty without surrounding whitespace", field.name)
		}
		if strings.ContainsRune(field.value, '\x00') {
			return fmt.Errorf("%s contains NUL", field.name)
		}
	}
	baseRevision, err := parseExecutionRevision(binding.BaseRevision, "base revision", 2147483646)
	if err != nil {
		return err
	}
	targetRevision, err := parseExecutionRevision(binding.TargetRevision, "target revision", 2147483647)
	if err != nil {
		return err
	}
	if targetRevision != baseRevision+1 {
		return fmt.Errorf("target revision must immediately follow base revision")
	}
	if err := validateCanonicalPlanDigest(binding.UpgradeArgvDigest, "upgrade argv digest"); err != nil {
		return err
	}
	if binding.HooksPolicy != HooksPolicyNoHooks {
		return fmt.Errorf("hooks policy must be %q", HooksPolicyNoHooks)
	}
	return nil
}

func parseExecutionRevision(value, label string, maximum uint64) (uint64, error) {
	if !utf8.ValidString(value) || value == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
		return 0, fmt.Errorf("%s must be a positive canonical decimal integer", label)
	}
	if value[0] == '0' {
		return 0, fmt.Errorf("%s must be a positive canonical decimal integer", label)
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("%s must be a positive canonical decimal integer", label)
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 31)
	if err != nil || parsed == 0 || parsed > maximum {
		return 0, fmt.Errorf("%s must be a positive canonical decimal integer no greater than %d", label, maximum)
	}
	return parsed, nil
}

func validateRollbackOwnershipEvidence(evidence RollbackOwnershipEvidence, domain Domain) error {
	if evidence.Domain != domain {
		return fmt.Errorf("domain mismatch")
	}
	fields := []struct {
		name  string
		value string
	}{
		{name: "ownership digest", value: evidence.OwnershipDigest},
		{name: "classification context digest", value: evidence.ClassificationContextDigest},
		{name: "base manifest digest", value: evidence.BaseManifestDigest},
		{name: "target manifest digest", value: evidence.TargetManifestDigest},
		{name: "repeated target manifest digest", value: evidence.RepeatedTargetManifestDigest},
		{name: "forward evidence digest", value: evidence.ForwardEvidenceDigest},
		{name: "reverse evidence digest", value: evidence.ReverseEvidenceDigest},
	}
	for _, field := range fields {
		if err := validateCanonicalPlanDigest(field.value, field.name); err != nil {
			return err
		}
	}
	if evidence.TargetManifestDigest != evidence.RepeatedTargetManifestDigest {
		return fmt.Errorf("target manifest digest differs from repeated target manifest digest")
	}
	if evidence.ForwardEvidenceDigest != evidence.ReverseEvidenceDigest {
		return fmt.Errorf("forward evidence digest differs from reverse evidence digest")
	}
	return nil
}

func verifyCanonicalNoHooksManifest(name string, data []byte, expectedDigest string, spec *OwnershipSpec, defaultNamespace string) error {
	if rollbackDigestBytes(data) != expectedDigest {
		return fmt.Errorf("rollback ownership %s manifest digest mismatch", name)
	}
	canonical, err := CanonicalizeRenderedManifest(data, spec, defaultNamespace)
	if err != nil {
		return fmt.Errorf("rollback ownership %s manifest: %w", name, err)
	}
	if !bytes.Equal(canonical, data) {
		return fmt.Errorf("rollback ownership %s manifest is not canonical", name)
	}
	objects, unknown := decodeManifest(data, spec, defaultNamespace, name)
	if len(unknown) != 0 {
		return fmt.Errorf("rollback ownership %s manifest cannot be inspected for hooks", name)
	}
	for _, object := range objects {
		if len(helmHooks(object, true)) != 0 {
			return fmt.Errorf("rollback ownership %s manifest contains a Helm hook", name)
		}
	}
	return nil
}

func verifyExactRenderedOwnership(direction string, classification RenderedClassification, domain Domain) error {
	if len(classification.Unknown) != 0 {
		return fmt.Errorf("rollback ownership %s rendered evidence is unknown", direction)
	}
	if !isExactTransactionDomain(classification.Domains, domain) {
		return fmt.Errorf("rollback ownership %s rendered evidence is not exactly domain %q", direction, domain)
	}
	if len(classification.Evidence) == 0 {
		return fmt.Errorf("rollback ownership %s rendered evidence is empty", direction)
	}
	for _, item := range classification.Evidence {
		if item.Ignored || !isExactTransactionDomain(item.Domains, domain) {
			return fmt.Errorf("rollback ownership %s detailed evidence is not exactly domain %q", direction, domain)
		}
	}
	return nil
}

func verifyExactRollbackOwnershipPair(authorized, forward, reverse RenderedClassification, domain Domain) error {
	if err := verifyExactRenderedOwnership("forward", forward, domain); err != nil {
		return err
	}
	if !reflect.DeepEqual(forward, authorized) {
		return fmt.Errorf("rollback ownership forward evidence differs from authorized transaction evidence")
	}
	if err := verifyExactRenderedOwnership("reverse", reverse, domain); err != nil {
		return err
	}
	if !reflect.DeepEqual(reverse, forward) {
		return fmt.Errorf("rollback ownership reverse evidence differs from forward evidence")
	}
	return nil
}

func rollbackRenderedEvidenceDigest(classification RenderedClassification) string {
	encoded, err := json.Marshal(classification)
	if err != nil {
		panic(fmt.Sprintf("marshal rollback ownership evidence: %v", err))
	}
	return rollbackDigestBytes(encoded)
}

func rollbackDigestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func executionAuthorizationSeal(planDigest string, domain Domain, binding ExecutionBinding, evidence RollbackOwnershipEvidence) [sha256.Size]byte {
	payload := struct {
		Version    string                    `json:"version"`
		PlanDigest string                    `json:"planDigest"`
		Domain     Domain                    `json:"domain"`
		Binding    ExecutionBinding          `json:"binding"`
		Evidence   RollbackOwnershipEvidence `json:"evidence"`
	}{
		Version:    "fugue-release-domain-execution-authorization-v1",
		PlanDigest: planDigest,
		Domain:     domain,
		Binding:    binding,
		Evidence:   evidence,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("marshal execution authorization seal: %v", err))
	}
	return sha256.Sum256(encoded)
}
