package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

const (
	TransactionEnvelopeAPIVersion = "release-domain-transaction.fugue.dev/v1"
	TransactionEnvelopeKind       = "ReleaseDomainTransactionEnvelope"
	maxTransactionEnvelopeBytes   = 8 << 20
)

// TransactionEnvelope is the complete, persisted authorization input for one
// release-domain transaction. Every field is required so the authorization
// boundary has only one JSON representation for absent data: rejection.
type TransactionEnvelope struct {
	APIVersion     string `json:"apiVersion"`
	Kind           string `json:"kind"`
	PlanDigest     string `json:"planDigest"`
	ExpectedDomain Domain `json:"expectedDomain"`
	Plan           Plan   `json:"plan"`
}

// TransactionAuthorization is an opaque, immutable authorization result.
// Callers can select the already-verified adapter, bind evidence to the plan
// digest, and recheck the private seal immediately before entering the
// transaction. The untrusted Plan is intentionally not exposed.
type TransactionAuthorization struct {
	domain     Domain
	planDigest string
	seal       [sha256.Size]byte
}

// Domain returns the single release domain authorized by the envelope.
func (authorization TransactionAuthorization) Domain() Domain {
	return authorization.domain
}

// PlanDigest returns the independently bound plan digest.
func (authorization TransactionAuthorization) PlanDigest() string {
	return authorization.planDigest
}

// Verify detects a zero value or mutation of the authorization's frozen
// fields. It is safe to call again directly before the first possible write.
func (authorization TransactionAuthorization) Verify() error {
	if err := validateCanonicalPlanDigest(authorization.planDigest, "authorization plan digest"); err != nil {
		return err
	}
	if _, err := ParseDomain(string(authorization.domain)); err != nil {
		return fmt.Errorf("authorization domain: %w", err)
	}
	expected := transactionAuthorizationSeal(authorization.planDigest, authorization.domain)
	if subtle.ConstantTimeCompare(authorization.seal[:], expected[:]) != 1 {
		return fmt.Errorf("transaction authorization seal mismatch")
	}
	return nil
}

// NewTransactionEnvelope constructs a canonical envelope only from a
// semantically reproducible single-domain Plan. expectedPlanDigest is an
// independent input; copying the Plan's self-declared digest is insufficient.
func NewTransactionEnvelope(plan Plan, expectedPlanDigest string, expectedDomain Domain) (TransactionEnvelope, error) {
	if err := validateCanonicalPlanDigest(expectedPlanDigest, "expected plan digest"); err != nil {
		return TransactionEnvelope{}, err
	}
	if _, err := ParseDomain(string(expectedDomain)); err != nil {
		return TransactionEnvelope{}, fmt.Errorf("expected transaction domain: %w", err)
	}

	rebuilt, err := rebuildExecutableTransactionPlan(plan, expectedPlanDigest, expectedDomain)
	if err != nil {
		return TransactionEnvelope{}, err
	}
	return TransactionEnvelope{
		APIVersion:     TransactionEnvelopeAPIVersion,
		Kind:           TransactionEnvelopeKind,
		PlanDigest:     expectedPlanDigest,
		ExpectedDomain: expectedDomain,
		Plan:           rebuilt,
	}, nil
}

// DecodeAndVerifyTransactionEnvelope strictly decodes one transaction
// envelope and binds it to trusted digest and domain inputs supplied outside
// the envelope. It returns only an opaque authorization, never the untrusted
// plan or any caller-controlled derived outcome.
func DecodeAndVerifyTransactionEnvelope(reader io.Reader, trustedPlanDigest string, trustedExpectedDomain Domain) (TransactionAuthorization, error) {
	if err := validateCanonicalPlanDigest(trustedPlanDigest, "trusted plan digest"); err != nil {
		return TransactionAuthorization{}, err
	}
	if _, err := ParseDomain(string(trustedExpectedDomain)); err != nil {
		return TransactionAuthorization{}, fmt.Errorf("trusted expected domain: %w", err)
	}
	if isNilReader(reader) {
		return TransactionAuthorization{}, fmt.Errorf("transaction envelope reader is nil")
	}

	data, err := io.ReadAll(io.LimitReader(reader, maxTransactionEnvelopeBytes+1))
	if err != nil {
		return TransactionAuthorization{}, fmt.Errorf("read transaction envelope: %w", err)
	}
	if len(data) > maxTransactionEnvelopeBytes {
		return TransactionAuthorization{}, fmt.Errorf("transaction envelope exceeds %d-byte limit", maxTransactionEnvelopeBytes)
	}
	if !utf8.Valid(data) {
		return TransactionAuthorization{}, fmt.Errorf("decode transaction envelope: input contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return TransactionAuthorization{}, fmt.Errorf("decode transaction envelope: %w", err)
	}
	if err := validateStrictTransactionEnvelopeJSON(data); err != nil {
		return TransactionAuthorization{}, fmt.Errorf("decode transaction envelope: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope TransactionEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return TransactionAuthorization{}, fmt.Errorf("decode transaction envelope: %w", err)
	}
	if envelope.APIVersion != TransactionEnvelopeAPIVersion {
		return TransactionAuthorization{}, fmt.Errorf("transaction envelope apiVersion %q is unsupported", envelope.APIVersion)
	}
	if envelope.Kind != TransactionEnvelopeKind {
		return TransactionAuthorization{}, fmt.Errorf("transaction envelope kind %q is unsupported", envelope.Kind)
	}
	if err := validateCanonicalPlanDigest(envelope.PlanDigest, "envelope plan digest"); err != nil {
		return TransactionAuthorization{}, err
	}
	if envelope.PlanDigest != trustedPlanDigest {
		return TransactionAuthorization{}, fmt.Errorf("trusted plan digest mismatch")
	}
	if _, err := ParseDomain(string(envelope.ExpectedDomain)); err != nil {
		return TransactionAuthorization{}, fmt.Errorf("envelope expected domain: %w", err)
	}
	if envelope.ExpectedDomain != trustedExpectedDomain {
		return TransactionAuthorization{}, fmt.Errorf("trusted expected domain mismatch")
	}

	if _, err := rebuildExecutableTransactionPlan(envelope.Plan, trustedPlanDigest, trustedExpectedDomain); err != nil {
		return TransactionAuthorization{}, err
	}
	authorization := newTransactionAuthorization(trustedPlanDigest, trustedExpectedDomain)
	if err := authorization.Verify(); err != nil {
		return TransactionAuthorization{}, err
	}
	return authorization, nil
}

func validateStrictTransactionEnvelopeJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValueForType(decoder, reflect.TypeOf(TransactionEnvelope{}), "transactionEnvelope"); err != nil {
		return err
	}
	if trailing, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode trailing data: %w", err)
		}
		return fmt.Errorf("transaction envelope must contain one JSON value; found trailing token %v", trailing)
	}
	return nil
}

func rebuildExecutableTransactionPlan(plan Plan, expectedPlanDigest string, expectedDomain Domain) (Plan, error) {
	if err := validateCanonicalPlanDigest(plan.PlanDigest, "nested plan digest"); err != nil {
		return Plan{}, err
	}
	if plan.PlanDigest != expectedPlanDigest {
		return Plan{}, fmt.Errorf("nested plan digest mismatch")
	}
	if plan.APIVersion != PlanAPIVersion || plan.Kind != PlanKind {
		return Plan{}, fmt.Errorf("nested release domain plan identity is unsupported")
	}
	if err := VerifyPlanDigest(plan); err != nil {
		return Plan{}, fmt.Errorf("verify nested release domain plan: %w", err)
	}
	if plan.Result != OutcomeSingle || !plan.SingleDomainDispatchAllowed() {
		return Plan{}, fmt.Errorf("transaction requires a single-domain plan")
	}
	if plan.SelectedDomain != expectedDomain {
		return Plan{}, fmt.Errorf("nested selected domain mismatch")
	}
	if !isExactTransactionDomain(plan.Domains, expectedDomain) ||
		!isExactTransactionDomain(plan.Files.Domains, expectedDomain) ||
		!isExactTransactionDomain(plan.Rendered.Domains, expectedDomain) {
		return Plan{}, fmt.Errorf("transaction plan domain evidence must contain exactly the expected domain")
	}
	if plan.Files.AllNonRuntime {
		return Plan{}, fmt.Errorf("single-domain transaction cannot be marked all-non-runtime")
	}
	if len(plan.Unknown) != 0 || len(plan.Files.Unknown) != 0 || len(plan.Rendered.Unknown) != 0 {
		return Plan{}, fmt.Errorf("single-domain transaction contains unknown evidence")
	}

	rebuilt := BuildPlan(PlanInput{
		Files:    plan.Files,
		Rendered: plan.Rendered,
		Digests:  plan.Digests,
	})
	if rebuilt.Result != OutcomeSingle || rebuilt.SelectedDomain != expectedDomain || rebuilt.PlanDigest != expectedPlanDigest {
		return Plan{}, fmt.Errorf("reconstructed transaction plan does not authorize the expected domain")
	}
	originalJSON, err := json.Marshal(plan)
	if err != nil {
		return Plan{}, fmt.Errorf("marshal nested transaction plan: %w", err)
	}
	rebuiltJSON, err := json.Marshal(rebuilt)
	if err != nil {
		return Plan{}, fmt.Errorf("marshal reconstructed transaction plan: %w", err)
	}
	if !bytes.Equal(originalJSON, rebuiltJSON) {
		return Plan{}, fmt.Errorf("nested transaction plan is not canonically reproducible")
	}
	return rebuilt, nil
}

func isExactTransactionDomain(domains []Domain, expected Domain) bool {
	return len(domains) == 1 && domains[0] == expected
}

func validateCanonicalPlanDigest(digest, label string) error {
	if !utf8.ValidString(digest) {
		return fmt.Errorf("%s is not valid UTF-8", label)
	}
	const prefix = "sha256:"
	if len(digest) != len(prefix)+sha256.Size*2 || !strings.HasPrefix(digest, prefix) {
		return fmt.Errorf("%s must be lowercase sha256:<64-hex>", label)
	}
	for _, digit := range digest[len(prefix):] {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return fmt.Errorf("%s must be lowercase sha256:<64-hex>", label)
		}
	}
	return nil
}

func newTransactionAuthorization(planDigest string, domain Domain) TransactionAuthorization {
	return TransactionAuthorization{
		domain:     domain,
		planDigest: planDigest,
		seal:       transactionAuthorizationSeal(planDigest, domain),
	}
}

func transactionAuthorizationSeal(planDigest string, domain Domain) [sha256.Size]byte {
	return sha256.Sum256([]byte("fugue-release-domain-transaction-authorization-v1\x00" + planDigest + "\x00" + string(domain)))
}
