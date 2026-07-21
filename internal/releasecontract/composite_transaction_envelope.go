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
	"strconv"
	"unicode/utf8"
)

const (
	CompositeTransactionEnvelopeAPIVersion = "release-domain-transaction.fugue.dev/v2"
	CompositeTransactionEnvelopeKind       = "CompositeReleaseTransactionEnvelope"
	CompositeTransactionEnvelopePolicy     = "evidence-bound-composite-noop-v1"
	CompositeTransactionModeNoop           = "noop"
	maxCompositeTransactionEnvelopeBytes   = 8 << 20
)

// CompositeTransactionBinding is supplied independently by the durable
// coordinator. It prevents a self-consistent envelope from authorizing a
// different record, revision, plan, generation, or fencing epoch.
type CompositeTransactionBinding struct {
	RecordID                  string
	RecordDigest              string
	RecordRevision            string
	PlanDigest                string
	ImageActivationPlanDigest string
	Generation                string
	FencingEpoch              string
}

// CompositeTransactionEnvelope is the persisted v2 authorization input. Its
// only accepted mode is noop; it does not contain or imply an execution path.
type CompositeTransactionEnvelope struct {
	APIVersion                string               `json:"apiVersion"`
	Kind                      string               `json:"kind"`
	Policy                    string               `json:"policy"`
	Mode                      string               `json:"mode"`
	CoordinatorRecordID       string               `json:"coordinatorRecordId"`
	CoordinatorRecordDigest   string               `json:"coordinatorRecordDigest"`
	ExpectedRecordRevision    string               `json:"expectedRecordRevision"`
	PlanDigest                string               `json:"planDigest"`
	ImageActivationPlanDigest string               `json:"imageActivationPlanDigest"`
	Generation                string               `json:"generation"`
	FencingEpoch              string               `json:"fencingEpoch"`
	Plan                      CompositeReleasePlan `json:"plan"`
	Digest                    string               `json:"digest"`
}

func NewCompositeTransactionEnvelope(plan CompositeReleasePlan, binding CompositeTransactionBinding) (CompositeTransactionEnvelope, error) {
	if err := verifyCompositeTransactionBinding(binding); err != nil {
		return CompositeTransactionEnvelope{}, err
	}
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		return CompositeTransactionEnvelope{}, fmt.Errorf("composite transaction plan: %w", err)
	}
	if err := matchCompositeTransactionPlan(plan, binding); err != nil {
		return CompositeTransactionEnvelope{}, err
	}
	plan = cloneCompositeReleasePlan(plan)
	envelope := CompositeTransactionEnvelope{
		APIVersion:                CompositeTransactionEnvelopeAPIVersion,
		Kind:                      CompositeTransactionEnvelopeKind,
		Policy:                    CompositeTransactionEnvelopePolicy,
		Mode:                      CompositeTransactionModeNoop,
		CoordinatorRecordID:       binding.RecordID,
		CoordinatorRecordDigest:   binding.RecordDigest,
		ExpectedRecordRevision:    binding.RecordRevision,
		PlanDigest:                binding.PlanDigest,
		ImageActivationPlanDigest: binding.ImageActivationPlanDigest,
		Generation:                binding.Generation,
		FencingEpoch:              binding.FencingEpoch,
		Plan:                      plan,
	}
	envelope.Digest = DigestCompositeTransactionEnvelope(envelope)
	if err := VerifyCompositeTransactionEnvelope(envelope); err != nil {
		return CompositeTransactionEnvelope{}, err
	}
	return envelope, nil
}

func VerifyCompositeTransactionEnvelope(envelope CompositeTransactionEnvelope) error {
	if envelope.APIVersion != CompositeTransactionEnvelopeAPIVersion ||
		envelope.Kind != CompositeTransactionEnvelopeKind ||
		envelope.Policy != CompositeTransactionEnvelopePolicy ||
		envelope.Mode != CompositeTransactionModeNoop {
		return fmt.Errorf("composite transaction envelope identity is unsupported")
	}
	binding := CompositeTransactionBinding{
		RecordID: envelope.CoordinatorRecordID, RecordDigest: envelope.CoordinatorRecordDigest,
		RecordRevision: envelope.ExpectedRecordRevision, PlanDigest: envelope.PlanDigest,
		ImageActivationPlanDigest: envelope.ImageActivationPlanDigest,
		Generation:                envelope.Generation, FencingEpoch: envelope.FencingEpoch,
	}
	if err := verifyCompositeTransactionBinding(binding); err != nil {
		return err
	}
	if err := VerifyCompositeReleasePlan(envelope.Plan); err != nil {
		return fmt.Errorf("composite transaction plan: %w", err)
	}
	if err := matchCompositeTransactionPlan(envelope.Plan, binding); err != nil {
		return err
	}
	if err := validateDigest(envelope.Digest, "composite transaction envelope digest"); err != nil {
		return err
	}
	if envelope.Digest != DigestCompositeTransactionEnvelope(envelope) {
		return fmt.Errorf("composite transaction envelope digest mismatch")
	}
	return nil
}

func MarshalCompositeTransactionEnvelope(envelope CompositeTransactionEnvelope) ([]byte, error) {
	if err := VerifyCompositeTransactionEnvelope(envelope); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal composite transaction envelope: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxCompositeTransactionEnvelopeBytes {
		return nil, fmt.Errorf("composite transaction envelope exceeds %d-byte limit", maxCompositeTransactionEnvelopeBytes)
	}
	return encoded, nil
}

func DecodeAndVerifyCompositeTransactionEnvelope(reader io.Reader, trusted CompositeTransactionBinding) (CompositeTransactionEnvelope, error) {
	if err := verifyCompositeTransactionBinding(trusted); err != nil {
		return CompositeTransactionEnvelope{}, err
	}
	data, err := readCompositeTransactionEnvelope(reader)
	if err != nil {
		return CompositeTransactionEnvelope{}, err
	}
	if err := validateStrictCompositeTransactionEnvelopeJSON(data); err != nil {
		return CompositeTransactionEnvelope{}, fmt.Errorf("decode composite transaction envelope: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope CompositeTransactionEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return CompositeTransactionEnvelope{}, fmt.Errorf("decode composite transaction envelope: %w", err)
	}
	if err := requireCompositeTransactionEnvelopeEOF(decoder); err != nil {
		return CompositeTransactionEnvelope{}, err
	}
	if err := VerifyCompositeTransactionEnvelope(envelope); err != nil {
		return CompositeTransactionEnvelope{}, err
	}
	actual := CompositeTransactionBinding{
		RecordID: envelope.CoordinatorRecordID, RecordDigest: envelope.CoordinatorRecordDigest,
		RecordRevision: envelope.ExpectedRecordRevision, PlanDigest: envelope.PlanDigest,
		ImageActivationPlanDigest: envelope.ImageActivationPlanDigest,
		Generation:                envelope.Generation, FencingEpoch: envelope.FencingEpoch,
	}
	if actual != trusted {
		return CompositeTransactionEnvelope{}, fmt.Errorf("trusted composite transaction binding mismatch")
	}
	return envelope, nil
}

func DigestCompositeTransactionEnvelope(envelope CompositeTransactionEnvelope) string {
	envelope.Digest = ""
	encoded, err := json.Marshal(envelope)
	if err != nil {
		panic(fmt.Sprintf("marshal composite transaction envelope digest: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func verifyCompositeTransactionBinding(binding CompositeTransactionBinding) error {
	if !validCompositeRecordID(binding.RecordID) {
		return fmt.Errorf("composite transaction record ID is invalid")
	}
	for label, value := range map[string]string{
		"record digest":           binding.RecordDigest,
		"plan digest":             binding.PlanDigest,
		"image activation digest": binding.ImageActivationPlanDigest,
	} {
		if err := validateDigest(value, "composite transaction "+label); err != nil {
			return err
		}
	}
	for label, value := range map[string]string{
		"record revision": binding.RecordRevision,
		"generation":      binding.Generation,
		"fencing epoch":   binding.FencingEpoch,
	} {
		if !validPositiveDecimal(value) {
			return fmt.Errorf("composite transaction %s must be canonical positive decimal", label)
		}
	}
	return nil
}

func matchCompositeTransactionPlan(plan CompositeReleasePlan, binding CompositeTransactionBinding) error {
	if plan.Digest != binding.PlanDigest ||
		plan.ImageActivationPlanDigest != binding.ImageActivationPlanDigest ||
		plan.Generation != binding.Generation || plan.FencingEpoch != binding.FencingEpoch {
		return fmt.Errorf("composite transaction plan binding mismatch")
	}
	return nil
}

func cloneCompositeReleasePlan(plan CompositeReleasePlan) CompositeReleasePlan {
	plan.BaseVersions = append([]DomainVersion(nil), plan.BaseVersions...)
	plan.TargetVersions = append([]DomainVersion(nil), plan.TargetVersions...)
	plan.Steps = CloneCompositeSteps(plan.Steps)
	return plan
}

func validCompositeRecordID(value string) bool {
	if !validText(value, 128) {
		return false
	}
	for index, char := range value {
		if index == 0 && !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')) {
			return false
		}
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func readCompositeTransactionEnvelope(reader io.Reader) ([]byte, error) {
	if isNilReader(reader) {
		return nil, fmt.Errorf("composite transaction envelope reader is nil")
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxCompositeTransactionEnvelopeBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read composite transaction envelope: %w", err)
	}
	if len(data) > maxCompositeTransactionEnvelopeBytes {
		return nil, fmt.Errorf("composite transaction envelope exceeds %d-byte limit", maxCompositeTransactionEnvelopeBytes)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("composite transaction envelope contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return nil, fmt.Errorf("decode composite transaction envelope: %w", err)
	}
	return data, nil
}

func validateStrictCompositeTransactionEnvelopeJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValueForType(decoder, reflect.TypeOf(CompositeTransactionEnvelope{}), "composite transaction envelope"); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("JSON has trailing content")
	}
	return nil
}

func requireCompositeTransactionEnvelopeEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("composite transaction envelope must contain one JSON value")
	}
	return nil
}

func CompositeTransactionBindingForRecord(recordID, recordDigest string, revision int64, plan CompositeReleasePlan) CompositeTransactionBinding {
	return CompositeTransactionBinding{
		RecordID: recordID, RecordDigest: recordDigest, RecordRevision: strconv.FormatInt(revision, 10),
		PlanDigest: plan.Digest, ImageActivationPlanDigest: plan.ImageActivationPlanDigest,
		Generation: plan.Generation, FencingEpoch: plan.FencingEpoch,
	}
}
