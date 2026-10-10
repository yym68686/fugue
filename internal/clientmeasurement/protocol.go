package clientmeasurement

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/netip"
	"slices"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

const RequestHeader = "X-Fugue-Network-Probe"
const AttestationHeader = "X-Fugue-Network-Attestation"
const BodyBytes = 1 << 20

func Payload(seed string) []byte {
	result := make([]byte, BodyBytes)
	for offset := 0; offset < len(result); offset += sha256.Size {
		input := append([]byte(seed), make([]byte, 8)...)
		binary.BigEndian.PutUint64(input[len(input)-8:], uint64(offset/sha256.Size))
		digest := sha256.Sum256(input)
		copy(result[offset:], digest[:])
	}
	return result
}

func Digest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func sign(value any, key string, domain string) string {
	raw, _ := json.Marshal(value)
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(domain + "\x00"))
	mac.Write(raw)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func verificationKey(keys bundleauth.Keyring, keyID string) (string, bool) {
	if _, revoked := keys.RevokedKeyIDs[strings.ToLower(keyID)]; revoked || keyID == "" {
		return "", false
	}
	if keyID == keys.PrimaryKeyID && keys.PrimaryKey != "" {
		return keys.PrimaryKey, true
	}
	if keyID == keys.PreviousKeyID && keys.PreviousKey != "" {
		return keys.PreviousKey, true
	}
	return "", false
}

func SignPermit(value model.EdgeClientProbePermit, keys bundleauth.Keyring) (model.EdgeClientProbePermit, error) {
	if keys.PrimaryKey == "" || keys.PrimaryKeyID == "" {
		return value, errors.New("probe signing identity unavailable")
	}
	value.KeyID, value.Signature = keys.PrimaryKeyID, ""
	value.Signature = sign(value, keys.PrimaryKey, "fugue-client-probe-permit-v1")
	return value, nil
}

func ValidatePermit(value model.EdgeClientProbePermit) error {
	if len(value.TargetEdgeIDs) < 2 || len(value.TargetEdgeIDs) > 8 || !slices.IsSorted(value.TargetEdgeIDs) || !slices.Contains(value.TargetEdgeIDs, value.EdgeID) {
		return errors.New("signed complete physical target set required")
	}
	for index, target := range value.TargetEdgeIDs {
		if target == "" || len(target) > 128 || strings.ContainsAny(target, " \t\r\n\x00") || index > 0 && value.TargetEdgeIDs[index-1] == target {
			return errors.New("invalid or duplicate signed physical target")
		}
	}
	address, err := netip.ParseAddr(value.Address)
	if err != nil || !address.IsGlobalUnicast() || address.IsPrivate() || address.Is4In6() || address.String() != value.Address ||
		value.Schema != "fugue.client-probe-permit/v1" || value.BodyBytes != BodyBytes || value.IssuedAt.IsZero() || !value.ExpiresAt.After(value.IssuedAt) || value.ExpiresAt.Sub(value.IssuedAt) > 2*time.Minute ||
		(value.TrafficClass != "dynamic_api" && value.TrafficClass != "streaming") || value.Hostname != strings.TrimSuffix(strings.ToLower(value.Hostname), ".") || strings.ContainsAny(value.Hostname, "/:@?# \t\r\n") ||
		!strings.HasPrefix(value.Path, "/") || len(value.Path) > 2048 || strings.ContainsAny(value.Path, "?#\r\n") {
		return errors.New("invalid bounded client probe permit")
	}
	for _, text := range []string{value.RoundID, value.AttemptID, value.ObserverID, value.Hostname, value.EdgeID, value.EdgeGroupID, value.BundleVersion, value.KeyID, value.Signature} {
		if text == "" || len(text) > 256 || strings.ContainsAny(text, " \t\r\n\x00") {
			return errors.New("invalid client probe identity")
		}
	}
	seed, err := hex.DecodeString(value.BodySeed)
	if err != nil || len(seed) != 32 || len(value.BodySeed) != 64 {
		return errors.New("invalid client probe payload seed")
	}
	for _, digest := range []string{value.RouteDigest, value.BodySHA256} {
		raw, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
		if err != nil || len(raw) != 32 || len(digest) != 71 {
			return errors.New("invalid probe digest")
		}
	}
	return nil
}

func VerifyPermit(value model.EdgeClientProbePermit, keys bundleauth.Keyring, now time.Time) error {
	if err := ValidatePermit(value); err != nil {
		return err
	}
	key, found := verificationKey(keys, value.KeyID)
	actual := value.Signature
	value.Signature = ""
	if !found || !hmac.Equal([]byte(actual), []byte(sign(value, key, "fugue-client-probe-permit-v1"))) || value.IssuedAt.After(now) || !value.ExpiresAt.After(now) {
		return errors.New("expired or unauthenticated probe permit")
	}
	return nil
}

func SignAttestation(value model.EdgeClientProbeAttestation, keys bundleauth.Keyring) (model.EdgeClientProbeAttestation, error) {
	if keys.PrimaryKey == "" || keys.PrimaryKeyID == "" {
		return value, errors.New("probe attestation signing identity unavailable")
	}
	value.KeyID, value.Signature = keys.PrimaryKeyID, ""
	value.Signature = sign(value, keys.PrimaryKey, "fugue-client-probe-attestation-v1")
	return value, nil
}

func ValidateAttestation(value model.EdgeClientProbeAttestation, permit model.EdgeClientProbePermit) error {
	if value.Schema != "fugue.client-probe-attestation/v1" || value.AttemptID != permit.AttemptID || value.EdgeID != permit.EdgeID || value.EdgeGroupID != permit.EdgeGroupID || value.RouteDigest != permit.RouteDigest || value.BundleVersion != permit.BundleVersion ||
		value.ObservedAt.Before(permit.IssuedAt) || !value.ObservedAt.Before(permit.ExpiresAt) || !value.ClientNetwork.ObservedAt.Equal(value.ObservedAt) || model.ValidateEdgeClientNetworkSample(&value.ClientNetwork) != nil || value.ClientNetwork.Backend == nil || !value.ClientNetwork.TCPInfoAvailable || value.ClientNetwork.RTTMS == nil || value.KeyID == "" || value.Signature == "" {
		return errors.New("probe attestation does not bind a public socket and exact route")
	}
	return nil
}

func VerifyAttestation(value model.EdgeClientProbeAttestation, permit model.EdgeClientProbePermit, keys bundleauth.Keyring) error {
	if err := ValidateAttestation(value, permit); err != nil {
		return err
	}
	key, found := verificationKey(keys, value.KeyID)
	actual := value.Signature
	value.Signature = ""
	if !found || !hmac.Equal([]byte(actual), []byte(sign(value, key, "fugue-client-probe-attestation-v1"))) {
		return errors.New("unauthenticated public socket attestation")
	}
	return nil
}

func ValidateReport(report model.EdgeClientProbeReport) (string, error) {
	plan := report.Plan
	if report.Schema != "fugue.client-probe-report/v1" || plan.Schema != "fugue.client-probe-plan/v1" || len(plan.Permits) < 2 || len(plan.Permits) > 8 || len(plan.Permits) != len(report.Outcomes) || plan.ObserverLabel == "" || len(plan.ObserverLabel) > 128 || strings.ContainsAny(plan.ObserverLabel, "\r\n\x00") {
		return "", errors.New("complete bounded client probe round required")
	}
	first := plan.Permits[0]
	if len(first.TargetEdgeIDs) != len(plan.Permits) {
		return "", errors.New("client report omits a signed physical target")
	}
	seen := map[string]bool{}
	outcomes := map[string]model.EdgeClientProbeOutcome{}
	for _, outcome := range report.Outcomes {
		if _, exists := outcomes[outcome.AttemptID]; exists {
			return "", errors.New("duplicate client probe attempt outcome")
		}
		outcomes[outcome.AttemptID] = outcome
	}
	cohort := ""
	for _, permit := range plan.Permits {
		if err := ValidatePermit(permit); err != nil {
			return "", err
		}
		if seen[permit.EdgeID] || !slices.Equal(permit.TargetEdgeIDs, first.TargetEdgeIDs) || permit.RoundID != plan.RoundID || permit.ObserverID != first.ObserverID || permit.Hostname != first.Hostname || permit.Path != first.Path || permit.TrafficClass != first.TrafficClass || permit.BodySeed != first.BodySeed || permit.BodySHA256 != first.BodySHA256 || !permit.IssuedAt.Equal(first.IssuedAt) || !permit.ExpiresAt.Equal(first.ExpiresAt) {
			return "", errors.New("cross-edge probes lack one identical payload and observer plan")
		}
		seen[permit.EdgeID] = true
		outcome, found := outcomes[permit.AttemptID]
		if !found || outcome.StartedAt.Before(permit.IssuedAt) || outcome.CompletedAt.Before(outcome.StartedAt) || outcome.CompletedAt.After(permit.ExpiresAt) || outcome.CompletedAt.Sub(outcome.StartedAt) > 90*time.Second || outcome.BytesReceived < 0 || outcome.BytesReceived > BodyBytes || math.IsNaN(outcome.BodySeconds) || math.IsInf(outcome.BodySeconds, 0) || outcome.BodySeconds < 0 || outcome.BodySeconds > outcome.CompletedAt.Sub(outcome.StartedAt).Seconds()+0.01 {
			return "", errors.New("probe timing or attempt denominator is invalid")
		}
		switch outcome.Failure {
		case "":
			if outcome.BytesReceived != BodyBytes || outcome.BodySHA256 != permit.BodySHA256 || outcome.BodySeconds <= 0 || outcome.Attestation == nil {
				return "", errors.New("successful probe lacks complete identical response bytes")
			}
		case "connect", "tls", "response", "body", "integrity":
		default:
			return "", errors.New("unsupported client probe failure category")
		}
		if outcome.Attestation != nil {
			if outcome.Attestation.ObservedAt.Before(outcome.StartedAt) || outcome.Attestation.ObservedAt.After(outcome.CompletedAt) {
				return "", errors.New("public socket attestation falls outside client attempt")
			}
			if err := ValidateAttestation(*outcome.Attestation, permit); err != nil {
				return "", err
			}
			if cohort != "" && cohort != outcome.Attestation.ClientNetwork.Scope {
				return "", errors.New("client observer changed public network within a round")
			}
			cohort = outcome.Attestation.ClientNetwork.Scope
		}
	}
	return cohort, nil
}
