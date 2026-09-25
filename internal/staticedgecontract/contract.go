// Package staticedgecontract defines the standalone manager protocol. It has no
// dependency on Fugue control-plane authentication, inventory or routing.
package staticedgecontract

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

const SchemaV1 = "fugue.static-edge/v1"
const RPCSchema = "fugue.static-edge.rpc/v1"
const MaxBytes = 4 << 20

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$`)

func ValidID(s string) bool { return identifier.MatchString(s) }

type Bundle struct {
	Schema       string          `json:"schema" yaml:"schema"`
	EdgeID       string          `json:"edge_id" yaml:"edge_id"`
	Role         string          `json:"role" yaml:"role"`
	Generation   uint64          `json:"generation" yaml:"generation"`
	Mode         string          `json:"mode" yaml:"mode"`
	CaddyConfig  json.RawMessage `json:"caddy_config" yaml:"-"`
	HealthChecks []string        `json:"health_checks" yaml:"health_checks"`
	BundleDigest string          `json:"bundle_digest,omitempty" yaml:"bundle_digest,omitempty"`
	Signature    string          `json:"signature,omitempty" yaml:"signature,omitempty"`
	SigningKeyID string          `json:"signing_key_id" yaml:"signing_key_id"`
}
type Request struct {
	Schema           string  `json:"schema"`
	EdgeID           string  `json:"edge_id"`
	RequestID        string  `json:"request_id"`
	Operation        string  `json:"operation"`
	ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
	TargetDigest     string  `json:"target_digest,omitempty"`
	LookupRequestID  string  `json:"lookup_request_id,omitempty"`
	CredentialSlot   string  `json:"credential_slot,omitempty"`
	Bundle           *Bundle `json:"bundle,omitempty"`
}
type Response struct {
	Schema    string    `json:"schema"`
	RequestID string    `json:"request_id"`
	EdgeID    string    `json:"edge_id"`
	OK        bool      `json:"ok"`
	Status    int       `json:"status"`
	Error     string    `json:"error,omitempty"`
	Result    *Observed `json:"result,omitempty"`
	Receipt   *Receipt  `json:"receipt,omitempty"`
	Receipts  []Receipt `json:"receipts,omitempty"`
}
type Observed struct {
	EdgeID              string `json:"edge_id"`
	Role                string `json:"role"`
	Revision            uint64 `json:"revision"`
	ActiveGeneration    uint64 `json:"active_generation,omitempty"`
	CandidateGeneration uint64 `json:"candidate_generation,omitempty"`
	LKGGeneration       uint64 `json:"lkg_generation,omitempty"`
	ActiveDigest        string `json:"active_digest,omitempty"`
	CandidateDigest     string `json:"candidate_digest,omitempty"`
	LKGDigest           string `json:"lkg_digest,omitempty"`
	RuntimeDigest       string `json:"runtime_digest,omitempty"`
	RuntimeMatches      bool   `json:"runtime_matches"`
	Draining            bool   `json:"draining"`
	Ready               bool   `json:"ready"`
	PendingRequestID    string `json:"pending_request_id,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	CredentialSlot      string `json:"credential_slot"`
	CertificateNotAfter string `json:"certificate_not_after,omitempty"`
}
type Receipt struct {
	RequestID       string `json:"request_id"`
	Operation       string `json:"operation"`
	Actor           string `json:"actor"`
	Outcome         string `json:"outcome"`
	Revision        uint64 `json:"revision"`
	BundleDigest    string `json:"bundle_digest,omitempty"`
	RuntimeVerified bool   `json:"runtime_verified"`
	LKGPreserved    bool   `json:"lkg_preserved"`
	Error           string `json:"error,omitempty"`
	CreatedAt       string `json:"created_at"`
}

func ReadOnly(op string) bool {
	switch op {
	case "status", "health", "evidence", "operation", "plan", "cert-status":
		return true
	}
	return false
}
func OperationAllowed(op string) bool {
	if ReadOnly(op) {
		return true
	}
	switch op {
	case "stage", "adopt", "activate", "drain", "undrain", "rollback", "recover", "cert-rotate":
		return true
	}
	return false
}

// StrictJSON rejects duplicate keys, unknown fields, trailing values and oversized
// input. json.Number avoids changing signed integers through float64 conversion.
func StrictJSON(raw []byte, value any) error {
	if len(raw) > MaxBytes {
		return errors.New("JSON exceeds size limit")
	}
	if _, err := CanonicalJSON(raw); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(value); err != nil {
		return err
	}
	return nil
}
func CanonicalJSON(raw []byte) ([]byte, error) {
	if len(raw) > MaxBytes {
		return nil, errors.New("JSON exceeds size limit")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var parse func(int) (any, error)
	parse = func(depth int) (any, error) {
		if depth > 64 {
			return nil, errors.New("JSON nesting exceeds limit")
		}
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return tok, nil
		}
		switch delim {
		case '{':
			m := map[string]any{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return nil, e
				}
				key, ok := k.(string)
				if !ok {
					return nil, errors.New("invalid JSON key")
				}
				if _, exists := m[key]; exists {
					return nil, fmt.Errorf("duplicate JSON key %q", key)
				}
				v, e := parse(depth + 1)
				if e != nil {
					return nil, e
				}
				m[key] = v
			}
			_, err = d.Token()
			return m, err
		case '[':
			a := []any{}
			for d.More() {
				v, e := parse(depth + 1)
				if e != nil {
					return nil, e
				}
				a = append(a, v)
			}
			_, err = d.Token()
			return a, err
		}
		return nil, errors.New("invalid JSON delimiter")
	}
	v, err := parse(0)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("expected exactly one JSON value")
	}
	return json.Marshal(v)
}
func Hash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func ConfigDigest(raw []byte) (string, error) {
	b, e := CanonicalJSON(raw)
	if e != nil {
		return "", e
	}
	return Hash(b), nil
}
func (b Bundle) Digest() (string, error) {
	b.BundleDigest = ""
	b.Signature = ""
	raw, e := json.Marshal(b)
	if e != nil {
		return "", e
	}
	canonical, e := CanonicalJSON(raw)
	if e != nil {
		return "", e
	}
	return Hash(canonical), nil
}
func ValidateBundle(b *Bundle) error {
	if b == nil || b.Schema != SchemaV1 || !ValidID(b.EdgeID) || !ValidID(b.SigningKeyID) || b.Generation == 0 {
		return errors.New("bundle requires supported schema, valid edge_id, signing_key_id and positive generation")
	}
	if b.Role != "edge" && b.Role != "origin" {
		return errors.New("bundle role must be edge or origin")
	}
	if b.Mode != "serving" && b.Mode != "draining" {
		return errors.New("bundle mode must be serving or draining")
	}
	if len(b.CaddyConfig) > 256<<10 {
		return errors.New("configuration exceeds 256 KiB bound")
	}
	var obj map[string]json.RawMessage
	if err := StrictJSON(b.CaddyConfig, &obj); err != nil || obj == nil {
		return errors.New("caddy_config must be a JSON object")
	}
	if len(b.HealthChecks) == 0 || len(b.HealthChecks) > 16 {
		return errors.New("bundle requires 1-16 health check references")
	}
	seen := map[string]bool{}
	for _, id := range b.HealthChecks {
		if !ValidID(id) || seen[id] {
			return errors.New("invalid or duplicate health check reference")
		}
		seen[id] = true
	}
	d, e := b.Digest()
	if e != nil {
		return e
	}
	if b.BundleDigest != "" && b.BundleDigest != d {
		return errors.New("bundle digest mismatch")
	}
	b.BundleDigest = d
	return nil
}
func SignBundle(b *Bundle, key ed25519.PrivateKey, keyID string) error {
	if len(key) != ed25519.PrivateKeySize {
		return errors.New("invalid Ed25519 private key")
	}
	b.SigningKeyID = keyID
	b.BundleDigest = ""
	b.Signature = ""
	if err := ValidateBundle(b); err != nil {
		return err
	}
	b.Signature = hex.EncodeToString(ed25519.Sign(key, []byte(RPCSchema+"\n"+b.BundleDigest)))
	return nil
}
func VerifyBundle(b Bundle, key ed25519.PublicKey) error {
	if b.BundleDigest == "" || b.Signature == "" {
		return errors.New("signed bundle required")
	}
	if err := ValidateBundle(&b); err != nil {
		return err
	}
	sig, err := hex.DecodeString(b.Signature)
	if err != nil || len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, []byte(RPCSchema+"\n"+b.BundleDigest), sig) {
		return errors.New("bundle signature verification failed")
	}
	return nil
}
func ParsePrivateKeyPEM(raw []byte) (ed25519.PrivateKey, error) {
	b, rest := pem.Decode(raw)
	if b == nil || strings.TrimSpace(string(rest)) != "" {
		return nil, errors.New("one PKCS8 PEM private key required")
	}
	k, e := x509.ParsePKCS8PrivateKey(b.Bytes)
	if e != nil {
		return nil, e
	}
	p, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("Ed25519 key required")
	}
	return p, nil
}
func ParsePublicKeyPEM(raw []byte) (ed25519.PublicKey, error) {
	b, rest := pem.Decode(raw)
	if b == nil || strings.TrimSpace(string(rest)) != "" {
		return nil, errors.New("one PKIX PEM public key required")
	}
	k, e := x509.ParsePKIXPublicKey(b.Bytes)
	if e != nil {
		return nil, e
	}
	p, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("Ed25519 key required")
	}
	return p, nil
}
