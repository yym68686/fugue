package livediagnostics

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const CatalogProtocol = "fugue.diagnostics/v1"
const CatalogConfigMap = "fugue-diagnostic-catalog"
const TrustConfigMap = "fugue-diagnostic-trust"
const ProbeBinary = "/usr/local/bin/fugue-diagnostic-probe"
const ProbeRefAnnotation = "fugue.pro/diagnostic-probe-ref"
const ProbeDigestAnnotation = "fugue.pro/diagnostic-probe-digest"
const CatalogDigestAnnotation = "fugue.pro/diagnostic-catalog-digest"
const ProbeRequestEnv = "FUGUE_DIAGNOSTIC_REQUEST"

var imageDigestPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}$`)
var probeIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

type Parameter struct {
	Description string   `json:"description,omitempty"`
	Required    bool     `json:"required,omitempty"`
	Default     string   `json:"default,omitempty"`
	Enum        []string `json:"enum,omitempty"`
	Pattern     string   `json:"pattern,omitempty"`
	MaxLength   int      `json:"max_length,omitempty"`
}

type Probe struct {
	ID                 string               `json:"id"`
	Description        string               `json:"description,omitempty"`
	Image              string               `json:"image"`
	Profile            string               `json:"profile"`
	TargetTypes        []TargetType         `json:"target_types"`
	MaxDurationSeconds int                  `json:"max_duration_seconds"`
	Parameters         map[string]Parameter `json:"parameters"`
	Config             json.RawMessage      `json:"config,omitempty"`
}

type CatalogPolicy struct {
	Namespaces     []string `json:"namespaces"`
	Profiles       []string `json:"profiles"`
	ServiceAccount string   `json:"service_account"`
}

type Catalog struct {
	SourceRevision string        `json:"source_revision,omitempty"`
	Protocol       string        `json:"protocol"`
	Generation     int64         `json:"generation"`
	RunnerImage    string        `json:"runner_image"`
	Policy         CatalogPolicy `json:"policy"`
	Probes         []Probe       `json:"probes"`
}

type SignedCatalog struct {
	KeyID     string `json:"key_id"`
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

type VerifiedCatalog struct {
	Catalog
	Digest string `json:"digest"`
	Status string `json:"status"`
}

type ProbeDescriptor struct {
	ID                 string               `json:"id"`
	Digest             string               `json:"digest"`
	Description        string               `json:"description,omitempty"`
	Image              string               `json:"image"`
	Profile            string               `json:"profile"`
	TargetTypes        []TargetType         `json:"target_types"`
	MaxDurationSeconds int                  `json:"max_duration_seconds"`
	Parameters         map[string]Parameter `json:"parameters"`
}

func DecodeStrict(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("unexpected trailing JSON data")
	}
	return nil
}

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func VerifyCatalog(data []byte, keys map[string]string) (VerifiedCatalog, error) {
	if len(data) > 256<<10 {
		return VerifiedCatalog{}, errors.New("diagnostic catalog exceeds 256 KiB")
	}
	var signed SignedCatalog
	if err := DecodeStrict(data, &signed); err != nil {
		return VerifiedCatalog{}, err
	}
	key, err := base64.StdEncoding.DecodeString(keys[signed.KeyID])
	if err != nil || len(key) != ed25519.PublicKeySize {
		return VerifiedCatalog{}, errors.New("diagnostic catalog signing key is not trusted")
	}
	payload, err := base64.StdEncoding.DecodeString(signed.Payload)
	if err != nil {
		return VerifiedCatalog{}, errors.New("invalid diagnostic catalog payload")
	}
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(key), payload, signature) {
		return VerifiedCatalog{}, errors.New("diagnostic catalog signature is invalid")
	}
	var c Catalog
	if err := DecodeStrict(payload, &c); err != nil {
		return VerifiedCatalog{}, err
	}
	if err := c.Validate(); err != nil {
		return VerifiedCatalog{}, err
	}
	return VerifiedCatalog{Catalog: c, Digest: Digest(payload), Status: "current"}, nil
}

func SignCatalog(c Catalog, keyID string, key ed25519.PrivateKey) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if len(key) != ed25519.PrivateKeySize || keyID == "" {
		return nil, errors.New("invalid catalog signing identity")
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return json.Marshal(SignedCatalog{KeyID: keyID, Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))})
}

func (c Catalog) Validate() error {
	if c.SourceRevision != "" && !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(c.SourceRevision) {
		return errors.New("invalid catalog source revision")
	}
	if c.Protocol != CatalogProtocol || c.Generation < 1 || !imageDigestPattern.MatchString(c.RunnerImage) {
		return errors.New("invalid diagnostic catalog protocol, generation or runner digest")
	}
	if len(c.Policy.Namespaces) == 0 || len(c.Policy.Profiles) == 0 || !probeIDPattern.MatchString(c.Policy.ServiceAccount) {
		return errors.New("diagnostic catalog requires explicit namespaces, profiles and service account")
	}
	for _, p := range c.Policy.Profiles {
		if p != "host-read" && p != "cluster-read" && p != "process-profile" && p != "kernel-profile" {
			return fmt.Errorf("unsupported capability profile %q", p)
		}
	}
	for _, n := range c.Policy.Namespaces {
		if n != "*" && !probeIDPattern.MatchString(n) {
			return errors.New("invalid catalog namespace policy")
		}
	}
	if len(c.Probes) > 64 {
		return errors.New("too many catalog probes")
	}
	seen := map[string]bool{}
	for _, p := range c.Probes {
		if !probeIDPattern.MatchString(p.ID) || seen[p.ID] {
			return errors.New("invalid or duplicate probe id")
		}
		seen[p.ID] = true
		if !imageDigestPattern.MatchString(p.Image) || !contains(c.Policy.Profiles, p.Profile) {
			return errors.New("probe image or capability profile is not allowed")
		}
		if p.MaxDurationSeconds < 5 || p.MaxDurationSeconds > 360 || len(p.Config) > 32<<10 || len(p.Parameters) > 32 || p.Parameters == nil {
			return errors.New("probe budget exceeds protocol bounds")
		}
		if len(p.TargetTypes) == 0 {
			return errors.New("probe requires target types")
		}
		for _, t := range p.TargetTypes {
			if t != TargetPlatformComponent && t != TargetNodeProcess && t != TargetNode {
				return errors.New("unsupported probe target type")
			}
		}
		for name, param := range p.Parameters {
			if !probeIDPattern.MatchString(strings.ReplaceAll(name, "_", "-")) || param.MaxLength < 0 || param.MaxLength > 4096 {
				return errors.New("invalid probe parameter contract")
			}
			if param.Pattern != "" {
				if _, err := regexp.Compile(param.Pattern); err != nil {
					return err
				}
			}
			if param.Default != "" {
				if err := param.validate(param.Default); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c VerifiedCatalog) Resolve(ref string, params map[string]string) (Probe, map[string]string, error) {
	id, pinned, _ := strings.Cut(strings.TrimSpace(ref), "@")
	for _, p := range c.Probes {
		if p.ID == id {
			if pinned != "" && pinned != p.Digest() {
				return Probe{}, nil, errors.New("probe manifest digest does not match the requested reference")
			}
			out := map[string]string{}
			for k := range params {
				if _, ok := p.Parameters[k]; !ok {
					return Probe{}, nil, fmt.Errorf("unsupported probe parameter %q", k)
				}
			}
			for k, spec := range p.Parameters {
				v, ok := params[k]
				if !ok {
					v = spec.Default
				}
				if v == "" && spec.Required {
					return Probe{}, nil, fmt.Errorf("probe parameter %q is required", k)
				}
				if v != "" {
					if err := spec.validate(v); err != nil {
						return Probe{}, nil, fmt.Errorf("parameter %s: %w", k, err)
					}
					out[k] = v
				}
			}
			return p, out, nil
		}
	}
	return Probe{}, nil, errors.New("probe reference is not registered")
}
func (p Parameter) validate(s string) error {
	max := p.MaxLength
	if max == 0 {
		max = 4096
	}
	if len(s) > max {
		return errors.New("value exceeds maximum length")
	}
	if len(p.Enum) > 0 && !contains(p.Enum, s) {
		return errors.New("value is not in the allowed enum")
	}
	if p.Pattern != "" {
		r, err := regexp.Compile(p.Pattern)
		if err != nil || !r.MatchString(s) {
			return errors.New("value does not match parameter pattern")
		}
	}
	return nil
}
func (p Probe) Digest() string { b, _ := json.Marshal(p); return Digest(b) }
func (c VerifiedCatalog) Descriptors() []ProbeDescriptor {
	out := make([]ProbeDescriptor, 0, len(c.Probes))
	for _, p := range c.Probes {
		out = append(out, ProbeDescriptor{p.ID, p.Digest(), p.Description, p.Image, p.Profile, p.TargetTypes, p.MaxDurationSeconds, p.Parameters})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (c Catalog) AllowsNamespace(ns string) bool {
	return contains(c.Policy.Namespaces, ns) || contains(c.Policy.Namespaces, "*")
}
func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}
