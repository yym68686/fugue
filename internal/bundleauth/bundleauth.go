package bundleauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

var (
	ErrUnsupportedSchemaVersion = errors.New("unsupported bundle schema version")
	ErrExpiredBundle            = errors.New("bundle expired")
	ErrMissingSignature         = errors.New("bundle signature missing")
	ErrInvalidSignature         = errors.New("bundle signature invalid")
	ErrKeyIDMismatch            = errors.New("bundle key id mismatch")
)

type Keyring struct {
	PrimaryKey    string
	PrimaryKeyID  string
	PreviousKey   string
	PreviousKeyID string
	RevokedKeyIDs map[string]struct{}
}

func NewKeyring(primaryKey, primaryKeyID, previousKey, previousKeyID string, revokedKeyIDs []string) Keyring {
	keyring := Keyring{
		PrimaryKey:    strings.TrimSpace(primaryKey),
		PrimaryKeyID:  strings.TrimSpace(primaryKeyID),
		PreviousKey:   strings.TrimSpace(previousKey),
		PreviousKeyID: strings.TrimSpace(previousKeyID),
	}
	if len(revokedKeyIDs) > 0 {
		keyring.RevokedKeyIDs = make(map[string]struct{}, len(revokedKeyIDs))
		for _, keyID := range revokedKeyIDs {
			keyID = strings.TrimSpace(keyID)
			if keyID == "" {
				continue
			}
			keyring.RevokedKeyIDs[strings.ToLower(keyID)] = struct{}{}
		}
	}
	return keyring
}

func SignDiscoveryBundle(bundle model.DiscoveryBundle, key, keyID string, validFor time.Duration) model.DiscoveryBundle {
	return SignDiscoveryBundleWithKeyring(bundle, NewKeyring(key, keyID, "", "", nil), validFor)
}

func SignDiscoveryBundleWithKeyring(bundle model.DiscoveryBundle, keyring Keyring, validFor time.Duration) model.DiscoveryBundle {
	return signBundle(bundle, keyring, validFor, bundle.GeneratedAt, func(out *model.DiscoveryBundle) {
		out.SchemaVersion = model.BundleSchemaVersionV1
		if strings.TrimSpace(out.Issuer) == "" {
			out.Issuer = model.BundleIssuerFugue
		}
	})
}

func SignEdgeRouteBundle(bundle model.EdgeRouteBundle, key, keyID string, validFor time.Duration) model.EdgeRouteBundle {
	return SignEdgeRouteBundleWithKeyring(bundle, NewKeyring(key, keyID, "", "", nil), validFor)
}

func SignEdgeRouteBundleWithKeyring(bundle model.EdgeRouteBundle, keyring Keyring, validFor time.Duration) model.EdgeRouteBundle {
	return signBundle(bundle, keyring, validFor, bundle.GeneratedAt, func(out *model.EdgeRouteBundle) {
		out.SchemaVersion = model.BundleSchemaVersionV1
		if strings.TrimSpace(out.Issuer) == "" {
			out.Issuer = model.BundleIssuerFugue
		}
	})
}

func SignEdgeDNSBundle(bundle model.EdgeDNSBundle, key, keyID string, validFor time.Duration) model.EdgeDNSBundle {
	return SignEdgeDNSBundleWithKeyring(bundle, NewKeyring(key, keyID, "", "", nil), validFor)
}

func SignEdgeDNSBundleWithKeyring(bundle model.EdgeDNSBundle, keyring Keyring, validFor time.Duration) model.EdgeDNSBundle {
	return signBundle(bundle, keyring, validFor, bundle.GeneratedAt, func(out *model.EdgeDNSBundle) {
		out.SchemaVersion = model.BundleSchemaVersionV1
		if strings.TrimSpace(out.Issuer) == "" {
			out.Issuer = model.BundleIssuerFugue
		}
	})
}

func VerifyDiscoveryBundle(bundle model.DiscoveryBundle, key, keyID string, now time.Time) error {
	return VerifyDiscoveryBundleWithKeyring(bundle, NewKeyring(key, keyID, "", "", nil), now)
}

func VerifyDiscoveryBundleWithKeyring(bundle model.DiscoveryBundle, keyring Keyring, now time.Time) error {
	if err := validateBundleSchemaVersion(bundle.SchemaVersion); err != nil {
		return err
	}
	return verifyBundleSignature(bundle, keyring, now, bundle.ValidUntil, bundle.KeyID, bundle.Signature, bundle.Signatures)
}

func VerifyEdgeRouteBundle(bundle model.EdgeRouteBundle, key, keyID string, now time.Time) error {
	return VerifyEdgeRouteBundleWithKeyring(bundle, NewKeyring(key, keyID, "", "", nil), now)
}

func VerifyEdgeRouteBundleWithKeyring(bundle model.EdgeRouteBundle, keyring Keyring, now time.Time) error {
	if err := validateBundleSchemaVersion(bundle.SchemaVersion); err != nil {
		return err
	}
	return verifyBundleSignature(bundle, keyring, now, bundle.ValidUntil, bundle.KeyID, bundle.Signature, bundle.Signatures)
}

func VerifyEdgeDNSBundle(bundle model.EdgeDNSBundle, key, keyID string, now time.Time) error {
	return VerifyEdgeDNSBundleWithKeyring(bundle, NewKeyring(key, keyID, "", "", nil), now)
}

func VerifyEdgeDNSBundleWithKeyring(bundle model.EdgeDNSBundle, keyring Keyring, now time.Time) error {
	if err := validateBundleSchemaVersion(bundle.SchemaVersion); err != nil {
		return err
	}
	return verifyBundleSignature(bundle, keyring, now, bundle.ValidUntil, bundle.KeyID, bundle.Signature, bundle.Signatures)
}

func SchemaMajor(schemaVersion string) string {
	schemaVersion = strings.TrimSpace(schemaVersion)
	if schemaVersion == "" {
		return ""
	}
	major, _, _ := strings.Cut(schemaVersion, ".")
	return major
}

func validateBundleSchemaVersion(schemaVersion string) error {
	if strings.TrimSpace(schemaVersion) == "" {
		return nil
	}
	if SchemaMajor(schemaVersion) != SchemaMajor(model.BundleSchemaVersionV1) {
		return fmt.Errorf("%w: %s", ErrUnsupportedSchemaVersion, schemaVersion)
	}
	return nil
}

func verifyBundleSignature[T any](bundle T, keyring Keyring, now, validUntil time.Time, bundleKeyID, signature string, signatures []model.BundleSignature) error {
	if !validUntil.IsZero() && !now.IsZero() && now.After(validUntil) {
		return ErrExpiredBundle
	}
	keyring = NewKeyring(keyring.PrimaryKey, keyring.PrimaryKeyID, keyring.PreviousKey, keyring.PreviousKeyID, keyring.revokedKeyIDs())
	candidates := []model.BundleSignature{
		{
			KeyID:      bundleKeyID,
			Signature:  signature,
			ValidUntil: validUntil,
		},
	}
	candidates = append(candidates, signatures...)
	if keyring.primaryKey() == "" && keyring.previousKey() == "" {
		return nil
	}
	foundSignature := false
	matchedKey := false
	invalidSignature := false
	for _, candidate := range candidates {
		keyID := strings.TrimSpace(candidate.KeyID)
		sig := strings.TrimSpace(candidate.Signature)
		if keyID == "" || sig == "" {
			continue
		}
		foundSignature = true
		if keyring.isRevoked(keyID) {
			continue
		}
		key, ok := keyring.keyForID(keyID)
		if !ok || key == "" {
			continue
		}
		matchedKey = true
		payload := cloneBundleForSigning(bundle, candidate.ValidUntil, keyID)
		raw, _ := json.Marshal(payload)
		mac := hmac.New(sha256.New, []byte(key))
		_, _ = mac.Write(raw)
		expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		if hmac.Equal([]byte(sig), []byte(expected)) {
			return nil
		}
		invalidSignature = true
	}
	if !foundSignature {
		return ErrMissingSignature
	}
	if invalidSignature {
		return ErrInvalidSignature
	}
	if matchedKey {
		return ErrInvalidSignature
	}
	if bundleKeyID != "" {
		return fmt.Errorf("%w: got %s", ErrKeyIDMismatch, strings.TrimSpace(bundleKeyID))
	}
	return ErrInvalidSignature
}

func signBundle[T any](bundle T, keyring Keyring, validFor time.Duration, generatedAt time.Time, init func(*T)) T {
	out := bundle
	if init != nil {
		init(&out)
	}
	validUntil := generatedAt
	if validFor > 0 {
		validUntil = generatedAt.Add(validFor)
	}
	keyring = NewKeyring(keyring.PrimaryKey, keyring.PrimaryKeyID, keyring.PreviousKey, keyring.PreviousKeyID, keyring.revokedKeyIDs())
	primaryKey, primaryKeyID := keyring.primaryKey(), keyring.primaryKeyID()
	if primaryKey == "" {
		switch typed := any(&out).(type) {
		case *model.DiscoveryBundle:
			typed.ValidUntil = validUntil
			typed.KeyID = primaryKeyID
		case *model.EdgeRouteBundle:
			typed.ValidUntil = validUntil
			typed.KeyID = primaryKeyID
		case *model.EdgeDNSBundle:
			typed.ValidUntil = validUntil
			typed.KeyID = primaryKeyID
		}
		return out
	}
	signatures := make([]model.BundleSignature, 0, 2)
	appendSignature := func(key, keyID string) string {
		if key == "" || keyID == "" || keyring.isRevoked(keyID) {
			return ""
		}
		payload := cloneBundleForSigning(out, validUntil, keyID)
		raw, _ := json.Marshal(payload)
		mac := hmac.New(sha256.New, []byte(key))
		_, _ = mac.Write(raw)
		return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	}
	signature := appendSignature(primaryKey, primaryKeyID)
	previousSignature := appendSignature(keyring.previousKey(), keyring.previousKeyID())
	switch typed := any(&out).(type) {
	case *model.DiscoveryBundle:
		typed.ValidUntil = validUntil
		typed.KeyID = primaryKeyID
		typed.Signature = signature
		if signature != "" {
			signatures = append(signatures, bundleSignature(typed.Issuer, primaryKeyID, signature, typed.GeneratedAt, validUntil))
		}
		if previousSignature != "" {
			signatures = append(signatures, bundleSignature(typed.Issuer, keyring.previousKeyID(), previousSignature, typed.GeneratedAt, validUntil))
		}
		typed.Signatures = signatures
	case *model.EdgeRouteBundle:
		typed.ValidUntil = validUntil
		typed.KeyID = primaryKeyID
		typed.Signature = signature
		signatures = signatures[:0]
		if signature != "" {
			signatures = append(signatures, bundleSignature(typed.Issuer, primaryKeyID, signature, typed.GeneratedAt, validUntil))
		}
		if previousSignature != "" {
			signatures = append(signatures, bundleSignature(typed.Issuer, keyring.previousKeyID(), previousSignature, typed.GeneratedAt, validUntil))
		}
		typed.Signatures = signatures
	case *model.EdgeDNSBundle:
		typed.ValidUntil = validUntil
		typed.KeyID = primaryKeyID
		typed.Signature = signature
		signatures = signatures[:0]
		if signature != "" {
			signatures = append(signatures, bundleSignature(typed.Issuer, primaryKeyID, signature, typed.GeneratedAt, validUntil))
		}
		if previousSignature != "" {
			signatures = append(signatures, bundleSignature(typed.Issuer, keyring.previousKeyID(), previousSignature, typed.GeneratedAt, validUntil))
		}
		typed.Signatures = signatures
	}
	return out
}

func (k Keyring) primaryKey() string {
	return strings.TrimSpace(k.PrimaryKey)
}

func (k Keyring) primaryKeyID() string {
	return strings.TrimSpace(k.PrimaryKeyID)
}

func (k Keyring) previousKey() string {
	return strings.TrimSpace(k.PreviousKey)
}

func (k Keyring) previousKeyID() string {
	return strings.TrimSpace(k.PreviousKeyID)
}

func (k Keyring) revokedKeyIDs() []string {
	if len(k.RevokedKeyIDs) == 0 {
		return nil
	}
	out := make([]string, 0, len(k.RevokedKeyIDs))
	for keyID := range k.RevokedKeyIDs {
		out = append(out, keyID)
	}
	return out
}

func (k Keyring) isRevoked(keyID string) bool {
	if len(k.RevokedKeyIDs) == 0 {
		return false
	}
	_, ok := k.RevokedKeyIDs[strings.ToLower(strings.TrimSpace(keyID))]
	return ok
}

func (k Keyring) keyForID(keyID string) (string, bool) {
	keyID = strings.TrimSpace(keyID)
	if keyID == "" || k.isRevoked(keyID) {
		return "", false
	}
	if k.primaryKeyID() != "" && strings.EqualFold(k.primaryKeyID(), keyID) {
		return k.primaryKey(), true
	}
	if k.previousKeyID() != "" && strings.EqualFold(k.previousKeyID(), keyID) {
		return k.previousKey(), true
	}
	return "", false
}

func bundleSignature(issuer, keyID, signature string, generatedAt, validUntil time.Time) model.BundleSignature {
	return model.BundleSignature{
		SchemaVersion: model.BundleSchemaVersionV1,
		Issuer:        strings.TrimSpace(issuer),
		KeyID:         strings.TrimSpace(keyID),
		Signature:     strings.TrimSpace(signature),
		GeneratedAt:   generatedAt,
		ValidUntil:    validUntil,
	}
}

type bundleSigningPayload struct {
	SchemaVersion      string            `json:"schema_version,omitempty"`
	Version            string            `json:"version,omitempty"`
	Generation         string            `json:"generation,omitempty"`
	PreviousGeneration string            `json:"previous_generation,omitempty"`
	GeneratedAt        time.Time         `json:"generated_at,omitempty"`
	ValidUntil         time.Time         `json:"valid_until,omitempty"`
	Issuer             string            `json:"issuer,omitempty"`
	KeyID              string            `json:"key_id,omitempty"`
	EdgeID             string            `json:"edge_id,omitempty"`
	EdgeGroupID        string            `json:"edge_group_id,omitempty"`
	Routes             any               `json:"routes,omitempty"`
	TLSAllowlist       any               `json:"tls_allowlist,omitempty"`
	Records            any               `json:"records,omitempty"`
	APIEndpoints       any               `json:"api_endpoints,omitempty"`
	Kubernetes         any               `json:"kubernetes,omitempty"`
	Registry           any               `json:"registry,omitempty"`
	EdgeGroups         any               `json:"edge_groups,omitempty"`
	EdgeNodes          any               `json:"edge_nodes,omitempty"`
	DNSNodes           any               `json:"dns_nodes,omitempty"`
	PlatformRoutes     any               `json:"platform_routes,omitempty"`
	PublicRuntimeEnv   map[string]string `json:"public_runtime_env,omitempty"`
}

func cloneBundleForSigning[T any](bundle T, validUntil time.Time, keyID string) bundleSigningPayload {
	var payload bundleSigningPayload
	switch typed := any(bundle).(type) {
	case model.DiscoveryBundle:
		payload = bundleSigningPayload{
			SchemaVersion:      typed.SchemaVersion,
			Generation:         typed.Generation,
			PreviousGeneration: typed.PreviousGeneration,
			GeneratedAt:        typed.GeneratedAt,
			ValidUntil:         validUntil,
			Issuer:             typed.Issuer,
			KeyID:              keyID,
			APIEndpoints:       typed.APIEndpoints,
			Kubernetes:         typed.Kubernetes,
			Registry:           typed.Registry,
			EdgeGroups:         typed.EdgeGroups,
			EdgeNodes:          typed.EdgeNodes,
			DNSNodes:           typed.DNSNodes,
			PlatformRoutes:     typed.PlatformRoutes,
			PublicRuntimeEnv:   typed.PublicRuntimeEnv,
		}
	case model.EdgeRouteBundle:
		payload = bundleSigningPayload{
			SchemaVersion:      typed.SchemaVersion,
			Version:            typed.Version,
			Generation:         typed.Generation,
			PreviousGeneration: typed.PreviousGeneration,
			GeneratedAt:        typed.GeneratedAt,
			ValidUntil:         validUntil,
			Issuer:             typed.Issuer,
			KeyID:              keyID,
			EdgeID:             typed.EdgeID,
			EdgeGroupID:        typed.EdgeGroupID,
			Routes:             typed.Routes,
			TLSAllowlist:       typed.TLSAllowlist,
		}
	case model.EdgeDNSBundle:
		payload = bundleSigningPayload{
			SchemaVersion:      typed.SchemaVersion,
			Version:            typed.Version,
			Generation:         typed.Generation,
			PreviousGeneration: typed.PreviousGeneration,
			GeneratedAt:        typed.GeneratedAt,
			ValidUntil:         validUntil,
			Issuer:             typed.Issuer,
			KeyID:              keyID,
			EdgeID:             typed.DNSNodeID,
			EdgeGroupID:        typed.EdgeGroupID,
			Records:            typed.Records,
		}
	}
	return payload
}
