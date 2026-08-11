package edgeauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

const (
	APIVersion            = "release.fugue.dev/v1"
	RouteBundleRecordKind = "RouteBundleRecord"
)

var (
	groupPattern     = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
	componentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,63}$`)
	shaPattern       = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	signaturePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43,128}$`)
)

// RouteBundleRecord is the immutable contract shared by the Edge Control
// producer and the Guardian authority writer. This package contains no
// rollout, Kubernetes, process or network behavior.
type RouteBundleRecord struct {
	APIVersion           string `json:"apiVersion"`
	Kind                 string `json:"kind"`
	GroupID              string `json:"groupId"`
	Epoch                int64  `json:"epoch"`
	BundleDigest         string `json:"bundleDigest"`
	SourceSHA            string `json:"sourceSha"`
	ControlImageDigest   string `json:"controlImageDigest"`
	InventoryDigest      string `json:"inventoryDigest"`
	ManifestDigest       string `json:"manifestDigest"`
	HealthContractDigest string `json:"healthContractDigest"`
	IssuedAt             string `json:"issuedAt"`
	KeyID                string `json:"keyId"`
	Signature            string `json:"signature"`
	RecordDigest         string `json:"recordDigest"`
}

func (record RouteBundleRecord) Seal() (RouteBundleRecord, error) {
	record.APIVersion = APIVersion
	record.Kind = RouteBundleRecordKind
	record.RecordDigest = ""
	if err := record.validateUnsigned(); err != nil {
		return RouteBundleRecord{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return RouteBundleRecord{}, err
	}
	record.RecordDigest = digest(raw)
	return record, nil
}

func (record RouteBundleRecord) Validate() error {
	if !digestPattern.MatchString(record.RecordDigest) || record.validateUnsigned() != nil {
		return errors.New("route bundle record is invalid")
	}
	copy := record
	copy.RecordDigest = ""
	raw, err := json.Marshal(copy)
	if err != nil || digest(raw) != record.RecordDigest {
		return errors.New("route bundle record digest is invalid")
	}
	return nil
}

func (record RouteBundleRecord) validateUnsigned() error {
	if record.APIVersion != APIVersion || record.Kind != RouteBundleRecordKind ||
		!groupPattern.MatchString(record.GroupID) || record.Epoch < 1 ||
		!digestPattern.MatchString(record.BundleDigest) || !shaPattern.MatchString(record.SourceSHA) ||
		!digestPattern.MatchString(record.ControlImageDigest) || !digestPattern.MatchString(record.InventoryDigest) ||
		!digestPattern.MatchString(record.ManifestDigest) || !digestPattern.MatchString(record.HealthContractDigest) ||
		!componentPattern.MatchString(record.KeyID) || !signaturePattern.MatchString(record.Signature) {
		return errors.New("route bundle record identity is invalid")
	}
	issuedAt, err := time.Parse(time.RFC3339Nano, record.IssuedAt)
	if err != nil || !issuedAt.Equal(issuedAt.UTC()) {
		return errors.New("route bundle record issuance time is invalid")
	}
	return nil
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
