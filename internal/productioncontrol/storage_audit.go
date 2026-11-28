package productioncontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"

	"fugue/internal/storageaudit"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	StorageAuditPolicyResource = "maintenance/storage-audit-policy"
	StorageAuditShadowSchema   = "fugue.storage-audit-policy-shadow/v1"
)

type StorageAuditShadowBinding struct {
	Kubernetes   ParticipationKubernetesBinding `json:"kubernetes"`
	Namespace    string                         `json:"namespace"`
	ConfigMap    string                         `json:"configMap"`
	ConfigMapUID string                         `json:"configMapUid"`
}

func (s StorageAuditShadowBinding) Validate() error {
	if s.Kubernetes.Validate() != nil || !participationUID.MatchString(s.ConfigMapUID) ||
		len(validation.IsDNS1123Label(s.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(s.ConfigMap)) != 0 {
		return errors.New("invalid storage audit shadow Kubernetes binding")
	}
	return nil
}

type StorageAuditPolicyVersion struct {
	Document storageaudit.Policy `json:"document"`
	Digest   string              `json:"digest"`
}

func (v StorageAuditPolicyVersion) valid() bool {
	raw, err := json.Marshal(v.Document)
	if err != nil {
		return false
	}
	policy, err := storageaudit.ParsePolicy(raw)
	return err == nil && policy.EffectiveFrom.Nanosecond() == 0 && policy == v.Document && v.Digest == "sha256:"+policy.Digest()
}

type StorageAuditShadow struct {
	Schema                   string                           `json:"schema"`
	ExecutionEvidence        bool                             `json:"executionEvidence"`
	Binding                  StorageAuditShadowBinding        `json:"binding"`
	ConfigMapResourceVersion string                           `json:"configMapResourceVersion"`
	Provider                 storageaudit.MaintenanceObserver `json:"provider"`
	Owner                    string                           `json:"owner"`
	Activation               storageaudit.OwnerActivation     `json:"activation"`
	Current                  StorageAuditPolicyVersion        `json:"current"`
	Target                   StorageAuditPolicyVersion        `json:"target"`
	Rollback                 StorageAuditPolicyVersion        `json:"rollback"`
}

func (s StorageAuditShadow) validate() error {
	if s.Schema != StorageAuditShadowSchema || s.ExecutionEvidence || s.Binding.Validate() != nil ||
		s.ConfigMapResourceVersion == "" || len(s.ConfigMapResourceVersion) > 128 ||
		s.Provider.Validate() != nil || s.Owner != "worker" || s.Activation.ActivatedAt.IsZero() ||
		!digestPattern.MatchString("sha256:"+s.Activation.DeclarationDigest) ||
		!digestPattern.MatchString("sha256:"+s.Activation.PolicyDigest) ||
		!participationUID.MatchString(s.Activation.WorkerSecretUID) ||
		!s.Current.valid() || !s.Target.valid() || s.Current != s.Rollback || s.Current.Digest == s.Target.Digest {
		return errors.New("storage audit policy shadow does not bind a predecessor, candidate, and inverse")
	}
	return nil
}

func DecodeStorageAuditShadow(raw []byte) (StorageAuditShadow, error) {
	var s StorageAuditShadow
	if len(raw) > 16<<10 || decodeControlJSON(raw, &s) != nil {
		return StorageAuditShadow{}, errors.New("invalid storage audit policy shadow")
	}
	return s, s.validate()
}

// DraftStorageAuditPolicy binds full policy documents, the exact provider
// identities and the shadow preconditions to the native journal predecessor.
func DraftStorageAuditPolicy(cluster, request string, before Version, s StorageAuditShadow) (Submission, error) {
	if err := s.validate(); err != nil {
		return Submission{}, err
	}
	if !before.valid() || before.Generation == math.MaxInt64 || before.Digest != s.Current.Digest {
		return Submission{}, ErrResourceDrift
	}
	payload, err := json.Marshal(s)
	if err != nil {
		return Submission{}, err
	}
	i, err := NewIntent(cluster, StorageAuditPolicyResource, request, Maintenance, before.Generation,
		s.Current.Digest, s.Target.Digest, s.Rollback.Digest, payload)
	return Submission{i, payload}, err
}

func InspectStorageAuditPolicy(s Submission) (StorageAuditShadow, error) {
	plan, err := DecodeStorageAuditShadow(s.Payload)
	if err != nil {
		return StorageAuditShadow{}, err
	}
	i := s.Intent
	expected, err := DraftStorageAuditPolicy(i.ClusterID, i.RequestID, Version{i.Generation, i.CurrentDigest}, plan)
	if err != nil || expected.Intent != i || !bytes.Equal(expected.Payload, s.Payload) {
		return StorageAuditShadow{}, errors.New("production intent does not bind this exact storage audit policy shadow")
	}
	return plan, nil
}
