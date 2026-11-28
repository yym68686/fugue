package productioncontrol

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

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
	PublishedPolicyRetired   bool                             `json:"publishedPolicyRetired,omitempty"`
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

// DispatchStorageAuditPolicy commits the exact claim, provider CAS and
// provider receipt in one database transaction. A lost response can be
// recovered from the existing claim without attempting another write.
func (j *Journal) DispatchStorageAuditPolicy(ctx context.Context, cluster, actor, intentDigest, approvalDigest string,
	verifyPublishedBoundary func(context.Context, StorageAuditShadow) error) (Claim, bool, error) {
	scope := Scope{cluster, StorageAuditPolicyResource}
	if j == nil || !scope.valid() || !tokenPattern.MatchString(actor) || verifyPublishedBoundary == nil ||
		!digestPattern.MatchString(intentDigest) || !digestPattern.MatchString(approvalDigest) {
		return Claim{}, false, errors.New("invalid storage audit policy dispatch")
	}

	var claim Claim
	record, err := j.record(ctx, intentDigest, func(tx *sql.Tx) (bool, error) {
		if err := lockResource(ctx, tx, scope); err != nil {
			return false, err
		}
		existing, err := readClaim(ctx, tx, intentDigest)
		if err == nil {
			if existing.Scope != scope || existing.ApprovalDigest != approvalDigest {
				return false, ErrStaleFence
			}
			claim = existing
			return false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		intent, raw, err := readIntent(ctx, tx, intentDigest)
		if err != nil || intent.scope() != scope {
			return false, errors.Join(ErrStaleFence, err)
		}
		plan, err := InspectStorageAuditPolicy(Submission{intent, raw})
		if err != nil {
			return false, err
		}
		if !plan.PublishedPolicyRetired {
			return false, errors.New("the approved shadow still uses the legacy published storage audit policy")
		}
		state, err := readResource(ctx, tx, scope)
		if err != nil {
			return false, err
		}
		if state.Pending != "" {
			return false, ErrResourceBusy
		}
		if state.LastVerified != (Version{intent.Generation, intent.CurrentDigest}) || state.LastFence == math.MaxInt64 {
			return false, ErrResourceDrift
		}
		if err := verifyRecordedApproval(ctx, tx, j, intent, intentDigest, approvalDigest); err != nil {
			return false, err
		}
		if err := verifyPublishedBoundary(ctx, plan); err != nil {
			return false, err
		}
		claim = Claim{scope, state.LastFence + 1, intentDigest, approvalDigest}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.execution_claims
			(cluster_id,resource_id,fence,intent_digest,approval_digest,actor) VALUES ($1,$2,$3,$4,$5,$6)`,
			claim.ClusterID, claim.ResourceID, claim.Fence, intentDigest, approvalDigest, actor); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `SELECT fugue_storage_audit.commit_native_policy($1)`, intentDigest)
		return err == nil, err
	})
	if err != nil {
		return Claim{}, false, err
	}
	return claim, record.Created, nil
}

type StorageAuditPolicyTransition struct {
	Claim             Claim     `json:"claim"`
	Verified          Version   `json:"verified"`
	ObservationDigest string    `json:"observationDigest"`
	AuthorityDigest   string    `json:"authorityDigest"`
	RecordedAt        time.Time `json:"recordedAt"`
}

// ReadStorageAuditPolicyTransition checks the current selected policy and the
// immutable receipt. An independent observer uses a dedicated read-only role.
func (j *Journal) ReadStorageAuditPolicyTransition(ctx context.Context, claim Claim) (StorageAuditPolicyTransition, error) {
	if j == nil || claim.ResourceID != StorageAuditPolicyResource || !claim.Scope.valid() ||
		claim.Fence < 1 || !digestPattern.MatchString(claim.IntentDigest) || !digestPattern.MatchString(claim.ApprovalDigest) {
		return StorageAuditPolicyTransition{}, ErrStaleFence
	}
	intent, raw, err := j.ReadIntent(ctx, claim.IntentDigest)
	if err != nil {
		return StorageAuditPolicyTransition{}, err
	}
	return readStorageAuditPolicyTransition(ctx, j.db, claim, intent, raw)
}

func readStorageAuditPolicyTransition(ctx context.Context, db queryRow, claim Claim, intent Intent, raw []byte) (StorageAuditPolicyTransition, error) {
	plan, err := InspectStorageAuditPolicy(Submission{intent, raw})
	if err != nil || intent.scope() != claim.Scope {
		return StorageAuditPolicyTransition{}, ErrStaleFence
	}
	var committed time.Time
	var freeze, activation, xid string
	err = db.QueryRowContext(ctx, `SELECT r.recorded_at,f.evidence_digest,a.activation_evidence,r.writer_xid::text
		FROM fugue_storage_audit.native_policy_receipts r
		JOIN fugue_storage_audit.native_policy_freeze f ON f.cluster_id=r.cluster_id
		JOIN fugue_storage_audit.native_policy_authority a ON a.writer_oid=r.writer_oid
		JOIN fugue_storage_audit.lifecycle_control p ON p.singleton AND p.owner='worker'
		JOIN fugue_control.execution_claims c ON c.intent_digest=r.intent_digest AND c.cluster_id=r.cluster_id
			AND c.resource_id='maintenance/storage-audit-policy' AND c.approval_digest=r.approval_digest AND c.fence=r.fence
		CROSS JOIN pg_catalog.pg_control_system() system
		WHERE r.intent_digest=$1 AND r.approval_digest=$2 AND r.cluster_id=$3 AND r.fence=$4
			AND r.previous_digest=$5 AND r.target_digest=$6
			AND r.previous_policy=(convert_from($7::bytea,'UTF8')::jsonb)#>'{current,document}'
			AND r.target_policy=(convert_from($7::bytea,'UTF8')::jsonb)#>'{target,document}'
			AND 'sha256:' || p.policy_sha256=r.target_digest AND p.policy_document=r.target_policy
			AND NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.current_database()=$8 AND system.system_identifier::text=$9`,
		claim.IntentDigest, claim.ApprovalDigest, claim.ClusterID, claim.Fence, intent.CurrentDigest,
		intent.TargetDigest, raw, plan.Provider.Database, plan.Provider.SystemID).
		Scan(&committed, &freeze, &activation, &xid)
	if err != nil {
		return StorageAuditPolicyTransition{}, errors.Join(errors.New("storage audit policy lacks a matching committed receipt and selected policy"), err)
	}
	policy, err := signedBytes("fugue.storage-audit.policy-observation.v1", plan.Target)
	if err != nil {
		return StorageAuditPolicyTransition{}, err
	}
	authority, err := signedBytes("fugue.storage-audit.policy-receipt.v1", struct {
		Claim                           Claim
		Freeze, Enrollment, Transaction string
		CommittedAt                     time.Time
	}{claim, freeze, activation, xid, committed.UTC()})
	if err != nil {
		return StorageAuditPolicyTransition{}, err
	}
	return StorageAuditPolicyTransition{
		Claim: claim, Verified: Version{intent.Generation + 1, intent.TargetDigest},
		ObservationDigest: digest(policy), AuthorityDigest: digest(authority), RecordedAt: committed.UTC(),
	}, nil
}

func verifyStorageAuditPolicyClosure(ctx context.Context, tx *sql.Tx, claim Claim, intent Intent, result ExecutionResult) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1',0))`); err != nil {
		return err
	}
	_, raw, err := readIntent(ctx, tx, claim.IntentDigest)
	if err != nil {
		return err
	}
	observed, err := readStorageAuditPolicyTransition(ctx, tx, claim, intent, raw)
	if err != nil {
		return err
	}
	if result.Outcome != Applied || result.Verified != observed.Verified {
		return ErrStaleFence
	}
	return nil
}

// NewStorageAuditPolicyExecutionJournal requires live observation of the
// already committed receipt again after checking the independent signature.
func NewStorageAuditPolicyExecutionJournal(j *Journal, scope Scope, source WitnessReader,
	trust map[string]TrustedWitness) (*ExecutionJournal, error) {
	if j == nil || !scope.valid() || scope.ResourceID != StorageAuditPolicyResource {
		return nil, errors.New("invalid storage audit execution scope")
	}
	for _, witness := range trust {
		if witness.Scope != scope {
			return nil, errors.New("storage audit observer trust must use the exact resource scope")
		}
	}
	e, err := NewObservedExecutionJournal(j, source, trust, func(ctx context.Context, signed ExecutionWitness) error {
		if signed.Kind != TerminalWitness || signed.Outcome != Applied {
			return errors.New("expected a signed applied storage audit policy receipt")
		}
		observed, err := j.ReadStorageAuditPolicyTransition(ctx, Claim{scope, signed.Fence, signed.IntentDigest, signed.ApprovalDigest})
		if err != nil {
			return err
		}
		if observed.Verified != signed.version() || observed.ObservationDigest != signed.ObservationDigest ||
			observed.AuthorityDigest != signed.AuthorityDigest || signed.ObservedAt < observed.RecordedAt.Unix() {
			return errors.New("signed storage audit receipt differs from current provider readback")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.storageAudit = true
	return e, nil
}
