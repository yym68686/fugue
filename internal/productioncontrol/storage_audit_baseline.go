package productioncontrol

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/storageaudit"
)

type StorageAuditPolicyBaseline struct {
	Scope             Scope           `json:"scope"`
	Verified          Version         `json:"verified"`
	Policy            json.RawMessage `json:"policy"`
	FreezeEvidence    string          `json:"freezeEvidence"`
	FrozenAt          time.Time       `json:"frozenAt"`
	ObservationDigest string          `json:"observationDigest"`
}

func storageAuditPolicyBaselineVersion(raw []byte) (Version, error) {
	p, err := storageaudit.ParsePolicy(raw)
	if err != nil || p.EffectiveFrom.Nanosecond() != 0 {
		return Version{}, errors.New("invalid storage audit baseline policy")
	}
	canonical, err := json.Marshal(p)
	if err != nil || !bytes.Equal(raw, canonical) {
		return Version{}, errors.New("storage audit baseline requires the canonical policy")
	}
	return Version{1, "sha256:" + p.Digest()}, nil
}

// ReadFrozenStorageAuditPolicy observes only the exact PostgreSQL primary
// and only while its policy is frozen with no enrolled writer.
func (j *Journal) ReadFrozenStorageAuditPolicy(ctx context.Context, scope Scope, systemID, database string) (StorageAuditPolicyBaseline, error) {
	if j == nil || !scope.valid() || scope.ResourceID != StorageAuditPolicyResource ||
		(storageaudit.MaintenanceObserver{SystemID: systemID, Database: database, Role: "policy-observer"}).Validate() != nil {
		return StorageAuditPolicyBaseline{}, errors.New("invalid frozen storage audit provider scope")
	}
	var raw []byte
	var digestFromProvider, approval, ownerPolicy, workerUID string
	var enrolled time.Time
	observed := StorageAuditPolicyBaseline{Scope: scope}
	err := j.db.QueryRowContext(ctx, `SELECT f.baseline_policy,f.baseline_digest,f.evidence_digest,f.frozen_at,
			a.approval_sha256,a.policy_sha256,a.worker_secret_uid::text,a.activated_at
		FROM fugue_storage_audit.native_policy_freeze f
		JOIN fugue_storage_audit.lifecycle_control p USING (singleton)
		JOIN fugue_storage_audit.lifecycle_activation a USING (singleton)
		CROSS JOIN pg_catalog.pg_control_system() system
		WHERE f.cluster_id=$1 AND NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.current_database()=$2
		  AND system.system_identifier::text=$3 AND p.owner='worker' AND 'sha256:'||p.policy_sha256=f.baseline_digest
		  AND p.policy_document=f.baseline_policy AND NOT EXISTS (SELECT FROM fugue_storage_audit.native_policy_authority)`,
		scope.ClusterID, database, systemID).Scan(&raw, &digestFromProvider, &observed.FreezeEvidence,
		&observed.FrozenAt, &approval, &ownerPolicy, &workerUID, &enrolled)
	if err != nil {
		return StorageAuditPolicyBaseline{}, errors.Join(errors.New("could not observe the frozen storage audit worker policy"), err)
	}
	policy, err := storageaudit.ParsePolicy(raw)
	if err != nil {
		return StorageAuditPolicyBaseline{}, err
	}
	observed.Policy, err = json.Marshal(policy)
	if err != nil {
		return StorageAuditPolicyBaseline{}, err
	}
	observed.Verified, err = storageAuditPolicyBaselineVersion(observed.Policy)
	if err != nil || observed.Verified.Digest != digestFromProvider || !digestPattern.MatchString(observed.FreezeEvidence) ||
		!digestPattern.MatchString("sha256:"+approval) || !digestPattern.MatchString("sha256:"+ownerPolicy) ||
		!participationUID.MatchString(workerUID) || enrolled.IsZero() || observed.FrozenAt.Before(enrolled) {
		return StorageAuditPolicyBaseline{}, errors.New("frozen storage audit baseline or worker lineage is inconsistent")
	}
	observed.FrozenAt = observed.FrozenAt.UTC()
	signed, err := signedBytes("fugue.storage-audit.frozen-policy.v1", struct {
		Scope                                                                  Scope
		System, Database, Policy, Freeze, Activation, ActivationPolicy, Worker string
		FrozenAt, ActivatedAt                                                  time.Time
	}{scope, systemID, database, digestFromProvider, observed.FreezeEvidence, approval, ownerPolicy, workerUID, observed.FrozenAt, enrolled.UTC()})
	if err != nil {
		return StorageAuditPolicyBaseline{}, err
	}
	observed.ObservationDigest = digest(signed)
	return observed, nil
}

// ImportStorageAuditPolicyBaseline records the frozen policy and a separately
// signed technical revocation witness. Enrollment can only occur afterwards.
func (j *Journal) ImportStorageAuditPolicyBaseline(ctx context.Context, baseline Baseline, systemID, database, revokedEvidence string,
	source WitnessReader, witnessKeyID string, witnessKey TrustedWitness) (bool, error) {
	if j == nil || baseline.Scope != witnessKey.Scope || baseline.ResourceID != StorageAuditPolicyResource ||
		!digestPattern.MatchString(revokedEvidence) {
		return false, errors.New("storage audit baseline requires scoped principal revocation evidence")
	}
	stored, err := j.ReadResource(ctx, baseline.Scope)
	if err == nil {
		if stored.Baseline != baseline {
			return false, ErrBaselineConflict
		}
		_, err := readBaselineSnapshot(ctx, j.db, baseline.Scope, baseline.Verified)
		return false, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var snapshot []byte
	e, err := NewObservedExecutionJournal(j, source, map[string]TrustedWitness{witnessKeyID: witnessKey},
		func(ctx context.Context, signed ExecutionWitness) error {
			live, err := j.ReadFrozenStorageAuditPolicy(ctx, baseline.Scope, systemID, database)
			if err != nil {
				return err
			}
			if signed.Kind != BaselineWitness || signed.version() != live.Verified || signed.ObservationDigest != live.ObservationDigest ||
				signed.AuthorityDigest != revokedEvidence || live.FreezeEvidence == revokedEvidence || signed.ObservedAt < live.FrozenAt.Unix() {
				return errors.New("signed storage audit baseline differs from the frozen policy or revocation evidence")
			}
			snapshot = live.Policy
			return nil
		})
	if err != nil {
		return false, err
	}
	return e.anchorResource(ctx, "storage-audit-policy-observer", baseline, func() []byte { return snapshot })
}
