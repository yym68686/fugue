package productioncontrol

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"time"
)

const GlobalProducerCutoverResource = "producer/global-policy-cutover"

type DNSCutoverPhase string

const (
	DNSFreeze   DNSCutoverPhase = "freeze"
	DNSActivate DNSCutoverPhase = "activate"
)

var (
	cutoverReleaseID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$`)
	cutoverWriter    = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

type DNSCutoverPlan struct {
	Phase             DNSCutoverPhase `json:"phase"`
	Origin            string          `json:"origin"`
	ReleaseID         string          `json:"releaseId"`
	Version           Version         `json:"version"`
	CheckpointDigest  string          `json:"checkpointDigest"`
	ShadowPayload     json.RawMessage `json:"shadowPayload,omitempty"`
	FreezeIntent      string          `json:"freezeIntent,omitempty"`
	BaselineEvidence  string          `json:"baselineEvidence,omitempty"`
	ActivationWitness string          `json:"activationWitness,omitempty"`
	Writer            string          `json:"writer,omitempty"`
	WriterOID         uint32          `json:"writerOid,omitempty"`
}

func DecodeDNSCutoverPlan(raw []byte) (DNSCutoverPlan, error) {
	var p DNSCutoverPlan
	if len(raw) > MaxPayloadBytes || decodeControlJSON(raw, &p) != nil {
		return DNSCutoverPlan{}, errors.New("invalid DNS cutover plan")
	}
	return p, p.validate()
}

func (p DNSCutoverPlan) validate() error {
	origin, err := url.Parse(p.Origin)
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil || origin.Path != "" ||
		origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" || p.Origin != origin.String() ||
		!cutoverReleaseID.MatchString(p.ReleaseID) || !p.Version.valid() || !digestPattern.MatchString(p.CheckpointDigest) {
		return errors.New("DNS cutover plan lacks an exact serving predecessor")
	}
	switch p.Phase {
	case DNSFreeze:
		if len(p.ShadowPayload) == 0 || !json.Valid(p.ShadowPayload) || p.FreezeIntent != "" || p.BaselineEvidence != "" ||
			p.ActivationWitness != "" || p.Writer != "" || p.WriterOID != 0 {
			return errors.New("DNS freeze requires shadow evidence and excludes writer authorization")
		}
	case DNSActivate:
		if len(p.ShadowPayload) != 0 || !digestPattern.MatchString(p.FreezeIntent) || !digestPattern.MatchString(p.BaselineEvidence) ||
			!digestPattern.MatchString(p.ActivationWitness) || p.FreezeIntent == p.BaselineEvidence || p.FreezeIntent == p.ActivationWitness ||
			!cutoverWriter.MatchString(p.Writer) || p.WriterOID == 0 {
			return errors.New("DNS activation requires an exact frozen baseline, revocation witness and dedicated writer")
		}
	default:
		return errors.New("unknown DNS cutover phase")
	}
	return nil
}

func DraftDNSCutover(cluster, request string, p DNSCutoverPlan) (Submission, error) {
	if err := p.validate(); err != nil {
		return Submission{}, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return Submission{}, err
	}
	i, err := NewIntent(cluster, GlobalProducerCutoverResource, request, DNSCutover, p.Version.Generation,
		p.Version.Digest, digest(raw), p.Version.Digest, raw)
	return Submission{i, raw}, err
}

func inspectDNSCutover(s Submission) (DNSCutoverPlan, error) {
	p, err := DecodeDNSCutoverPlan(s.Payload)
	canonical, encodeErr := json.Marshal(p)
	if err != nil || encodeErr != nil || !bytes.Equal(canonical, s.Payload) || s.Intent.VerifyPayload(s.Payload) != nil ||
		s.Intent.ResourceID != GlobalProducerCutoverResource || s.Intent.Operation != DNSCutover ||
		s.Intent.Generation != p.Version.Generation || s.Intent.CurrentDigest != p.Version.Digest ||
		s.Intent.RollbackDigest != p.Version.Digest || s.Intent.TargetDigest != digest(s.Payload) {
		return DNSCutoverPlan{}, errors.New("invalid DNS cutover intent")
	}
	return p, nil
}

// ApplyDNSCutover rereads owner authorization at the provider write boundary.
// check reads live serving and observer evidence before taking database locks.
func (j *Journal) ApplyDNSCutover(ctx context.Context, cluster, actor, intentRef, approvalRef string, phase DNSCutoverPhase,
	check func(context.Context, DNSCutoverPlan) (time.Time, error)) (Recorded, error) {
	if j == nil || check == nil || !tokenPattern.MatchString(cluster) || !tokenPattern.MatchString(actor) ||
		!digestPattern.MatchString(intentRef) || !digestPattern.MatchString(approvalRef) {
		return Recorded{}, errors.New("invalid DNS cutover request")
	}
	i, raw, err := j.ReadIntent(ctx, intentRef)
	if err != nil {
		return Recorded{}, err
	}
	p, err := inspectDNSCutover(Submission{i, raw})
	if err != nil || i.ClusterID != cluster || p.Phase != phase {
		return Recorded{}, errors.New("DNS cutover request differs from the recorded plan")
	}
	prior := func(db queryRow) (bool, error) {
		var intent, approval string
		err := db.QueryRowContext(ctx, `SELECT intent_digest,approval_digest FROM fugue_control.dns_cutovers
			WHERE cluster_id=$1 AND phase=$2`, cluster, phase).Scan(&intent, &approval)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err == nil && (intent != intentRef || approval != approvalRef) {
			err = errors.New("DNS cutover phase already has a different recorded decision")
		}
		return err == nil, err
	}
	if done, err := prior(j.db); done || err != nil {
		return Recorded{Digest: intentRef}, err
	}
	if err := verifyRecordedApproval(ctx, j.db, j, i, intentRef, approvalRef); err != nil {
		return Recorded{}, err
	}
	validUntil, err := check(ctx, p)
	if err != nil {
		return Recorded{}, err
	}
	if !j.now().Before(validUntil) {
		return Recorded{}, errors.New("DNS cutover observation expired")
	}
	ctx, cancel := context.WithDeadline(ctx, validUntil)
	defer cancel()
	return j.record(ctx, intentRef, func(tx *sql.Tx) (bool, error) {
		if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
			return false, err
		}
		if err := lockResource(ctx, tx, Scope{cluster, GlobalProducerPolicyResource}); err != nil {
			return false, err
		}
		if done, err := prior(tx); done || err != nil {
			return false, err
		}
		if err := verifyRecordedApproval(ctx, tx, j, i, intentRef, approvalRef); err != nil {
			return false, err
		}
		var res sql.Result
		switch phase {
		case DNSFreeze:
			res, err = tx.ExecContext(ctx, `UPDATE fugue_native_global_producer_gate SET
				frozen=TRUE,baseline_release_id=$1,baseline_fence=$2,baseline_digest=$3,evidence_digest=$4
				WHERE singleton AND NOT frozen`, p.ReleaseID, p.Version.Generation, p.Version.Digest, intentRef)
		case DNSActivate:
			res, err = tx.ExecContext(ctx, `INSERT INTO fugue_native_global_producer_authority
				(cluster_id,writer_role,baseline_evidence_digest,activation_evidence_digest,writer_oid)
				SELECT $1,$2::text,$3,$4,$9::oid FROM fugue_native_global_producer_gate g
				WHERE g.singleton AND g.frozen AND g.baseline_release_id=$5 AND g.baseline_fence=$6 AND g.baseline_digest=$7
				  AND g.evidence_digest=$8
				  AND EXISTS (SELECT 1 FROM fugue_control.dns_cutovers c WHERE c.cluster_id=$1 AND c.phase='freeze' AND c.intent_digest=$8)
				  AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname::text=$2::text AND oid=$9::oid)`,
				cluster, p.Writer, p.BaselineEvidence, intentRef, p.ReleaseID, p.Version.Generation, p.Version.Digest, p.FreezeIntent, p.WriterOID)
		}
		if err != nil {
			return false, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return false, err
		}
		if n != 1 {
			return false, errors.New("DNS cutover provider predecessor or writer no longer matches approval")
		}
		if err := verifyRecordedApproval(ctx, tx, j, i, intentRef, approvalRef); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_control.dns_cutovers (cluster_id,resource_id,phase,intent_digest,approval_digest,actor)
			VALUES ($1,$2,$3,$4,$5,$6)`, cluster, GlobalProducerCutoverResource, phase, intentRef, approvalRef, actor)
		return err == nil, err
	})
}
