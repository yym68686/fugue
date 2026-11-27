package productioncontrol

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	ParticipationTopologyResource = "topology/node-participation"
	ParticipationPolicySchema     = "fugue.automatic-node-participation/v1"
	MaxParticipationPolicyBytes   = 128 << 10
)

var (
	participationName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,200}$`)
	participationUID  = regexp.MustCompile(`^[a-zA-Z0-9-]{1,100}$`)
	participationHost = regexp.MustCompile(`^[a-z0-9.-]{1,253}$`)
)

type ParticipationPolicy struct {
	Schema     string                `json:"schema"`
	Generation int64                 `json:"generation"`
	Namespace  string                `json:"namespace"`
	Origin     string                `json:"origin"`
	Readers    ParticipationReaders  `json:"readers"`
	Members    []ParticipationMember `json:"members"`
}

type ParticipationReaders struct {
	Guardian     string `json:"guardian"`
	CanaryProber string `json:"canary-prober"`
}

type ParticipationMember struct {
	Node          string               `json:"node"`
	NodeUID       string               `json:"node_uid"`
	Group         string               `json:"group"`
	Component     string               `json:"component"`
	Front         string               `json:"front"`
	Workers       ParticipationWorkers `json:"workers"`
	Reason        string               `json:"reason"`
	AdoptHold     *bool                `json:"adopt_hold"`
	StableSeconds int                  `json:"stable_seconds"`
	Probes        []ParticipationProbe `json:"probes"`
}

type ParticipationWorkers struct {
	A string `json:"a"`
	B string `json:"b"`
}

type ParticipationProbe struct {
	Host string `json:"host"`
	Path string `json:"path"`
}

func DecodeParticipationPolicy(raw []byte) (ParticipationPolicy, error) {
	var p ParticipationPolicy
	if len(raw) == 0 || len(raw) > MaxParticipationPolicyBytes || decodeControlJSON(raw, &p) != nil ||
		p.Schema != ParticipationPolicySchema || p.Generation < 1 || len(p.Members) < 1 || len(p.Members) > 16 {
		return ParticipationPolicy{}, errors.New("invalid node participation topology")
	}
	origin, err := url.Parse(p.Origin)
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil ||
		origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" || origin.ForceQuery || strings.Contains(p.Origin, "#") {
		return ParticipationPolicy{}, errors.New("participation topology requires a credential-free HTTPS origin")
	}
	nodes, uids := map[string]bool{}, map[string]bool{}
	for _, m := range p.Members {
		for _, name := range []string{p.Namespace, p.Readers.Guardian, p.Readers.CanaryProber, m.Node, m.Group, m.Component, m.Front, m.Workers.A, m.Workers.B} {
			if !participationName.MatchString(name) {
				return ParticipationPolicy{}, errors.New("invalid participation resource identity")
			}
		}
		if nodes[m.Node] || uids[m.NodeUID] || !participationUID.MatchString(m.NodeUID) || m.AdoptHold == nil ||
			m.StableSeconds < 120 || m.StableSeconds > 3600 || len(m.Probes) < 2 || len(m.Probes) > 8 ||
			(m.Reason != "quota_exhausted" && m.Reason != "network_unavailable") {
			return ParticipationPolicy{}, errors.New("participation requires an exact, unique physical Node and bounded policy")
		}
		nodes[m.Node], uids[m.NodeUID] = true, true
		for _, probe := range m.Probes {
			if !participationHost.MatchString(probe.Host) || !strings.HasPrefix(probe.Path, "/") || strings.ContainsAny(probe.Path, "\r\n?#") {
				return ParticipationPolicy{}, errors.New("invalid participation route probe")
			}
		}
	}
	return p, nil
}

// DraftParticipationTopology binds the exact topology bytes and expected
// previous native version. A first import may retain the legacy policy's
// generation; subsequent revisions are strictly consecutive. This operation
// is not a grant to hold, readmit, or alter a Node.
func DraftParticipationTopology(cluster, request string, previous Version, raw []byte) (Submission, error) {
	p, err := DecodeParticipationPolicy(raw)
	if err != nil {
		return Submission{}, err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return Submission{}, err
	}
	initial := previous == (Version{})
	if initial {
		previous = Version{Generation: 1, Digest: participationGenesisDigest()}
	} else if !previous.valid() || previous.Digest == participationGenesisDigest() ||
		previous.Generation == math.MaxInt64 || p.Generation != previous.Generation+1 {
		return Submission{}, errors.New("participation topology must follow the exact previous native revision")
	}
	intent, err := NewIntent(cluster, ParticipationTopologyResource, request, ParticipationTopology, previous.Generation,
		previous.Digest, digest(payload), previous.Digest, payload)
	if err != nil {
		return Submission{}, err
	}
	return Submission{intent, payload}, nil
}

func participationGenesisDigest() string {
	return digest([]byte("fugue.participation-topology.genesis.v1"))
}

func validateParticipationIntent(i Intent, raw []byte) (ParticipationPolicy, error) {
	p, err := DecodeParticipationPolicy(raw)
	if err != nil {
		return ParticipationPolicy{}, err
	}
	expected, err := json.Marshal(p)
	if err != nil || !bytes.Equal(expected, raw) || i.Operation != ParticipationTopology || i.ResourceID != ParticipationTopologyResource ||
		i.VerifyPayload(raw) != nil || i.TargetDigest != i.PayloadDigest || i.CurrentDigest != i.RollbackDigest ||
		(i.CurrentDigest == participationGenesisDigest() && i.Generation != 1) ||
		(i.CurrentDigest != participationGenesisDigest() && (i.Generation == math.MaxInt64 || p.Generation != i.Generation+1)) {
		return ParticipationPolicy{}, errors.New("intent does not bind this exact participation topology")
	}
	return p, nil
}

type VerifiedParticipationTopology struct {
	Version        Version             `json:"version"`
	IntentDigest   string              `json:"intentDigest"`
	ApprovalDigest string              `json:"approvalDigest"`
	RecordedAt     time.Time           `json:"recordedAt"`
	Policy         ParticipationPolicy `json:"policy"`
}

// ActivateParticipationTopology changes only the append-only Fugue catalog.
// A second document with the same predecessor cannot replace the winner.
func (j *Journal) ActivateParticipationTopology(ctx context.Context, cluster, actor, intentDigest, approvalDigest string) (Recorded, error) {
	scope := Scope{cluster, ParticipationTopologyResource}
	if !scope.valid() || !tokenPattern.MatchString(actor) || !digestPattern.MatchString(intentDigest) || !digestPattern.MatchString(approvalDigest) {
		return Recorded{}, errors.New("invalid participation topology activation")
	}
	return j.record(ctx, intentDigest, func(tx *sql.Tx) (bool, error) {
		if err := lockResource(ctx, tx, scope); err != nil {
			return false, err
		}
		intent, raw, err := readIntent(ctx, tx, intentDigest)
		if err != nil {
			return false, err
		}
		p, err := validateParticipationIntent(intent, raw)
		if err != nil || intent.scope() != scope {
			return false, errors.New("topology does not belong to this cluster")
		}
		var existing string
		err = tx.QueryRowContext(ctx, `SELECT approval_digest FROM fugue_control.participation_topologies WHERE intent_digest=$1`, intentDigest).Scan(&existing)
		if err == nil {
			if existing != approvalDigest {
				return false, ErrResourceDrift
			}
			return false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		current := Version{Generation: 1, Digest: participationGenesisDigest()}
		err = tx.QueryRowContext(ctx, `SELECT generation, policy_digest FROM fugue_control.participation_topologies WHERE cluster_id=$1 ORDER BY generation DESC LIMIT 1`, cluster).
			Scan(&current.Generation, &current.Digest)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if current != (Version{intent.Generation, intent.CurrentDigest}) {
			return false, ErrResourceDrift
		}
		at := j.now().UTC().Truncate(time.Microsecond)
		if err := verifyRecordedApprovalAt(ctx, tx, j, intent, intentDigest, approvalDigest, at); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_control.participation_topologies
			(cluster_id, resource_id, generation, policy_digest, intent_digest, approval_digest, actor, recorded_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, cluster, scope.ResourceID, p.Generation, intent.TargetDigest, intentDigest, approvalDigest, actor, at)
		return err == nil, err
	})
}

// ReadParticipationTopology authenticates the stored policy at the moment it
// was recorded. Approval expiry limits activation, not the lifetime of an
// already installed topology. Changing trusted keys still requires an
// explicit policy/key transition; invalid persisted state fails closed.
func (j *Journal) ReadParticipationTopology(ctx context.Context, cluster string) (VerifiedParticipationTopology, error) {
	return j.readParticipationTopology(ctx, cluster, 0)
}

// ReadParticipationTopologyVersion retrieves an authenticated historic
// revision for review or for drafting a new rollback. It never moves the head.
func (j *Journal) ReadParticipationTopologyVersion(ctx context.Context, cluster string, generation int64) (VerifiedParticipationTopology, error) {
	if generation < 1 {
		return VerifiedParticipationTopology{}, errors.New("invalid participation topology generation")
	}
	return j.readParticipationTopology(ctx, cluster, generation)
}

func (j *Journal) readParticipationTopology(ctx context.Context, cluster string, generation int64) (VerifiedParticipationTopology, error) {
	var row VerifiedParticipationTopology
	if !tokenPattern.MatchString(cluster) {
		return row, errors.New("invalid participation topology cluster")
	}
	query := `SELECT generation, policy_digest, intent_digest, approval_digest, recorded_at
		FROM fugue_control.participation_topologies WHERE cluster_id=$1`
	args := []any{cluster}
	if generation == 0 {
		query += ` ORDER BY generation DESC LIMIT 1`
	} else {
		query += ` AND generation=$2`
		args = append(args, generation)
	}
	err := j.db.QueryRowContext(ctx, query, args...).
		Scan(&row.Version.Generation, &row.Version.Digest, &row.IntentDigest, &row.ApprovalDigest, &row.RecordedAt)
	if err != nil {
		return row, err
	}
	intent, raw, err := j.ReadIntent(ctx, row.IntentDigest)
	if err != nil {
		return VerifiedParticipationTopology{}, err
	}
	p, err := validateParticipationIntent(intent, raw)
	if err != nil || intent.ClusterID != cluster || row.Version != (Version{p.Generation, intent.TargetDigest}) ||
		row.RecordedAt.After(j.now()) {
		return VerifiedParticipationTopology{}, errors.New("stored participation topology differs from its binding")
	}
	if err := verifyRecordedApprovalAt(ctx, j.db, j, intent, row.IntentDigest, row.ApprovalDigest, row.RecordedAt); err != nil {
		return VerifiedParticipationTopology{}, err
	}
	row.Policy = p
	return row, nil
}
