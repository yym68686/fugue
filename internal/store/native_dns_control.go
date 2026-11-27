package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/dnscontrol"
)

const nativeDNSActor = "guardian-native-dns"

// NativeDNSControl is a narrow store adapter for the dedicated Guardian
// process. It shares the platform publication transaction and signature
// checks, but does not initialize the general Store, run bootstrap/repair,
// expose generic publication methods, or accept caller-supplied release
// options. Both the journal and provider must be in the same database.
type NativeDNSControl struct {
	store   *Store
	journal *productioncontrol.Journal
	reader  *dnscontrol.Reader
	cluster string
}

// NewNativeDNSControl uses an already configured TLS PostgreSQL pool for the
// dedicated native writer. It never provisions a role, changes the gate or
// uses the schema owner's credentials. The caller owns the pool lifetime.
func NewNativeDNSControl(db *sql.DB, cluster string, owners productioncontrol.Approvers, signing bundleauth.Keyring, reader *dnscontrol.Reader) (*NativeDNSControl, error) {
	if db == nil || reader == nil || cluster == "" || len(owners) == 0 || signing.PrimaryKey == "" || signing.PrimaryKeyID == "" {
		return nil, errors.New("native DNS control requires its database, cluster, owner keys, artifact verifier and read-only observer")
	}
	for _, owner := range owners {
		if owner.ClusterID != cluster {
			return nil, errors.New("native DNS control configured with an owner of another cluster")
		}
	}
	j, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, dbReady: true}
	s.ConfigurePlatformArtifactSigning(signing)
	if err := s.ConfigureCompilerInputArchiveFromEnv(); err != nil {
		return nil, err
	}
	return &NativeDNSControl{store: s, journal: j, reader: reader, cluster: cluster}, nil
}

type NativeDNSDispatchResult struct {
	Claim             productioncontrol.Claim `json:"claim"`
	Replayed          bool                    `json:"replayed"`
	SelectedReleaseID string                  `json:"selectedReleaseId,omitempty"`
	SelectedFence     int64                   `json:"selectedFence,omitempty"`
	ExecutionEvidence bool                    `json:"executionEvidence"`
}

func (c *NativeDNSControl) isActive(ctx context.Context) error {
	var ready bool
	err := c.store.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_native_global_producer_gate g
JOIN fugue_native_global_producer_authority a ON a.singleton=g.singleton
WHERE g.singleton AND g.frozen AND a.cluster_id=$1 AND a.writer_role=session_user
AND a.writer_oid=(SELECT oid FROM pg_roles WHERE rolname=session_user))`, c.cluster).Scan(&ready)
	if err != nil || !ready {
		return errors.New("native DNS provider authority has not been activated for this writer")
	}
	return nil
}

// Dispatch creates at most one durable claim. A duplicate, even following an
// unknown provider commit, can only return the already persisted claim; it
// never attempts a second publication. The result reports a policy selection,
// not serving convergence or sufficient evidence to settle the journal.
func (c *NativeDNSControl) Dispatch(ctx context.Context, intentDigest, approvalDigest string) (NativeDNSDispatchResult, error) {
	if c == nil || c.store == nil || c.journal == nil || c.reader == nil {
		return NativeDNSDispatchResult{}, ErrInvalidInput
	}
	i, payload, err := c.journal.ReadIntent(ctx, intentDigest)
	if err != nil || i.ClusterID != c.cluster {
		return NativeDNSDispatchResult{}, errors.New("native DNS intent unavailable or belongs to another cluster")
	}
	sub := productioncontrol.Submission{Intent: i, Payload: payload}
	approved, err := dnscontrol.Inspect(sub)
	if err != nil {
		return NativeDNSDispatchResult{}, err
	}
	if claim, _, err := c.journal.ReadExecution(ctx, dnscontrol.Scope(c.cluster), intentDigest); err == nil {
		if claim.ApprovalDigest != approvalDigest {
			return NativeDNSDispatchResult{}, productioncontrol.ErrStaleFence
		}
		return NativeDNSDispatchResult{Claim: claim, Replayed: true}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return NativeDNSDispatchResult{}, err
	}
	if err := c.isActive(ctx); err != nil {
		return NativeDNSDispatchResult{}, err
	}
	if err := dnscontrol.RevalidateRollback(ctx, c.journal, c.cluster, approved); err != nil {
		return NativeDNSDispatchResult{}, err
	}
	live, err := c.reader.Revalidate(ctx, sub, time.Now)
	if err != nil {
		return NativeDNSDispatchResult{}, err
	}
	// The existing compiler admission also requires an independently prepared
	// exact artifact graph and retention pins, and rechecks them in the write
	// transaction. No alternate or less strict native publication path exists.
	return c.dispatchPrepared(ctx, intentDigest, approvalDigest, sub, approved, live)
}

func (c *NativeDNSControl) dispatchPrepared(ctx context.Context, intentDigest, approvalDigest string, sub productioncontrol.Submission, approved, live dnscontrol.Payload) (out NativeDNSDispatchResult, returnedErr error) {
	if !dnscontrol.EqualChange(approved, live) || !platformproducer.ValidDigest(live.ShadowDigest) {
		return out, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, platformPublicationTransactionTimeout)
	defer cancel()
	ctx, trace := startPlatformMutationTrace(ctx, "native-dns-policy")
	defer func() { returnedErr = trace.finish(returnedErr) }()
	key, err := producerReconfigurationKey(model.PlatformArtifact{ID: approved.NextProducer.ID, ContentHash: approved.NextProducer.Digest}, approved.Precondition)
	if err != nil {
		return out, err
	}
	req := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", ProducerReconfiguration: &approved.Precondition,
		IdempotencyKey: key, Reason: "guardian native DNS policy " + intentDigest}
	trace.advance("prepare-input-admission")
	proof, pins, err := c.store.prepareCompilerInputPublication(ctx, approved.NextProducer.ID, "", nil, req)
	if err != nil {
		return out, err
	}
	claim, created, err := c.journal.ReserveExecution(ctx, nativeDNSActor, intentDigest, approvalDigest)
	if err != nil {
		return out, err
	}
	out.Claim, out.Replayed = claim, !created
	if !created {
		return out, nil
	}
	principal := model.Principal{ActorType: model.ActorTypeSystem, ActorID: nativeDNSActor}
	_, release, _, _, err := c.store.pgReleasePreparedPlatformArtifactWithAdmission(ctx, approved.NextProducer.ID, req, principal, nil, proof, pins, trace, func(ctx context.Context, tx *sql.Tx) error {
		locked, err := c.journal.LockVerifiedGlobalProducerClaim(ctx, tx, claim)
		if err != nil {
			return err
		}
		if locked.Intent != sub.Intent || !bytes.Equal(locked.Payload, sub.Payload) {
			return productioncontrol.ErrStaleFence
		}
		p, err := dnscontrol.Inspect(locked)
		if err != nil || !dnscontrol.EqualChange(p, approved) || p.ShadowDigest != approved.ShadowDigest {
			return productioncontrol.ErrStaleFence
		}
		// This check runs under the producer/target scope locks. The store's
		// existing signed reconfiguration validation still runs before and
		// after the mutation; this additionally binds the full exact journal
		// payload, including its LKG ID and immutable source references.
		if err := c.store.verifyNativeDNSSources(ctx, tx, p); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_native_global_producer_receipts
(intent_digest,approval_digest,claim_fence,cluster_id,idempotency_key,live_shadow_digest) VALUES ($1,$2,$3,$4,$5,$6)`,
			claim.IntentDigest, claim.ApprovalDigest, claim.Fence, claim.ClusterID, key, live.ShadowDigest)
		return mapDBErr(err)
	})
	if err != nil {
		// Claim stays pending even when a caller loses the commit response.
		// No caller can burn another claim without independent settlement.
		return out, err
	}
	out.SelectedReleaseID, out.SelectedFence = release.ID, release.FencingToken
	return out, nil
}

func (s *Store) verifyNativeDNSSources(ctx context.Context, tx *sql.Tx, p dnscontrol.Payload) error {
	var source dnscontrol.Sources
	for _, binding := range []struct {
		id string
		a  *model.PlatformArtifact
	}{
		{p.NextProducer.ID, &source.NextProducer}, {p.PreviousProducer.ID, &source.Producer.Artifact},
		{p.Precondition.ServingFull.ArtifactID, &source.Full.Artifact}, {p.Static.ID, &source.Static},
		{p.PreviousDNS.ID, &source.PreviousDNS}, {p.NextDNS.ID, &source.NextDNS},
	} {
		a, err := pgGetPlatformArtifactForUpdate(ctx, tx, binding.id, true)
		if err != nil || a.ID != binding.id {
			return ErrConflict
		}
		*binding.a = a
	}
	var err error
	source.Producer.Release, err = pgGetPlatformArtifactRelease(ctx, tx, p.Precondition.PreviousPolicy.ReleaseID, false)
	if err != nil {
		return err
	}
	source.Full.Release, err = pgGetPlatformArtifactRelease(ctx, tx, p.Precondition.ServingFull.ReleaseID, false)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	lkg, err := s.pgGetVerifiedPlatformLKGForUpdate(ctx, tx, model.PlatformArtifactKindReleaseSet, "global", now)
	if err != nil || lkg == nil || lkg.ID != p.LKGID {
		return ErrConflict
	}
	source.LKG = *lkg
	live, _, err := dnscontrol.ValidateSources(source, now)
	if err != nil {
		return err
	}
	live.Origin, live.ShadowDigest = p.Origin, p.ShadowDigest
	live.Reverses = p.Reverses // checked under the journal fence and by the receipt trigger
	if !dnscontrol.EqualChange(p, live) {
		return ErrConflict
	}
	return nil
}
