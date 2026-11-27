package main

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"github.com/jackc/pgx/v5/stdlib"
)

type nodeWorkerDispatchSettings struct {
	JournalFile       string                             `json:"journalFile"`
	Cutover           releaseguardian.FrontWorkerCutover `json:"cutover"`
	ObserverPublicKey []byte                             `json:"observerPublicKey"`
}

type nativeNodeWorkerAuthorization struct {
	journal     *productioncontrol.Journal
	gate        *releaseguardian.FrontWorkerProviderGate
	plan        productioncontrol.FrontWorkerShadow
	claim       productioncontrol.Claim
	recoverOnly bool
}

const nodeWorkerDispatchUsage = "usage: fugue-release-guardian node-worker-dispatch CONFIG INTENT_DIGEST APPROVAL_DIGEST ATTESTATION [--recover]"

func runNodeWorkerDispatch(parent context.Context, args []string, out io.Writer) error {
	recoverOnly := len(args) == 5 && args[4] == "--recover"
	if !recoverOnly && len(args) != 4 || out == nil || !exactSHA256Digest(args[1]) || !exactSHA256Digest(args[2]) {
		return errors.New(nodeWorkerDispatchUsage)
	}
	podUID := os.Getenv("FUGUE_RELEASE_GUARDIAN_POD_UID")
	if podUID == "" {
		return errors.New("native node Worker dispatcher requires its Guardian pod UID")
	}
	ctx, cancel := context.WithTimeout(parent, 16*time.Minute)
	defer cancel()
	raw, err := readOfflineDispatchInput(args[0], 16<<10, false)
	if err != nil {
		return err
	}
	var settings nodeWorkerDispatchSettings
	if err := decodeStrictJSON(raw, &settings); err != nil || len(settings.ObserverPublicKey) != ed25519.PublicKeySize {
		return errors.New("invalid native Front/Worker run configuration")
	}
	secret, err := readOfflineDispatchInput(settings.JournalFile, 32<<10, true)
	if err != nil {
		return err
	}
	clear(secret)
	cfg, owners, cluster, err := productioncontrol.LoadJournalEndpoint(settings.JournalFile)
	if err != nil {
		return err
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	db.SetMaxOpenConns(2)
	journal, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	scope := productioncontrol.Scope{ClusterID: cluster,
		ResourceID: productioncontrol.FrontWorkerResource(settings.Cutover.Namespace, settings.Cutover.Group)}
	attestation, err := readOfflineDispatchInput(args[3], releaseguardian.MaxCutoverAttestationBytes, false)
	if err != nil {
		return err
	}
	kube, err := loadKubeConfig()
	if err != nil {
		return err
	}
	targets, store, front, err := loadNodeWorkerAuthority(kube, kube, settings.Cutover.Group)
	if err != nil {
		return err
	}
	if front.config.Namespace != settings.Cutover.Namespace {
		return errors.New("native Front/Worker Kubernetes namespace differs from the configured cutover")
	}
	gate, err := releaseguardian.NewFrontWorkerProviderGate(front.client, settings.Cutover, journal, attestation, settings.ObserverPublicKey)
	if err != nil {
		return err
	}
	claim, terminal, err := journal.ReadExecution(ctx, scope, args[1])
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	emit := json.NewEncoder(out)
	if claim.Fence > 0 {
		if claim.ApprovalDigest != args[2] {
			return productioncontrol.ErrStaleFence
		}
		if !recoverOnly || terminal != nil {
			return emit.Encode(struct {
				Claim    productioncontrol.Claim            `json:"claim"`
				Terminal *productioncontrol.ExecutionResult `json:"terminal,omitempty"`
			}{claim, terminal})
		}
		intent, payload, err := journal.ReadIntent(ctx, args[1])
		if err != nil {
			return err
		}
		plan, err := productioncontrol.InspectFrontWorker(productioncontrol.Submission{Intent: intent, Payload: payload})
		if err != nil {
			return err
		}
		front.nodeWorkerAuthorization = &nativeNodeWorkerAuthorization{journal, gate, plan, claim, true}
		return front.recoverNodeWorker(ctx, store, emit)
	}
	if recoverOnly {
		return errors.New("there is no durable Front/Worker claim to recover")
	}
	approved, err := journal.ReadApprovedFrontWorker(ctx, cluster, args[1], args[2])
	if err != nil {
		return err
	}
	if err := gate.Verify(ctx, scope, args[1], args[2], approved); err != nil {
		return err
	}
	component, err := store.FindNodeWorkerComponent(ctx, targets, approved.Journal.Recipient.Group)
	if err != nil {
		return err
	}
	fresh, err := front.shadowNodeWorker(ctx, store, component, approved.Journal, false,
		func(ctx context.Context, observer *frontAuthorityActivator, observed nodeWorkerPreflight) error {
			_, err := observer.observeNodeWorkerIngress(ctx, approved.Journal, observed)
			return err
		})
	if err != nil {
		return err
	}
	if err := productioncontrol.MatchFreshFrontWorkerShadow(approved, fresh); err != nil {
		return err
	}
	claim, created, err := journal.ReserveExecution(ctx, "front-worker-guardian", args[1], args[2])
	if err != nil {
		return err
	}
	if !created {
		return emit.Encode(claim)
	}
	if claim.Scope != scope {
		return productioncontrol.ErrStaleFence
	}
	front.nodeWorkerAuthorization = &nativeNodeWorkerAuthorization{journal, gate, approved, claim, false}
	if err := emit.Encode(claim); err != nil {
		return err
	}
	if _, err := front.claimNodeWorker(ctx, store, component, podUID, approved.Journal); err != nil {
		return err
	}
	preflight, err := front.preflightNodeWorker(ctx, store, component, approved.Journal)
	if err != nil {
		return err
	}
	report, err := front.watchNodeWorker(ctx, store, approved.Journal, preflight, emit)
	if err != nil {
		return err
	}
	if err := emit.Encode(report); err != nil {
		return err
	}
	front.nodeWorkerAuthorization.recoverOnly = true
	return front.recoverNodeWorker(ctx, store, emit)
}

func (a *nativeNodeWorkerAuthorization) verifyForward(ctx context.Context, j edgegroupfront.NodeWorkerJournal) error {
	if a.recoverOnly || a.plan.Journal != j {
		return errors.New("forward node Worker write is outside the current native execution")
	}
	if err := a.gate.Verify(ctx, a.claim.Scope, a.claim.IntentDigest, a.claim.ApprovalDigest, a.plan); err != nil {
		return err
	}
	return a.journal.VerifyOpenClaim(ctx, a.claim)
}

func (a *frontAuthorityActivator) checkNodeWorkerReconcileAuthority(ctx context.Context, fence releaseguardian.NodeWorkerFence) error {
	auth := a.nodeWorkerAuthorization
	if auth == nil {
		if fence.Claim != (productioncontrol.Claim{}) {
			return errors.New("native node Worker fence requires the dedicated production journal reconciler")
		}
		return nil
	}
	if auth.claim != fence.Claim || auth.plan.Journal != fence.Journal {
		return errors.New("node Worker provider fence does not match the durable execution")
	}
	if err := auth.gate.VerifyRecovery(ctx, fence.Claim, auth.plan); err != nil {
		return err
	}
	claim, terminal, err := auth.journal.ReadExecution(ctx, fence.Claim.Scope, fence.Claim.IntentDigest)
	if err != nil || claim != fence.Claim || terminal != nil {
		return errors.Join(productioncontrol.ErrStaleFence, err)
	}
	state, err := auth.journal.ReadResource(ctx, fence.Claim.Scope)
	if err != nil || state.Pending != fence.Claim.IntentDigest || state.LastFence != fence.Claim.Fence {
		return errors.Join(productioncontrol.ErrStaleFence, err)
	}
	return nil
}

func (a *nativeNodeWorkerAuthorization) readFence(ctx context.Context, store *releaseguardian.AuthorityStore) (releaseguardian.NodeWorkerFence, error) {
	fence, found, err := store.LoadNodeWorker(ctx, a.plan.Journal.Recipient.Group)
	if err != nil || !found || fence.Claim != a.claim || fence.Journal != a.plan.Journal {
		return releaseguardian.NodeWorkerFence{}, errors.Join(errors.New("native Front/Worker has no matching Kubernetes execution fence"), err)
	}
	return fence, nil
}

func (a *nativeNodeWorkerAuthorization) arm(ctx context.Context, front *frontAuthorityActivator, store *releaseguardian.AuthorityStore, arm releaseguardian.NodeWorkerArm) error {
	fence, err := a.readFence(ctx, store)
	if err != nil {
		return err
	}
	if err := a.verifyForward(ctx, fence.Journal); err != nil {
		return err
	}
	return store.RecordNativeNodeWorkerArm(ctx, fence, arm, front.now().UTC())
}

func (a *nativeNodeWorkerAuthorization) witness(ctx context.Context, front *frontAuthorityActivator, store *releaseguardian.AuthorityStore,
	arm releaseguardian.NodeWorkerArm, witness releaseguardian.NodeWorkerWitness, state edgegroupfront.ActivationState) error {
	fence, err := a.readFence(ctx, store)
	if err != nil {
		return err
	}
	if err := a.verifyForward(ctx, fence.Journal); err != nil {
		return err
	}
	return store.RecordNativeNodeWorkerWitness(ctx, fence, arm, witness, state, front.now().UTC())
}

func (a *frontAuthorityActivator) recoverNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore, emit *json.Encoder) error {
	auth := a.nodeWorkerAuthorization
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		fence, found, err := store.LoadNodeWorker(ctx, auth.plan.Journal.Recipient.Group)
		if err != nil {
			return err
		}
		if !found {
			result, available, err := store.ReadNodeWorkerResult(ctx, auth.plan.Journal)
			if err != nil || !available || !result.MatchesExecution(auth.claim) {
				return errors.Join(errors.New("Front/Worker claim needs independent provider readback"), err)
			}
			return emit.Encode(result)
		}
		if fence.Claim != auth.claim || fence.Journal != auth.plan.Journal {
			return productioncontrol.ErrStaleFence
		}
		if _, _, err := a.reconcileNodeWorker(ctx, store); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
