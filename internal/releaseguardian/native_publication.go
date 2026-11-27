package releaseguardian

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	nativeIntentAnnotation = "fugue.pro/native-intent-digest"
	nativeFenceAnnotation  = "fugue.pro/native-execution-fence"
)

func hasNativePublicationMarker(annotations map[string]string) bool {
	_, intent := annotations[nativeIntentAnnotation]
	_, fence := annotations[nativeFenceAnnotation]
	return intent || fence
}

// NativePublicationBoundary identifies everything the publication client may
// write, and the workload on which only its designated Guardian may act.
type NativePublicationBoundary struct {
	Scope                        productioncontrol.Scope
	Namespace                    string
	MonitorComponent             string
	ComponentLease               string
	DesiredObject                string
	RecordObject                 string
	ExecutionObject              string
	Predecessor, Candidate       DesiredRelease
	DesiredResourceVersion       string
	Workload                     declarativerelease.ResourceIdentity
	WorkloadUID, WorkloadVersion string
	ForwardOCI, RollbackOCI      string
	ArtifactReceiptDigest        string
	ForwardReceipt               declarativerelease.ArtifactReceipt
	RollbackReceipt              declarativerelease.ArtifactReceipt
}

// NativePublicationAdmission is a trusted provider-side admission dependency,
// not an intent field. It must check that every old writer is fenced (including
// already authorized/delayed work), the running Guardian enforces the cutover,
// this writer is uniquely authorized for this exact boundary, and the exact
// forward and rollback artifacts are verified and available independently of
// GitHub. The provider policy must stay enforced at request execution, not
// merely when checked.
// A Kubernetes resourceVersion, local flag, signature or marker alone cannot
// implement this contract.
type NativePublicationAdmission interface {
	VerifyNativePublication(context.Context, NativePublicationBoundary) error
}

type nativeExecutionJournal interface {
	ReadIntent(context.Context, string) (productioncontrol.Intent, []byte, error)
	Execution(context.Context, productioncontrol.Scope, string) (productioncontrol.Claim, *productioncontrol.ExecutionResult, error)
	ReserveExecution(context.Context, string, string, string) (productioncontrol.Claim, bool, error)
	VerifyOpenClaim(context.Context, productioncontrol.Claim) error
}

type NativePublisher struct {
	store     *KubeStore
	journal   nativeExecutionJournal
	admission NativePublicationAdmission
	writer    configMapPublicationWriter
	clusterID string
	artifacts *IndependentRegistry
}

func newNativePublisher(store *KubeStore, journal *productioncontrol.ExecutionJournal, admission NativePublicationAdmission, clusterID string, writer configMapPublicationWriter) (*NativePublisher, error) {
	if store == nil || store.client == nil || journal == nil || admission == nil || writer == nil || !componentPattern.MatchString(clusterID) {
		return nil, errors.New("native Guardian publisher requires its cluster, journal and provider admission")
	}
	return &NativePublisher{store: store, journal: journal, admission: admission, clusterID: clusterID, writer: writer}, nil
}

// NativePublication describes a submission, not a successful rollout. After a
// Claim exists, only observation may settle the execution. Submitted=false may
// include an unknown write outcome; it never means that it is safe to retry.
type NativePublication struct {
	Claim     productioncontrol.Claim
	Desired   DesiredRelease
	Submitted bool
}

func (p *NativePublisher) Publish(ctx context.Context, actor, intentDigest, approvalDigest string) (NativePublication, error) {
	if p == nil || p.store == nil || p.journal == nil || p.admission == nil || p.writer == nil {
		return NativePublication{}, errors.New("native Guardian publisher is not configured")
	}
	intent, payload, err := p.readReleaseIntent(ctx, intentDigest)
	if err != nil {
		return NativePublication{}, err
	}
	scope := productioncontrol.Scope{ClusterID: intent.ClusterID, ResourceID: intent.ResourceID}
	if claim, _, err := p.journal.Execution(ctx, scope, intentDigest); err == nil {
		if claim.ApprovalDigest != approvalDigest {
			return NativePublication{}, productioncontrol.ErrStaleFence
		}
		return NativePublication{Claim: claim}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return NativePublication{}, err
	}
	key := payload.ProposedRecord.Key()
	publication, err := p.store.planDesiredPublicationFor(ctx, key, payload.Files, true)
	if err != nil {
		return NativePublication{}, err
	}
	boundary, err := bindNativePublication(intent, payload, publication)
	if err != nil {
		return NativePublication{}, err
	}
	if err := p.verifyBoundary(ctx, boundary); err != nil {
		return NativePublication{}, err
	}
	if p.artifacts != nil {
		if err := p.artifacts.VerifyReleaseArtifacts(ctx, boundary); err != nil {
			return NativePublication{}, err
		}
		// A streaming image check can take longer than a reconciliation
		// interval. Do not reserve based on the preceding workload read.
		if err := p.verifyBoundary(ctx, boundary); err != nil {
			return NativePublication{}, err
		}
	}
	claim, created, err := p.journal.ReserveExecution(ctx, actor, intentDigest, approvalDigest)
	result := NativePublication{Claim: claim}
	if err != nil || !created {
		return result, err
	}
	publication.desired.Annotations = map[string]string{
		nativeIntentAnnotation: claim.IntentDigest,
		nativeFenceAnnotation:  strconv.FormatInt(claim.Fence, 10),
	}
	_, desired, err := p.store.commitDesiredPublication(ctx, key, publication, p.writer, func(ctx context.Context) error {
		if err := p.verifyBoundary(ctx, boundary); err != nil {
			return err
		}
		return p.journal.VerifyOpenClaim(ctx, claim)
	})
	if err != nil {
		return result, fmt.Errorf("native Guardian publication requires observation before another dispatch: %w", err)
	}
	result.Desired, result.Submitted = desired, true
	return result, nil
}

func (p *NativePublisher) readReleaseIntent(ctx context.Context, intentDigest string) (productioncontrol.Intent, nativeReleasePayload, error) {
	return readNativeReleaseIntent(ctx, p.journal.ReadIntent, p.clusterID, intentDigest)
}

func readNativeReleaseIntent(ctx context.Context, read func(context.Context, string) (productioncontrol.Intent, []byte, error), clusterID, intentDigest string) (productioncontrol.Intent, nativeReleasePayload, error) {
	intent, raw, err := read(ctx, intentDigest)
	if err != nil {
		return productioncontrol.Intent{}, nativeReleasePayload{}, err
	}
	actualDigest, err := intent.Digest()
	if err != nil || actualDigest != intentDigest || intent.Operation != productioncontrol.ComponentRelease ||
		intent.ClusterID != clusterID || intent.VerifyPayload(raw) != nil {
		return productioncontrol.Intent{}, nativeReleasePayload{}, errors.New("production intent is not a release for this Guardian cluster")
	}
	var payload nativeReleasePayload
	if err := decodeStrict(raw, &payload); err != nil {
		return productioncontrol.Intent{}, nativeReleasePayload{}, fmt.Errorf("decode native Guardian release payload: %w", err)
	}
	canonical, err := declarativerelease.CanonicalJSON(payload)
	if err != nil || !bytes.Equal(raw, canonical) || payload.ProposedRecord.Validate() != nil ||
		payload.ProposedRecord.RecordDigest != intent.TargetDigest || payload.ExpectedDesiredDigest != intent.CurrentDigest ||
		payload.ProposedRecord.LKGRecordDigest != intent.RollbackDigest || !digestPattern.MatchString(payload.RollbackMonitor) {
		return productioncontrol.Intent{}, nativeReleasePayload{}, errors.New("native Guardian payload is not canonical or does not match its intent")
	}
	return intent, payload, nil
}

func bindNativePublication(intent productioncontrol.Intent, payload nativeReleasePayload, publication desiredPublication) (NativePublicationBoundary, error) {
	preview, bundle := publication.preview, publication.bundle
	prewrite, workload := bundle.Prepared.Prewrite, preview.PrewriteWorkload
	if intent.Generation > math.MaxInt64-2 || intent.Generation != preview.CurrentDesired.Generation || intent.CurrentDigest != preview.CurrentDesired.RecordDigest ||
		intent.CurrentDigest != preview.CurrentRecordDigest || intent.RollbackDigest != preview.Record.LKGRecordDigest ||
		intent.RollbackDigest != intent.CurrentDigest || intent.TargetDigest != preview.Record.RecordDigest ||
		intent.TargetDigest == intent.CurrentDigest || intent.ResourceID != nativeKubernetesResourceID(workload) ||
		payload.Namespace != preview.Namespace || payload.ProposedRecord != preview.Record ||
		payload.ExpectedDesiredDigest != preview.CurrentDesired.RecordDigest || payload.RollbackManifest != preview.RollbackManifestDigest ||
		payload.RollbackMonitor != preview.RollbackMonitorDigest ||
		!allLayersHealthy(preview.Health) {
		return NativePublicationBoundary{}, errors.New("native Guardian publication no longer matches the approved target or verified predecessor")
	}
	// Until the journal can atomically reserve multiple resource fences, do
	// not allow a release to mutate shared objects or delegate a transition.
	if publication.monitorComponent != bundle.Release.ComponentID ||
		workload.Namespace != preview.Namespace || workload.APIVersion != "apps/v1" || workload.Kind != "Deployment" ||
		bundle.Release.Transition != nil || len(bundle.Release.RuntimeResourcesFromForward) != 0 ||
		bundle.Release.Workload.NodeParticipation != "" || prewrite.Participation != nil ||
		!prewrite.Present || prewrite.UID == "" || prewrite.ResourceVersion == "" || len(prewrite.Resources) != 1 ||
		prewrite.Resources[0].Identity != workload || !prewrite.Resources[0].Present ||
		prewrite.Resources[0].UID != prewrite.UID || prewrite.Resources[0].ResourceVersion != prewrite.ResourceVersion {
		return NativePublicationBoundary{}, errors.New("native Guardian publication requires one existing Deployment without delegated writes")
	}
	for index, raw := range [][]byte{bundle.Forward, bundle.LKG} {
		target := bundle.Prepared.Forward
		if index == 1 {
			target = bundle.Prepared.LKG
		}
		if err := VerifyNativeDeploymentImages(raw, workload, target); err != nil {
			return NativePublicationBoundary{}, err
		}
		set, err := declarativerelease.DecodeResourceSet(bytes.NewReader(raw))
		if err != nil || len(set.Items) != 1 {
			return NativePublicationBoundary{}, errors.New("native Guardian forward and rollback must both affect only the fenced workload")
		}
		validate := set.Primary
		if index == 1 {
			validate = set.PredecessorPrimary
		}
		if _, err := validate(bundle.Release.Workload); err != nil {
			return NativePublicationBoundary{}, errors.New("native Guardian manifest does not identify the fenced workload")
		}
	}
	return NativePublicationBoundary{
		Scope: productioncontrol.Scope{ClusterID: intent.ClusterID, ResourceID: intent.ResourceID}, Namespace: preview.Namespace,
		ComponentLease: bundle.Release.Concurrency, MonitorComponent: publication.monitorComponent,
		DesiredObject: publication.desired.Name, RecordObject: publication.record.Name, ExecutionObject: publication.execution.Name,
		Predecessor: preview.CurrentDesired, Candidate: preview.ProposedDesired, DesiredResourceVersion: preview.DesiredResourceVersion,
		Workload: workload, WorkloadUID: prewrite.UID, WorkloadVersion: prewrite.ResourceVersion,
		ForwardOCI: bundle.Prepared.Forward.ImageRef, RollbackOCI: bundle.Prepared.LKG.ImageRef, ArtifactReceiptDigest: bundle.Artifact.ReceiptDigest,
		ForwardReceipt: bundle.Artifact, RollbackReceipt: publication.rollbackReceipt,
	}, nil
}

func (p *NativePublisher) verifyBoundary(ctx context.Context, boundary NativePublicationBoundary) error {
	if err := p.admission.VerifyNativePublication(ctx, boundary); err != nil {
		return fmt.Errorf("native Guardian provider authority denied: %w", err)
	}
	live, err := p.store.client.AppsV1().Deployments(boundary.Workload.Namespace).Get(ctx, boundary.Workload.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read native Guardian prewrite Deployment: %w", err)
	}
	if string(live.UID) != boundary.WorkloadUID || live.ResourceVersion != boundary.WorkloadVersion {
		return productioncontrol.ErrResourceDrift
	}
	return nil
}
