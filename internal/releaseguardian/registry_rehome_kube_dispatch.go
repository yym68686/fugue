package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/participation"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type registryRehomeDispatcher struct {
	client  kubernetes.Interface
	gate    registryRehomeKubeFence
	scope   productioncontrol.Scope
	journal *productioncontrol.RegistryRehomeJournal
	reader  *productioncontrol.RegistryRehomeAdmissionReader
	ledger  *productioncontrol.Journal
	archive verifiedReleaseRecoveryArchive
}

type registryRehomeDispatchResult struct {
	Claim     productioncontrol.Claim
	Submitted bool
	Workload  RecoveryRecordRef
}

// newRegistryRehomeDispatcher wires a separate provider and journal identity.
// It installs no policies and must not be called by the normal release lane.
func newRegistryRehomeDispatcher(config *rest.Config,
	gate registryRehomeKubeFence, cluster string, ledger *productioncontrol.Journal, observer ed25519.PublicKey,
	raw []byte, archiveDigest string) (*registryRehomeDispatcher, error) {
	if config == nil || config.Insecure || config.ExecProvider != nil || config.AuthProvider != nil ||
		config.Transport != nil || config.WrapTransport != nil || config.Proxy != nil || config.Dial != nil ||
		config.Impersonate.UserName != "" || config.Impersonate.UID != "" || len(config.Impersonate.Groups) != 0 || len(config.Impersonate.Extra) != 0 ||
		gate.hook == nil || ledger == nil {
		return nil, errors.New("registry rehome requires an explicit provider fence, TLS and dedicated Kubernetes credentials")
	}
	cfg, err := participation.BindKubernetesConfig(config, gate.provider)
	if err != nil {
		return nil, err
	}
	archive, err := readReleaseRecoveryArchive(raw, archiveDigest)
	if err != nil {
		return nil, err
	}
	id := recoveryWorkloadKey(archive.forward.workload).resource
	if id.APIVersion != "apps/v1" || id.Kind != "Deployment" || id.Namespace != gate.cutover.Namespace ||
		id.Name != gate.cutover.Deployment || archive.report.Pair.Component != gate.cutover.Key.Component ||
		gate.cutover.MonitorComponent != archive.report.Pair.Component {
		return nil, errors.New("sealed registry rehome archive differs from the enrolled workload")
	}
	cfg.NextProtos = []string{"http/1.1"}
	tls, err := rest.TLSConfigFor(cfg)
	if err != nil {
		return nil, err
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport, err := rest.HTTPWrappersForConfig(cfg, &http.Transport{
		Protocols: protocols, TLSClientConfig: tls, DisableKeepAlives: true,
	})
	if err != nil {
		return nil, err
	}
	// New connections only: no HTTP/2 or reused-connection retries and
	// no forwarding credentials or a pending PATCH to another endpoint.
	client, err := kubernetes.NewForConfigAndClient(cfg, &http.Client{
		Transport: transport, Timeout: 8 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	})
	if err != nil {
		return nil, err
	}
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: nativeKubernetesResourceID(id)}
	e := &registryRehomeDispatcher{client: client, gate: gate, scope: scope, ledger: ledger, archive: archive}
	e.reader, err = productioncontrol.NewRegistryRehomeAdmissionReader(ledger, scope, observer)
	if err != nil {
		return nil, err
	}
	e.journal, err = productioncontrol.NewRegistryRehomeJournal(ledger, scope, observer, func(ctx context.Context, a productioncontrol.RegistryRehomeAdmission, s productioncontrol.Submission) error {
		_, policy, write, err := e.preflight(ctx, s)
		if err != nil {
			return err
		}
		if policy != a.PolicyScope || write != a.ProviderWrite {
			return errors.New("signed registry rehome evidence differs from the enrolled provider request")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *registryRehomeDispatcher) preflight(ctx context.Context, s productioncontrol.Submission) (rehomeDeploymentCAS, string, string, error) {
	fail := func(err error) (rehomeDeploymentCAS, string, string, error) {
		return rehomeDeploymentCAS{}, "", "", err
	}
	review, err := InspectRegistryRehomeSubmission(s)
	if err != nil || s.Intent.ClusterID != e.scope.ClusterID || s.Intent.ResourceID != e.scope.ResourceID ||
		review.Source.Key != e.gate.cutover.Key || review.Source.ArchiveDigest != e.archive.report.ArchiveDigest {
		return fail(errors.Join(errors.New("intent does not bind the enrolled registry rehome source"), err))
	}
	if _, _, err := deriveRegistryRehomeRecovery(e.archive, review); err != nil {
		return fail(err)
	}
	auth, err := e.client.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil || auth.Status.UserInfo.Username != e.gate.scope.Writer {
		return fail(errors.Join(errors.New("Kubernetes did not authenticate the dedicated registry rehome writer"), err))
	}
	ns, err := e.client.CoreV1().Namespaces().Get(ctx, e.gate.cutover.Namespace, metav1.GetOptions{})
	if err != nil || ns.DeletionTimestamp != nil || string(ns.UID) != e.gate.provider.NamespaceUID {
		return fail(errors.Join(errors.New("registry rehome namespace does not match the enrolled provider"), err))
	}
	if err := e.gate.verify(ctx, e.client); err != nil {
		return fail(err)
	}
	current, err := observeRegistryRehomeSource(ctx, e.client, e.archive, review.Source.Key.Group)
	if err != nil || !reflect.DeepEqual(current, review.Source) {
		return fail(errors.Join(errors.New("live release no longer matches the signed rehome source"), err))
	}
	id := review.Draft.Workload
	live, err := e.client.AppsV1().Deployments(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil {
		return fail(err)
	}
	cas, err := prepareRegistryRehomeDeploymentCAS(review, live)
	if err != nil {
		return fail(err)
	}
	policy, write, err := e.gate.digests(cas)
	if err != nil {
		return fail(err)
	}
	return cas, policy, write, nil
}

func (e *registryRehomeDispatcher) dispatch(ctx context.Context, actor, intentDigest, approvalDigest string, evidence []byte) (registryRehomeDispatchResult, error) {
	if e == nil {
		return registryRehomeDispatchResult{}, errors.New("registry rehome dispatcher is not configured")
	}
	// An existing claim is always inspection-only, even if the attestation
	// has since expired or the workload no longer matches the preimage.
	if found, _, err := e.ledger.ReadExecution(ctx, e.scope, intentDigest); err == nil {
		if found.ApprovalDigest != approvalDigest {
			return registryRehomeDispatchResult{}, productioncontrol.ErrStaleFence
		}
		return registryRehomeDispatchResult{Claim: found}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return registryRehomeDispatchResult{}, err
	}
	claim, created, err := e.journal.Reserve(ctx, actor, intentDigest, approvalDigest, evidence)
	result := registryRehomeDispatchResult{Claim: claim}
	if err != nil || !created {
		return result, err
	}
	// Burn the claim even if preflight fails or the worker restarts. Neither
	// a definitive rejection nor an uncertain response advances the LKG.
	intent, payload, err := e.ledger.ReadIntent(ctx, intentDigest)
	if err != nil {
		return result, err
	}
	cas, policy, write, err := e.preflight(ctx, productioncontrol.Submission{Intent: intent, Payload: payload})
	if err != nil {
		return result, fmt.Errorf("registry rehome stopped after reservation: %w", err)
	}
	if err := e.reader.VerifyPendingWrite(ctx, claim, policy, write); err != nil {
		return result, err
	}
	live, err := writeRegistryRehomeDeploymentOnce(ctx, e.client.AppsV1().RESTClient(), cas)
	if err != nil {
		return result, err
	}
	result.Submitted, result.Workload = true, recoveryRef(live)
	return result, nil
}
