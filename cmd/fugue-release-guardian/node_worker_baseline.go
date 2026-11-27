package main

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"fugue/internal/releaseguardian/participation"
	"github.com/jackc/pgx/v5/stdlib"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type nodeWorkerBaselineSettings struct {
	nodeWorkerSettlementSettings
	Kubernetes productioncontrol.ParticipationKubernetesBinding `json:"kubernetes"`
	Members    []nodeWorkerBaselineMember                       `json:"members"`
	Evidence   nodeWorkerBaselineEvidence                       `json:"evidence"`
}

type nodeWorkerBaselineMember struct {
	Node         string `json:"node"`
	NodeUID      string `json:"nodeUid"`
	Front        string `json:"frontDaemonSet"`
	WorkerA      string `json:"workerDaemonSetA"`
	WorkerB      string `json:"workerDaemonSetB"`
	ServiceUID   string `json:"serviceUid"`
	ProbeUID     string `json:"probeUid,omitempty"`
	CanaryHost   string `json:"canaryHost"`
	CanaryPath   string `json:"canaryPath"`
	CanaryDigest string `json:"canaryDigest"`
}

type nodeWorkerBaselineEvidence struct {
	EndpointAdmission string `json:"endpointAdmissionDigest"`
	LegacyRevocation  string `json:"legacyRevocationDigest"`
	LegacyDrain       string `json:"legacyRequestDrainDigest"`
}

type nodeWorkerBaselineObservation struct {
	Member    nodeWorkerBaselineMember       `json:"member"`
	Recipient edgegroupfront.NativeRecipient `json:"recipient"`
	Process   string                         `json:"workerProcessDigest"`
	Serving   nodeWorkerTerminalServing      `json:"serving"`
}

type nodeWorkerBaselineCheckpoint struct {
	Scope        productioncontrol.Scope                          `json:"scope"`
	Verified     productioncontrol.Version                        `json:"verified"`
	Kubernetes   productioncontrol.ParticipationKubernetesBinding `json:"kubernetes"`
	Serving      []nodeWorkerBaselineObservation                  `json:"serving"`
	Boundary     releaseguardian.FrontWorkerBaselineBoundary      `json:"boundary"`
	Evidence     nodeWorkerBaselineEvidence                       `json:"evidence"`
	WitnessDraft productioncontrol.ExecutionWitness               `json:"witnessDraft"`
	snapshot     []byte
}

func runNodeWorkerBaseline(parent context.Context, persist bool, args []string, out io.Writer) error {
	if (!persist && len(args) != 1) || (persist && (len(args) != 2 || !exactSHA256Digest(args[1]))) {
		return errors.New("usage: fugue-release-guardian node-worker-baseline-checkpoint CONFIG or node-worker-baseline-import CONFIG WITNESS_DIGEST")
	}
	raw, err := readOfflineDispatchInput(args[0], 64<<10, false)
	if err != nil {
		return err
	}
	var settings nodeWorkerBaselineSettings
	if err := decodeStrictJSON(raw, &settings); err != nil {
		return err
	}
	if err := settings.validate(); err != nil {
		return err
	}
	slices.SortFunc(settings.Members, func(a, b nodeWorkerBaselineMember) int { return strings.Compare(a.Node, b.Node) })
	cfg, owners, cluster, err := productioncontrol.LoadJournalEndpoint(settings.JournalFile)
	if err != nil {
		return err
	}
	if !persist {
		if cfg.RuntimeParams == nil {
			cfg.RuntimeParams = map[string]string{}
		}
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	db.SetMaxOpenConns(2)
	journal, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: productioncontrol.FrontWorkerResource(settings.Cutover.Namespace, settings.Cutover.Group)}
	emit := json.NewEncoder(out).Encode
	state, err := journal.ReadResource(ctx, scope)
	if err == nil {
		if persist && state.EvidenceDigest == args[1] {
			if _, err := journal.ReadFrontWorkerBaselineSnapshot(ctx, scope); err != nil {
				return err
			}
			return emit(productioncontrol.Recorded{Digest: args[1]})
		}
		return productioncontrol.ErrBaselineConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	client, executor, err := newBaselineKubernetesClient(settings)
	if err != nil {
		return err
	}
	observe := func(ctx context.Context) (nodeWorkerBaselineCheckpoint, error) {
		return observeNodeWorkerBaseline(ctx, client, executor, scope, settings)
	}
	if !persist {
		checkpoint, err := observe(ctx)
		if err != nil {
			return err
		}
		return emit(checkpoint)
	}
	if !filepath.IsAbs(settings.WitnessDirectory) || len(settings.ObserverPublicKey) != ed25519.PublicKeySize {
		return errors.New("Front/Worker baseline requires independent witness storage and a trusted observer key")
	}
	root, err := os.OpenRoot(settings.WitnessDirectory)
	if err != nil {
		return err
	}
	defer root.Close()
	source := nodeWorkerWitnessDirectory{root: root}
	witnessRaw, err := source.ReadExecutionWitness(ctx, args[1])
	if err != nil {
		return err
	}
	var witness productioncontrol.ExecutionWitness
	if err := decodeStrictJSON(witnessRaw, &witness); err != nil {
		return err
	}
	var verifiedSnapshot []byte
	execution, err := productioncontrol.NewObservedExecutionJournal(journal, source,
		map[string]productioncontrol.TrustedWitness{"provider-observer": {Scope: scope, PublicKey: settings.ObserverPublicKey}},
		func(ctx context.Context, signed productioncontrol.ExecutionWitness) error {
			live, err := observe(ctx)
			if err != nil {
				return err
			}
			if signed.Generation != live.Verified.Generation || signed.VerifiedDigest != live.Verified.Digest ||
				signed.ObservationDigest != live.WitnessDraft.ObservationDigest || signed.AuthorityDigest != live.WitnessDraft.AuthorityDigest {
				return errors.New("signed Front/Worker baseline differs from the live group")
			}
			verifiedSnapshot = live.snapshot
			return nil
		})
	if err != nil {
		return err
	}
	version := productioncontrol.Version{Generation: witness.Generation, Digest: witness.VerifiedDigest}
	created, err := execution.AnchorFrontWorkerBaseline(ctx, "front-worker-observer",
		productioncontrol.Baseline{Scope: scope, Verified: version, EvidenceDigest: args[1]}, func() []byte { return verifiedSnapshot })
	if err != nil {
		return err
	}
	return emit(productioncontrol.Recorded{Digest: args[1], Created: created})
}

func (s nodeWorkerBaselineSettings) validate() error {
	if s.Cutover.Phase != releaseguardian.KubeCutoverNative || s.Kubernetes.Validate() != nil ||
		len(s.Members) == 0 || len(s.Members) > 32 {
		return errors.New("Front/Worker baseline requires a native scope, pinned Kubernetes endpoint and group membership")
	}
	if _, err := releaseguardian.FrontWorkerCutoverPolicies(s.Cutover); err != nil {
		return err
	}
	proofs := []string{s.Evidence.EndpointAdmission, s.Evidence.LegacyRevocation, s.Evidence.LegacyDrain}
	if proofs[0] == proofs[1] || proofs[0] == proofs[2] || proofs[1] == proofs[2] {
		return errors.New("Front/Worker baseline requires distinct endpoint, revocation and drain records")
	}
	for _, proof := range proofs {
		if !exactSHA256Digest(proof) {
			return errors.New("Front/Worker baseline requires digests of verified endpoint, revocation and drain records")
		}
	}
	nodes, nodeUIDs, services := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, m := range s.Members {
		path, err := url.ParseRequestURI(m.CanaryPath)
		if len(validation.IsDNS1123Subdomain(m.Node)) != 0 || len(validation.IsDNS1123Subdomain(m.Front)) != 0 ||
			!slices.Contains(s.Cutover.WorkerDaemonSets, m.WorkerA) || !slices.Contains(s.Cutover.WorkerDaemonSets, m.WorkerB) ||
			m.WorkerA == m.WorkerB || m.NodeUID == "" || m.ServiceUID == "" || m.ServiceUID == m.ProbeUID ||
			nodes[m.Node] || nodeUIDs[m.NodeUID] || services[m.ServiceUID] || m.ProbeUID != "" && services[m.ProbeUID] ||
			len(validation.IsDNS1123Subdomain(m.CanaryHost)) != 0 || err != nil || path.Host != "" ||
			!strings.HasPrefix(path.Path, "/") || strings.ContainsAny(m.CanaryPath, "\r\n?#") || !exactSHA256Digest(m.CanaryDigest) {
			return errors.New("Front/Worker baseline member lacks a unique serving Node, enclosed Worker controllers, or a valid canary")
		}
		nodes[m.Node], nodeUIDs[m.NodeUID], services[m.ServiceUID] = true, true, true
		if m.ProbeUID != "" {
			services[m.ProbeUID] = true
		}
	}
	return nil
}

func newBaselineKubernetesClient(settings nodeWorkerBaselineSettings) (kubernetes.Interface, podCommandExecutor, error) {
	config, err := loadKubeConfig()
	if err != nil {
		return nil, nil, err
	}
	if config.Insecure || config.Proxy != nil || config.Transport != nil || config.WrapTransport != nil || config.Dial != nil {
		return nil, nil, errors.New("Front/Worker baseline requires direct verified Kubernetes TLS")
	}
	config, err = participation.BindKubernetesConfig(config, settings.Kubernetes)
	if err != nil {
		return nil, nil, err
	}
	config.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	transport, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, nil, err
	}
	reader := *transport
	reader.Transport = nodeWorkerShadowObjects{next: transport.Transport}
	reader.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client, err := kubernetes.NewForConfigAndClient(config, &reader)
	if err != nil {
		return nil, nil, err
	}
	return client, nodeWorkerShadowExec{next: &kubePodExecutor{config: config, client: client}, namespace: settings.Cutover.Namespace}, nil
}

func observeNodeWorkerBaseline(ctx context.Context, client kubernetes.Interface, executor podCommandExecutor,
	scope productioncontrol.Scope, settings nodeWorkerBaselineSettings) (nodeWorkerBaselineCheckpoint, error) {
	cutover := settings.Cutover
	store, err := releaseguardian.NewAuthorityStore(client, cutover.Namespace)
	if err != nil {
		return nodeWorkerBaselineCheckpoint{}, err
	}
	nodes := make([]string, 0, len(settings.Members))
	for _, m := range settings.Members {
		nodes = append(nodes, m.Node)
	}
	readStableScope := func() (releaseguardian.FrontWorkerBaselineBoundary, []corev1.Service, []corev1.Pod, error) {
		facts, err := store.ObserveFrontWorkerBaselineBoundary(ctx, cutover, nodes)
		if err != nil {
			return facts, nil, nil, err
		}
		ns, err := client.CoreV1().Namespaces().Get(ctx, cutover.Namespace, metav1.GetOptions{})
		if err != nil || ns.Name != cutover.Namespace || ns.DeletionTimestamp != nil || ns.ResourceVersion == "" ||
			string(ns.UID) != settings.Kubernetes.NamespaceUID {
			return facts, nil, nil, errors.Join(errors.New("Front/Worker namespace is outside the pinned Kubernetes binding"), err)
		}
		options := metav1.ListOptions{Limit: nodeWorkerListLimit}
		services, err := client.CoreV1().Services(cutover.Namespace).List(ctx, options)
		if err != nil || services.Continue != "" || len(services.Items) > nodeWorkerListLimit {
			return facts, nil, nil, errors.Join(errors.New("Front/Worker baseline Service inventory is incomplete"), err)
		}
		pods, err := client.CoreV1().Pods(cutover.Namespace).List(ctx, options)
		if err != nil || pods.Continue != "" || len(pods.Items) > nodeWorkerListLimit {
			return facts, nil, nil, errors.Join(errors.New("Front/Worker baseline Pod inventory is incomplete"), err)
		}
		if err := checkNodeWorkerBaselineCoverage(cutover.Group, settings.Members, services.Items, pods.Items); err != nil {
			return facts, nil, nil, err
		}
		return facts, services.Items, pods.Items, nil
	}
	boundary, services, pods, err := readStableScope()
	if err != nil {
		return nodeWorkerBaselineCheckpoint{}, err
	}
	observed := make([]nodeWorkerBaselineObservation, 0, len(settings.Members))
	for _, member := range settings.Members {
		front := &frontAuthorityActivator{client: client, executor: executor, now: time.Now, config: frontAuthorityConfig{
			Namespace: cutover.Namespace, GroupID: cutover.Group, RouteHost: member.CanaryHost,
			RoutePath: member.CanaryPath, RouteBodyDigest: member.CanaryDigest}}
		fact, err := front.observeBaselineMember(ctx, member, services, pods)
		if err != nil {
			return nodeWorkerBaselineCheckpoint{}, fmt.Errorf("observe Front/Worker baseline for %s: %w", member.Node, err)
		}
		observed = append(observed, fact)
	}
	after, liveServices, livePods, err := readStableScope()
	if err != nil || !reflect.DeepEqual(boundary, after) {
		return nodeWorkerBaselineCheckpoint{}, errors.Join(errors.New("Front/Worker group write boundary changed during baseline readback"), err)
	}
	for _, fact := range observed {
		front := &frontAuthorityActivator{client: client, executor: executor, now: time.Now,
			config: frontAuthorityConfig{Namespace: cutover.Namespace, GroupID: cutover.Group}}
		latest, _, _, err := front.readBaselineMember(ctx, fact.Member, liveServices, livePods)
		if err != nil {
			return nodeWorkerBaselineCheckpoint{}, err
		}
		latest.Serving.RouteDigest = fact.Serving.RouteDigest
		if !reflect.DeepEqual(latest, fact) {
			return nodeWorkerBaselineCheckpoint{}, errors.New("Front/Worker serving binding changed before the group baseline closed")
		}
	}
	finalBoundary, err := store.ObserveFrontWorkerBaselineBoundary(ctx, cutover, nodes)
	if err != nil || !reflect.DeepEqual(after, finalBoundary) {
		return nodeWorkerBaselineCheckpoint{}, errors.Join(errors.New("Front/Worker group write boundary changed before baseline completed"), err)
	}
	return makeNodeWorkerBaselineCheckpoint(scope, settings, boundary, observed, time.Now())
}

func makeNodeWorkerBaselineCheckpoint(scope productioncontrol.Scope, settings nodeWorkerBaselineSettings,
	boundary releaseguardian.FrontWorkerBaselineBoundary, observed []nodeWorkerBaselineObservation, now time.Time) (nodeWorkerBaselineCheckpoint, error) {
	live, err := json.Marshal(struct {
		Scope      productioncontrol.Scope
		Kubernetes productioncontrol.ParticipationKubernetesBinding
		Serving    []nodeWorkerBaselineObservation
	}{scope, settings.Kubernetes, observed})
	if err != nil {
		return nodeWorkerBaselineCheckpoint{}, err
	}
	authority, err := json.Marshal(struct {
		Boundary releaseguardian.FrontWorkerBaselineBoundary
		Evidence nodeWorkerBaselineEvidence
	}{boundary, settings.Evidence})
	if err != nil {
		return nodeWorkerBaselineCheckpoint{}, err
	}
	version, err := productioncontrol.FrontWorkerBaselineVersion(live)
	if err != nil {
		return nodeWorkerBaselineCheckpoint{}, err
	}
	draft := productioncontrol.ExecutionWitness{
		Kind: productioncontrol.BaselineWitness, KeyID: "provider-observer", ClusterID: scope.ClusterID, ResourceID: scope.ResourceID,
		Generation: version.Generation, VerifiedDigest: version.Digest,
		ObservationDigest: shaDigest(append([]byte("fugue.front-worker.baseline-serving.v1\x00"), live...)),
		AuthorityDigest:   shaDigest(append([]byte("fugue.front-worker.baseline-authority.v1\x00"), authority...)),
		ObservedAt:        now.Unix(),
	}
	return nodeWorkerBaselineCheckpoint{Scope: scope, Verified: version, Kubernetes: settings.Kubernetes,
		Serving: observed, Boundary: boundary, Evidence: settings.Evidence, WitnessDraft: draft, snapshot: live}, nil
}

func checkNodeWorkerBaselineCoverage(group string, members []nodeWorkerBaselineMember, services []corev1.Service, pods []corev1.Pod) error {
	enrolled := make(map[string]string, len(members))
	for _, member := range members {
		enrolled[member.ServiceUID] = member.Node
	}
	for _, service := range services {
		if service.Labels["app.kubernetes.io/managed-by"] != "fugue-front-serving-transport" {
			continue
		}
		selector := labels.SelectorFromSet(service.Spec.Selector)
		inGroup := service.Spec.Selector["fugue.io/edge-group-id"] == group || service.Labels["fugue.io/edge-group-id"] == group
		for _, pod := range pods {
			if len(service.Spec.Selector) != 0 && pod.Labels["fugue.io/edge-group-id"] == group && selector.Matches(labels.Set(pod.Labels)) {
				inGroup = true
			}
		}
		if inGroup && enrolled[string(service.UID)] == "" {
			return fmt.Errorf("managed Front Service %s is outside the enrolled group baseline", service.Name)
		}
	}
	// A legacy host listener has no Service selector and would otherwise
	// be invisible in the group-wide inventory.
	for _, pod := range pods {
		if pod.Labels["fugue.io/edge-group-id"] != group || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if container.Name != "edge-front" {
				continue
			}
			for _, port := range container.Ports {
				if port.HostPort == 443 || port.HostPort == 80 || pod.Spec.HostNetwork && (port.ContainerPort == 443 || port.ContainerPort == 80) {
					return fmt.Errorf("Front Pod %s still has legacy direct ingress outside the managed group baseline", pod.Name)
				}
			}
		}
	}
	return nil
}

func (a *frontAuthorityActivator) readBaselineMember(ctx context.Context, member nodeWorkerBaselineMember, services []corev1.Service, pods []corev1.Pod) (nodeWorkerBaselineObservation, nodeWorkerRuntime, edgegroupfront.NodeWorkerJournal, error) {
	var observation nodeWorkerBaselineObservation
	var runtime nodeWorkerRuntime
	ns, group := a.config.Namespace, a.config.GroupID
	binding := edgegroupfront.NodeWorkerJournal{ServiceUID: member.ServiceUID, ProbeServiceUID: member.ProbeUID, NodeUID: member.NodeUID}
	service, host, err := selectNodeWorkerService(services, binding, ns)
	if err != nil {
		return observation, runtime, binding, err
	}
	selector := labels.SelectorFromSet(service.Spec.Selector)
	var front corev1.Pod
	for _, pod := range pods {
		if selector.Matches(labels.Set(pod.Labels)) {
			if front.UID != "" {
				return observation, runtime, binding, errors.New("baseline Service selects more than one Front Pod")
			}
			front = pod
		}
	}
	owner := metav1.GetControllerOf(&front)
	if front.UID == "" || front.Spec.NodeName != member.Node || front.Labels["fugue.io/edge-group-id"] != group ||
		owner == nil || owner.Name != member.Front || owner.Kind != "DaemonSet" || owner.UID == "" {
		return observation, runtime, binding, errors.New("baseline Service does not select the enrolled Front")
	}
	if err := a.verifyBaselineDaemonSet(ctx, owner); err != nil {
		return observation, runtime, binding, err
	}
	var recipient edgegroupfront.NativeRecipient
	raw, err := a.executor.Exec(ctx, ns, front.Name, "edge-front", frontActivationCASBinary, "--worker-recipient")
	if err != nil || decodeStrictJSON(raw, &recipient) != nil || recipient.PodUID != string(front.UID) ||
		recipient.Node != member.Node || recipient.Group != group || !exactSHA256Digest(recipient.Incarnation) {
		return observation, runtime, binding, errors.Join(errors.New("enrolled Front did not identify its exact running incarnation"), err)
	}
	binding.Recipient = recipient
	raw, err = a.executor.Exec(ctx, ns, front.Name, "edge-front", "cat", edgegroupfront.NativeStatePath)
	if err != nil {
		return observation, runtime, binding, err
	}
	state, err := edgegroupfront.DecodeActivationState(raw)
	if err != nil || state.GroupID != group || state.UpdatedAt.After(a.now().UTC()) {
		return observation, runtime, binding, errors.Join(errors.New("enrolled Front has invalid durable activation"), err)
	}
	binding.Expected = state
	activeWorker := member.WorkerA
	if state.ActiveSlot == "b" {
		activeWorker = member.WorkerB
	}
	for _, pod := range pods {
		if pod.Spec.NodeName != member.Node || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		owner := metav1.GetControllerOf(&pod)
		if owner != nil && owner.Kind == "DaemonSet" && owner.Name == activeWorker && owner.UID != "" {
			if binding.PredecessorWorkerUID != "" {
				return observation, runtime, binding, errors.New("baseline selected Worker has multiple identities")
			}
			binding.PredecessorWorkerUID = string(pod.UID)
		}
	}
	pin := edgegroupfront.ActivationRepairRelease{Slot: state.ActiveSlot, BundleGeneration: state.BundleGeneration,
		WorkerSourceCommit: state.WorkerSourceCommit, WorkerImageDigest: state.WorkerImageDigest}
	_, port := workerSlotPorts(state.ActiveSlot)
	worker, err := observeNodeWorkerProcess(pods, front, binding.PredecessorWorkerUID, pin, host, port)
	if err != nil {
		return observation, runtime, binding, err
	}
	if err := a.verifyBaselineDaemonSet(ctx, metav1.GetControllerOf(&worker.pod)); err != nil {
		return observation, runtime, binding, err
	}
	binding.PredecessorProcess = edgegroupfront.NodeWorkerProcessDigest(worker.edgeID, worker.caddyID)
	runtime, err = a.readNodeWorkerRuntime(ctx, binding, false, true, false, true)
	if err != nil {
		return observation, runtime, binding, err
	}
	local := make([]corev1.Pod, 0)
	for _, pod := range pods {
		if pod.Spec.NodeName == member.Node {
			local = append(local, pod)
		}
	}
	if err := requireExclusiveNodeWorkerIngress(services, local, runtime); err != nil {
		return observation, runtime, binding, err
	}
	var health nodeWorkerFrontHealth
	if err := readAuthorityRuntimeJSON(ctx, repairHealthEndpoint(runtime.front.Status.PodIP, frontHealthPort, "/healthz"), &health); err != nil ||
		!frontRepairLiveMatches(health.observedFront, state) || health.Target != runtime.prior.target {
		return observation, runtime, binding, errors.Join(errors.New("Front health does not match the selected Worker and durable activation"), err)
	}
	return nodeWorkerBaselineObservation{Member: member, Recipient: recipient, Process: binding.PredecessorProcess,
		Serving: nodeWorkerTerminalServing{Activation: state, FrontUID: recipient.PodUID, WorkerUID: binding.PredecessorWorkerUID,
			Target: runtime.prior.target, ServiceUID: member.ServiceUID, ProbeUID: member.ProbeUID, Canary: member.CanaryDigest}}, runtime, binding, nil
}

func (a *frontAuthorityActivator) verifyBaselineDaemonSet(ctx context.Context, owner *metav1.OwnerReference) error {
	if owner == nil || owner.Kind != "DaemonSet" || owner.APIVersion != "apps/v1" {
		return errors.New("baseline Pod lacks an exact DaemonSet owner")
	}
	ds, err := a.client.AppsV1().DaemonSets(a.config.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil || ds.UID != owner.UID || ds.ResourceVersion == "" || ds.DeletionTimestamp != nil {
		return errors.Join(errors.New("baseline Pod does not belong to the current DaemonSet incarnation"), err)
	}
	return nil
}

func (a *frontAuthorityActivator) observeBaselineMember(ctx context.Context, m nodeWorkerBaselineMember, services []corev1.Service, pods []corev1.Pod) (nodeWorkerBaselineObservation, error) {
	fact, runtime, binding, err := a.readBaselineMember(ctx, m, services, pods)
	if err != nil {
		return fact, err
	}
	if err := a.verifyLocalWorkerServing(ctx, binding, runtime.prior, fact.Serving.Activation.BundleGeneration); err != nil {
		return fact, err
	}
	public, probe, err := nodeWorkerIngressAddresses(runtime, binding)
	if err != nil {
		return fact, err
	}
	slot, target := fact.Serving.Activation.ActiveSlot, fact.Serving.Target
	socket, proof, err := a.openObservedNodeWorkerSocket(ctx, runtime, binding, public, slot, target)
	if err != nil {
		return fact, err
	}
	defer socket.tls.close()
	if probe != (netip.AddrPort{}) {
		probeSocket, probeProof, err := a.openObservedNodeWorkerSocket(ctx, runtime, binding, probe, slot, target)
		if err != nil {
			return fact, err
		}
		defer probeSocket.tls.close()
		if !sameNodeWorkerProof(proof, probeProof) || socket.fact.Connection.ID == probeSocket.fact.Connection.ID {
			return fact, errors.New("public and probe sockets did not independently reach the same baseline route")
		}
	}
	fact.Serving.RouteDigest = proof.Digest
	return fact, nil
}
