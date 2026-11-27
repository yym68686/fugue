package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"syscall"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"github.com/jackc/pgx/v5/stdlib"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type nodeWorkerSettlementSettings struct {
	JournalFile       string                             `json:"journalFile"`
	Cutover           releaseguardian.FrontWorkerCutover `json:"cutover"`
	WitnessDirectory  string                             `json:"witnessDirectory"`
	ObserverPublicKey ed25519.PublicKey                  `json:"observerPublicKey"`
}

type nodeWorkerTerminalServing struct {
	Activation  edgegroupfront.ActivationState `json:"activation"`
	FrontUID    string                         `json:"frontUid"`
	WorkerUID   string                         `json:"workerUid"`
	Target      string                         `json:"target"`
	ServiceUID  string                         `json:"serviceUid"`
	ProbeUID    string                         `json:"probeUid,omitempty"`
	Retirement  *nodeWorkerRetirementReadback  `json:"retirement,omitempty"`
	Canary      string                         `json:"canaryDigest"`
	RouteDigest string                         `json:"routeDigest"`
}

type nodeWorkerCheckpoint struct {
	Claim             productioncontrol.Claim                    `json:"claim"`
	Result            productioncontrol.ExecutionResult          `json:"result"`
	Serving           nodeWorkerTerminalServing                  `json:"serving"`
	Closure           releaseguardian.FrontWorkerTerminalClosure `json:"closure"`
	ObservationDigest string                                     `json:"observationDigest"`
	AuthorityDigest   string                                     `json:"authorityDigest"`
	WitnessDraft      productioncontrol.ExecutionWitness         `json:"witnessDraft"`
}

const nodeWorkerSettlementUsage = "usage: fugue-release-guardian node-worker-checkpoint CONFIG INTENT_DIGEST or node-worker-settle CONFIG INTENT_DIGEST WITNESS_DIGEST"

func runNodeWorkerSettlement(parent context.Context, settle bool, args []string, out io.Writer) error {
	if (!settle && len(args) != 2) || (settle && len(args) != 3) || out == nil ||
		!exactSHA256Digest(args[1]) || (settle && !exactSHA256Digest(args[2])) {
		return errors.New(nodeWorkerSettlementUsage)
	}
	raw, err := readOfflineDispatchInput(args[0], 16<<10, false)
	if err != nil {
		return err
	}
	var settings nodeWorkerSettlementSettings
	if err := decodeStrictJSON(raw, &settings); err != nil || settings.Cutover.Phase != releaseguardian.KubeCutoverNative {
		return errors.New("node Worker settlement requires a native provider scope")
	}
	if settle && (!filepath.IsAbs(settings.WitnessDirectory) || len(settings.ObserverPublicKey) != ed25519.PublicKeySize) {
		return errors.New("node Worker settlement requires independent witness storage and a trusted observer public key")
	}
	cfg, owners, cluster, err := productioncontrol.LoadJournalEndpoint(settings.JournalFile)
	if err != nil {
		return err
	}
	if !settle {
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
	ctx, cancel := context.WithTimeout(parent, 70*time.Second)
	defer cancel()
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: productioncontrol.FrontWorkerResource(settings.Cutover.Namespace, settings.Cutover.Group)}
	claim, terminal, err := journal.ReadExecution(ctx, scope, args[1])
	if err != nil {
		return err
	}
	intent, payload, err := journal.ReadIntent(ctx, args[1])
	if err != nil {
		return err
	}
	plan, err := productioncontrol.InspectFrontWorker(productioncontrol.Submission{Intent: intent, Payload: payload})
	if err != nil || plan.Namespace != settings.Cutover.Namespace || plan.Journal.Recipient.Group != settings.Cutover.Group ||
		!slices.Contains(settings.Cutover.ComponentLeases, plan.LeaseName) {
		return errors.Join(errors.New("Front/Worker execution is outside the observed provider scope"), err)
	}
	emit := json.NewEncoder(out)
	if terminal != nil {
		if settle && terminal.EvidenceDigest != args[2] {
			return productioncontrol.ErrResultConflict
		}
		return emit.Encode(terminal)
	}
	kube, err := loadKubeConfig()
	if err != nil {
		return err
	}
	reader := rest.CopyConfig(kube)
	reader.Wrap(func(next http.RoundTripper) http.RoundTripper { return nodeWorkerShadowObjects{next: next} })
	client, err := kubernetes.NewForConfig(reader)
	if err != nil {
		return err
	}
	store, err := releaseguardian.NewAuthorityStore(client, plan.Namespace)
	if err != nil {
		return err
	}
	front := &frontAuthorityActivator{client: client, now: time.Now,
		executor: nodeWorkerShadowExec{next: &kubePodExecutor{config: kube, client: client}, namespace: plan.Namespace},
		config: frontAuthorityConfig{GroupID: plan.Journal.Recipient.Group, Namespace: plan.Namespace,
			RouteHost: plan.Rollback.CanaryHost, RoutePath: plan.Rollback.CanaryPath, RouteBodyDigest: plan.Rollback.CanaryBodyDigest},
	}
	observe := func(ctx context.Context) (nodeWorkerCheckpoint, error) {
		state, err := journal.ReadResource(ctx, scope)
		if err != nil || state.Pending != claim.IntentDigest || state.LastFence != claim.Fence {
			return nodeWorkerCheckpoint{}, errors.Join(productioncontrol.ErrStaleFence, err)
		}
		return front.observeNodeWorkerCheckpoint(ctx, store, settings.Cutover, claim, intent, plan)
	}
	checkpoint, err := observe(ctx)
	if err != nil {
		return err
	}
	if !settle {
		return emit.Encode(checkpoint)
	}
	root, err := os.OpenRoot(settings.WitnessDirectory)
	if err != nil {
		return errors.New("independent Front/Worker witness directory is unavailable")
	}
	defer root.Close()
	execution, err := productioncontrol.NewObservedExecutionJournal(journal, nodeWorkerWitnessDirectory{root},
		map[string]productioncontrol.TrustedWitness{"provider-observer": {Scope: scope, PublicKey: settings.ObserverPublicKey}},
		func(ctx context.Context, witness productioncontrol.ExecutionWitness) error {
			latest, err := observe(ctx)
			if err != nil {
				return err
			}
			return checkNodeWorkerTerminalWitness(witness, latest)
		})
	if err != nil {
		return err
	}
	checkpoint.Result.EvidenceDigest = args[2]
	created, err := execution.CompleteExecution(ctx, "front-worker-observer", claim, checkpoint.Result)
	if err != nil {
		return err
	}
	return emit.Encode(productioncontrol.Recorded{Digest: args[2], Created: created})
}

func checkNodeWorkerTerminalWitness(witness productioncontrol.ExecutionWitness, live nodeWorkerCheckpoint) error {
	if witness.ObservationDigest != live.ObservationDigest || witness.AuthorityDigest != live.AuthorityDigest ||
		witness.Outcome != live.Result.Outcome || witness.Generation != live.Result.Verified.Generation ||
		witness.VerifiedDigest != live.Result.Verified.Digest {
		return errors.New("signed Front/Worker serving and closure evidence differs from live provider readback")
	}
	return nil
}

func (a *frontAuthorityActivator) observeNodeWorkerCheckpoint(ctx context.Context, store *releaseguardian.AuthorityStore,
	config releaseguardian.FrontWorkerCutover, claim productioncontrol.Claim, intent productioncontrol.Intent,
	plan productioncontrol.FrontWorkerShadow) (nodeWorkerCheckpoint, error) {
	j := plan.Journal
	before, result, err := store.ObserveFrontWorkerTerminalClosure(ctx, config, j, claim, a.now().UTC())
	if err != nil {
		return nodeWorkerCheckpoint{}, err
	}
	serving, err := a.observeNodeWorkerTerminalServing(ctx, store, plan, result)
	if err != nil {
		return nodeWorkerCheckpoint{}, err
	}
	closure, checked, err := store.ObserveFrontWorkerTerminalClosure(ctx, config, j, claim, a.now().UTC())
	if err != nil || !reflect.DeepEqual(before, closure) || !reflect.DeepEqual(result, checked) {
		return nodeWorkerCheckpoint{}, errors.Join(errors.New("Front/Worker terminal authority changed during serving observation"), err)
	}
	verified, err := terminalFrontWorkerVersion(intent, result.Outcome)
	if err != nil {
		return nodeWorkerCheckpoint{}, err
	}
	servingWire, err := json.Marshal(serving)
	if err != nil {
		return nodeWorkerCheckpoint{}, err
	}
	closureWire, err := json.Marshal(closure)
	if err != nil {
		return nodeWorkerCheckpoint{}, err
	}
	checkpoint := nodeWorkerCheckpoint{Claim: claim, Result: verified, Serving: serving, Closure: closure,
		ObservationDigest: shaDigest(append([]byte("fugue.front-worker.serving.v1\x00"), servingWire...)),
		AuthorityDigest:   shaDigest(append([]byte("fugue.front-worker.closed.v1\x00"), closureWire...))}
	checkpoint.WitnessDraft = productioncontrol.ExecutionWitness{
		Kind: productioncontrol.TerminalWitness, KeyID: "provider-observer", ClusterID: claim.ClusterID, ResourceID: claim.ResourceID,
		Generation: verified.Verified.Generation, VerifiedDigest: verified.Verified.Digest,
		ObservationDigest: checkpoint.ObservationDigest, AuthorityDigest: checkpoint.AuthorityDigest, ObservedAt: a.now().Unix(),
		IntentDigest: claim.IntentDigest, ApprovalDigest: claim.ApprovalDigest, Fence: claim.Fence, Outcome: verified.Outcome,
	}
	return checkpoint, nil
}

func terminalFrontWorkerVersion(intent productioncontrol.Intent, outcome string) (productioncontrol.ExecutionResult, error) {
	switch outcome {
	case "verified":
		return productioncontrol.ExecutionResult{Outcome: productioncontrol.Applied,
			Verified: productioncontrol.Version{Generation: intent.Generation + 1, Digest: intent.TargetDigest}}, nil
	case "reverted":
		return productioncontrol.ExecutionResult{Outcome: productioncontrol.Restored,
			Verified: productioncontrol.Version{Generation: intent.Generation + 1, Digest: intent.RollbackDigest}}, nil
	case "expired_without_cutover":
		return productioncontrol.ExecutionResult{Outcome: productioncontrol.NotApplied,
			Verified: productioncontrol.Version{Generation: intent.Generation, Digest: intent.CurrentDigest}}, nil
	default:
		return productioncontrol.ExecutionResult{}, errors.New("unknown Front/Worker terminal outcome")
	}
}

func (a *frontAuthorityActivator) observeNodeWorkerTerminalServing(ctx context.Context, store *releaseguardian.AuthorityStore,
	plan productioncontrol.FrontWorkerShadow, result releaseguardian.NodeWorkerResult) (nodeWorkerTerminalServing, error) {
	var observation nodeWorkerTerminalServing
	j := plan.Journal
	applied := result.Outcome == "verified"
	retired := applied && plan.Retirement != nil
	runtime, err := a.observeNodeWorkerRuntimeWithRetirement(ctx, j, applied, true, retired)
	if err != nil {
		return observation, err
	}
	raw, err := a.executor.Exec(ctx, plan.Namespace, runtime.front.Name, "edge-front", frontActivationCASBinary, "--worker-recipient")
	var recipient edgegroupfront.NativeRecipient
	if err != nil || decodeStrictJSON(raw, &recipient) != nil || recipient != j.Recipient {
		return observation, errors.Join(errors.New("Front/Worker terminal observation is on a different Front incarnation"), err)
	}
	selected, err := a.readNodeWorkerSelection(ctx, j, runtime)
	if err != nil || selected != result.Activation {
		return observation, errors.Join(errors.New("live Front does not serve the immutable terminal activation"), err)
	}
	worker := runtime.prior
	if applied {
		worker = runtime.next
		if err := store.VerifyNodeWorkerArtifact(ctx, j, worker.pod); err != nil {
			return observation, err
		}
	}
	if err := a.verifyLocalWorkerServing(ctx, j, worker, result.Activation.BundleGeneration); err != nil {
		return observation, err
	}
	public, probe, err := nodeWorkerIngressAddresses(runtime, j)
	if err != nil {
		return observation, err
	}
	socket, proof, err := a.openObservedNodeWorkerSocket(ctx, runtime, j, public, selected.ActiveSlot, worker.target)
	if err != nil {
		return observation, err
	}
	defer socket.tls.close()
	if probe.IsValid() {
		s, p, err := a.openObservedNodeWorkerSocket(ctx, runtime, j, probe, selected.ActiveSlot, worker.target)
		if err != nil {
			return observation, err
		}
		defer s.tls.close()
		if !sameNodeWorkerProof(proof, p) || s.fact.Connection.ID == socket.fact.Connection.ID {
			return observation, errors.New("public Front and probe did not independently serve the same terminal route")
		}
	}
	fresh, err := a.observeNodeWorkerRuntimeWithRetirement(ctx, j, applied, true, retired)
	if err != nil || !runtime.sameBinding(fresh, applied) {
		return observation, errors.Join(errors.New("Front/Worker physical binding changed during terminal readback"), err)
	}
	current, err := a.readNodeWorkerSelection(ctx, j, fresh)
	if err != nil || current != selected {
		return observation, errors.Join(errors.New("Front/Worker terminal selection changed during readback"), err)
	}
	var retirement *nodeWorkerRetirementReadback
	if retired {
		retirement, err = a.observeWorkerRetirement(ctx, plan, result, fresh.front.Status.HostIP)
		if err != nil {
			return observation, err
		}
	}
	return nodeWorkerTerminalServing{
		Activation: current, FrontUID: string(fresh.front.UID), WorkerUID: string(worker.pod.UID),
		Target: worker.target, ServiceUID: string(fresh.service.UID), ProbeUID: string(fresh.probe.UID),
		Canary: plan.Rollback.CanaryBodyDigest, RouteDigest: proof.Digest, Retirement: retirement,
	}, nil
}

type nodeWorkerRetirementReadback struct {
	Decision        workerRetirementDecision `json:"decision"`
	ReceiptUID      string                   `json:"receiptUid"`
	ReceiptRevision string                   `json:"receiptRevision"`
}

func (a *frontAuthorityActivator) observeWorkerRetirement(ctx context.Context, plan productioncontrol.FrontWorkerShadow,
	result releaseguardian.NodeWorkerResult, host string) (*nodeWorkerRetirementReadback, error) {
	j, approved := plan.Journal, plan.Retirement
	if result.DrainStart == nil || approved == nil {
		return nil, errors.New("Worker retirement lacks a drain start or approved plan")
	}
	expected := workerRetirementPlan{Schema: approved.Schema, JournalDigest: approved.JournalDigest,
		DaemonSet: approved.DaemonSet, DaemonSetUID: types.UID(approved.DaemonSetUID), Generation: approved.Generation}
	ds, err := a.client.AppsV1().DaemonSets(plan.Namespace).Get(ctx, expected.DaemonSet, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	decision, err := expected.checkDS(ds, j, result.DrainStart.Add(edgegroupfront.WorkerDrainLimit).UTC())
	if err != nil || decision == nil || ds.Status.ObservedGeneration != ds.Generation {
		return nil, errors.Join(errors.New("retired Worker's DaemonSet has not reconciled the approved node exclusion"), err)
	}
	name := retirementName("retired", j)
	receipt, err := a.client.CoreV1().ConfigMaps(plan.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var recorded workerRetirementDecision
	if receipt.Name != name || receipt.Namespace != plan.Namespace || receipt.UID == "" || receipt.ResourceVersion == "" ||
		receipt.DeletionTimestamp != nil || receipt.Immutable == nil || !*receipt.Immutable ||
		receipt.Labels["app.kubernetes.io/managed-by"] != "fugue-release-guardian" || receipt.Labels["fugue.pro/group"] != j.Recipient.Group ||
		receipt.Labels["fugue.pro/authority-kind"] != "node-worker-retired" || len(receipt.Data) != 1 || len(receipt.BinaryData) != 0 ||
		len(receipt.Data["retired.json"]) > 16<<10 || decodeStrictJSON([]byte(receipt.Data["retired.json"]), &recorded) != nil ||
		!reflect.DeepEqual(recorded, *decision) {
		return nil, errors.New("retired Worker's receipt does not prove the approved node exclusion")
	}
	httpPort, httpsPort := workerSlotPorts(j.Expected.ActiveSlot)
	for _, port := range []uint16{httpPort, httpsPort} {
		address := net.JoinHostPort(host, strconv.Itoa(int(port)))
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
		if err == nil {
			_ = conn.Close()
			return nil, errors.New("retired Worker listener is still reachable")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, errors.Join(errors.New("retired Worker listener did not confirm closure"), err)
		}
	}
	return &nodeWorkerRetirementReadback{Decision: recorded, ReceiptUID: string(receipt.UID), ReceiptRevision: receipt.ResourceVersion}, nil
}

type nodeWorkerWitnessDirectory struct{ root *os.Root }

func (s nodeWorkerWitnessDirectory) ReadExecutionWitness(ctx context.Context, digest string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !exactSHA256Digest(digest) {
		return nil, errors.New("invalid Front/Worker witness digest")
	}
	file, err := s.root.OpenFile(digest[7:]+".json", os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("Front/Worker witness not found")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > productioncontrol.MaxWitnessBytes {
		return nil, errors.New("Front/Worker witness must be a bounded regular file")
	}
	return io.ReadAll(io.LimitReader(file, productioncontrol.MaxWitnessBytes+1))
}
