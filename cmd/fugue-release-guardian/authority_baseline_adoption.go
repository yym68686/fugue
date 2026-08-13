package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	frontReadyPort   = 7831
	workerHealthPort = 7832
)

type authorityBaselineConfig struct {
	GroupID              string
	ExpectedRecordDigest string
	ExpectedWorkerSlot   releaseguardian.AuthoritySlot
	ExpectedEpoch        int64
	FrontComponent       string
	ExpectedNodes        int
	SlotAddresses        map[releaseguardian.AuthoritySlot]string
	Host                 string
	Path                 string
	ExpectedBodyDigest   string
}

type authorityBaselineStore interface {
	LoadCurrent(context.Context, string) (releaseguardian.CurrentAuthority, types.UID, string, error)
	LoadRouteBundleRecord(context.Context, string, string) (releaseguardian.RouteBundleRecord, error)
	AdoptCurrentBaseline(context.Context, releaseguardian.CurrentAuthority, releaseguardian.AuthorityBaselineReceipt, types.UID, string) (types.UID, string, error)
}

type baselineFrontHealth struct {
	Status             string `json:"status"`
	ActiveSlot         string `json:"active_slot"`
	ActivationPresent  bool   `json:"activation_present"`
	Generation         uint64 `json:"activation_generation"`
	BundleGeneration   string `json:"bundle_generation"`
	WorkerSourceCommit string `json:"worker_source_commit"`
	WorkerImageDigest  string `json:"worker_image_digest"`
	RouteAuthority     string `json:"route_authority"`
}

type baselineWorkerHealth struct {
	Healthy               bool   `json:"healthy"`
	EdgeGroupID           string `json:"edge_group_id"`
	BundleVersion         string `json:"bundle_version"`
	PublicationSequence   uint64 `json:"publication_sequence"`
	ServingGeneration     string `json:"serving_generation"`
	CandidateBundleLoaded bool   `json:"candidate_bundle_loaded"`
	CandidateRecordDigest string `json:"candidate_record_digest"`
	CandidateWorkerSlot   string `json:"candidate_worker_slot"`
}

func parseAuthorityBaselines(value string) ([]authorityBaselineConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	configs := make([]authorityBaselineConfig, 0, 2)
	seen := map[string]bool{}
	for _, raw := range strings.Split(value, ";") {
		fields := strings.Split(raw, ",")
		if len(fields) != 11 {
			return nil, errors.New("authority baseline must be group,record,slot,epoch,front-component,node-count,address-a,address-b,host,path,body-digest")
		}
		epoch, epochErr := strconv.ParseInt(strings.TrimSpace(fields[3]), 10, 64)
		nodes, nodesErr := strconv.Atoi(strings.TrimSpace(fields[5]))
		config := authorityBaselineConfig{
			GroupID: strings.TrimSpace(fields[0]), ExpectedRecordDigest: strings.TrimSpace(fields[1]),
			ExpectedWorkerSlot: releaseguardian.AuthoritySlot(strings.TrimSpace(fields[2])), ExpectedEpoch: epoch,
			FrontComponent: strings.TrimSpace(fields[4]), ExpectedNodes: nodes,
			SlotAddresses: map[releaseguardian.AuthoritySlot]string{releaseguardian.AuthoritySlotA: strings.TrimSpace(fields[6]), releaseguardian.AuthoritySlotB: strings.TrimSpace(fields[7])},
			Host:          strings.TrimSpace(fields[8]), Path: strings.TrimSpace(fields[9]), ExpectedBodyDigest: strings.TrimSpace(fields[10]),
		}
		probe := candidateCanaryProbe{GroupID: config.GroupID, SlotAddresses: config.SlotAddresses, Host: config.Host, Path: config.Path,
			ExpectedBodyDigest: config.ExpectedBodyDigest, Interval: 10 * time.Second, KeyID: "baseline-adoption", SigningMaterialFile: "/baseline/token"}
		if epochErr != nil || nodesErr != nil || config.ExpectedEpoch < 1 || config.ExpectedNodes < 1 || config.ExpectedNodes > 100 ||
			!exactSHA256Digest(config.ExpectedRecordDigest) || config.ExpectedWorkerSlot.Validate() != nil ||
			(releaseguardian.Key{Component: config.FrontComponent}).Validate() != nil || !validCandidateProbe(probe) || seen[config.GroupID] {
			return nil, errors.New("authority baseline configuration is invalid")
		}
		seen[config.GroupID] = true
		configs = append(configs, config)
	}
	return configs, nil
}

func startAuthorityBaselineAdopters(ctx context.Context, store *releaseguardian.AuthorityStore, client kubernetes.Interface, namespace string, configs []authorityBaselineConfig) {
	for _, config := range configs {
		config := config
		go func() {
			for {
				done, err := adoptAuthorityBaselineOnce(ctx, store, client, namespace, config, time.Now().UTC())
				if err != nil {
					fmt.Fprintf(os.Stderr, "authority baseline %s: %v\n", config.GroupID, err)
				}
				if done {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
				}
			}
		}()
	}
}

func adoptAuthorityBaselineOnce(ctx context.Context, store authorityBaselineStore, client kubernetes.Interface, namespace string, config authorityBaselineConfig, now time.Time) (bool, error) {
	if store == nil || client == nil || strings.TrimSpace(namespace) == "" {
		return false, errors.New("authority baseline dependency is unavailable")
	}
	before, uid, rv, err := store.LoadCurrent(ctx, config.GroupID)
	if err != nil {
		return false, err
	}
	if before.BaselineReceiptDigest != "" {
		return true, nil
	}
	if before.CurrentRecordDigest != config.ExpectedRecordDigest || before.CurrentWorkerSlot != config.ExpectedWorkerSlot || before.AuthorityEpoch != config.ExpectedEpoch ||
		before.PreviousRecordDigest != "" || before.PreviousWorkerSlot != "" {
		return false, errors.New("authority baseline predecessor does not match the one-time authorization")
	}
	fronts, err := observeBaselineFronts(ctx, client, namespace, config)
	if err != nil {
		return false, err
	}
	activeSlot := releaseguardian.AuthoritySlot("")
	for _, front := range fronts {
		if activeSlot == "" {
			activeSlot = releaseguardian.AuthoritySlot(front.health.ActiveSlot)
		} else if activeSlot != releaseguardian.AuthoritySlot(front.health.ActiveSlot) {
			return false, errors.New("Front baseline contains mixed active slots")
		}
	}
	if activeSlot.Validate() != nil || activeSlot == before.CurrentWorkerSlot {
		return false, errors.New("Front baseline is not a distinct active authority")
	}
	workers, recordDigest, epoch, witnesses, err := observeBaselineWorkers(ctx, client, namespace, config, activeSlot, fronts)
	if err != nil {
		return false, err
	}
	_ = workers
	record, err := store.LoadRouteBundleRecord(ctx, config.GroupID, recordDigest)
	if err != nil || record.Epoch != epoch {
		return false, errors.New("active Front route record is unavailable")
	}
	status, body, headers, routeErr := requestAuthorityBaselineRoute(ctx, config.SlotAddresses[activeSlot], config.Host, config.Path)
	if routeErr != nil || status < 200 || status >= 300 || shaDigest(body) != config.ExpectedBodyDigest ||
		strings.TrimSpace(headers.Get("X-Fugue-Candidate-Record-Digest")) != recordDigest ||
		strings.TrimSpace(headers.Get("X-Fugue-Candidate-Worker-Slot")) != string(activeSlot) {
		return false, errors.New("active Front route does not attest the baseline record")
	}
	receipt, err := (releaseguardian.AuthorityBaselineReceipt{
		GroupID: config.GroupID, BeforeRecordDigest: before.CurrentRecordDigest, BeforeWorkerSlot: before.CurrentWorkerSlot, BeforeAuthorityEpoch: before.AuthorityEpoch,
		RecordDigest: recordDigest, WorkerSlot: activeSlot, AuthorityEpoch: epoch, Nodes: witnesses, ObservedAt: now.Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		return false, err
	}
	after := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: config.GroupID, CurrentRecordDigest: recordDigest, CurrentWorkerSlot: activeSlot, AuthorityEpoch: epoch, BaselineReceiptDigest: receipt.ReceiptDigest}
	if _, _, err := store.AdoptCurrentBaseline(ctx, after, receipt, uid, rv); err != nil {
		return false, err
	}
	return true, nil
}

type observedBaselineFront struct {
	pod    corev1.Pod
	health baselineFrontHealth
}

func observeBaselineFronts(ctx context.Context, client kubernetes.Interface, namespace string, config authorityBaselineConfig) (map[string]observedBaselineFront, error) {
	selector := labels.Set{"fugue.io/edge-group-id": config.GroupID, "app.kubernetes.io/component": config.FrontComponent}.AsSelector().String()
	list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: int64(config.ExpectedNodes + 1)})
	if err != nil || list.Continue != "" || len(list.Items) != config.ExpectedNodes {
		return nil, errors.New("Front baseline cohort is unavailable")
	}
	result := make(map[string]observedBaselineFront, len(list.Items))
	for index := range list.Items {
		pod := list.Items[index]
		if pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Spec.NodeName == "" || pod.Status.PodIP == "" || !podReady(pod.Status.Conditions) {
			return nil, errors.New("Front baseline Pod is invalid")
		}
		var health baselineFrontHealth
		if err := readAuthorityBaselineJSON(ctx, "http://"+pod.Status.PodIP+":"+strconv.Itoa(frontReadyPort)+"/readyz", &health); err != nil || health.Status != "ok" ||
			!health.ActivationPresent || health.Generation < 1 || releaseguardian.AuthoritySlot(health.ActiveSlot).Validate() != nil || health.RouteAuthority != "edge-control" ||
			!exactSourceSHA(health.WorkerSourceCommit) || !exactSHA256Digest(health.WorkerImageDigest) {
			return nil, errors.New("Front baseline readiness is invalid")
		}
		if strings.TrimSpace(pod.Annotations["fugue.pro/source-commit"]) != health.WorkerSourceCommit ||
			containerRuntimeImageDigest(pod, "edge-front") != health.WorkerImageDigest {
			return nil, errors.New("Front runtime does not match its activation")
		}
		result[pod.Spec.NodeName] = observedBaselineFront{pod: pod, health: health}
	}
	return result, nil
}

func observeBaselineWorkers(ctx context.Context, client kubernetes.Interface, namespace string, config authorityBaselineConfig, slot releaseguardian.AuthoritySlot, fronts map[string]observedBaselineFront) (map[string]corev1.Pod, string, int64, []releaseguardian.AuthorityBaselineNodeWitness, error) {
	selector := labels.Set{"fugue.io/edge-group-id": config.GroupID, "fugue.io/edge-slot": string(slot)}.AsSelector().String()
	list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: int64(config.ExpectedNodes + 1)})
	if err != nil || list.Continue != "" || len(list.Items) != config.ExpectedNodes {
		return nil, "", 0, nil, errors.New("active worker baseline cohort is unavailable")
	}
	workers := make(map[string]corev1.Pod, len(list.Items))
	witnesses := make([]releaseguardian.AuthorityBaselineNodeWitness, 0, len(list.Items))
	recordDigest := ""
	var epoch uint64
	for index := range list.Items {
		pod := list.Items[index]
		front, exists := fronts[pod.Spec.NodeName]
		if !exists || pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Status.PodIP == "" || !podReady(pod.Status.Conditions) {
			return nil, "", 0, nil, errors.New("active worker baseline Pod is invalid")
		}
		var health baselineWorkerHealth
		if err := readAuthorityBaselineJSON(ctx, "http://"+pod.Status.PodIP+":"+strconv.Itoa(workerHealthPort)+"/healthz", &health); err != nil || !health.Healthy ||
			health.EdgeGroupID != config.GroupID || !health.CandidateBundleLoaded || health.CandidateWorkerSlot != string(slot) ||
			!exactSHA256Digest(health.CandidateRecordDigest) || health.PublicationSequence == 0 ||
			!strings.HasPrefix(health.BundleVersion, health.ServingGeneration+".p") || !strings.Contains(health.BundleVersion, ".p"+strconv.FormatUint(health.PublicationSequence, 10)+".") {
			return nil, "", 0, nil, errors.New("active worker baseline health is invalid")
		}
		source := strings.TrimSpace(pod.Annotations["fugue.pro/source-commit"])
		image := edgeRuntimeImageDigest(pod)
		if source != front.health.WorkerSourceCommit || image != front.health.WorkerImageDigest || front.health.BundleGeneration != health.BundleVersion {
			return nil, "", 0, nil, errors.New("Front and active worker baseline identities differ")
		}
		if recordDigest == "" {
			recordDigest, epoch = health.CandidateRecordDigest, health.PublicationSequence
		} else if recordDigest != health.CandidateRecordDigest || epoch != health.PublicationSequence {
			return nil, "", 0, nil, errors.New("active worker baseline contains mixed route records")
		}
		workers[pod.Spec.NodeName] = pod
		witnesses = append(witnesses, releaseguardian.AuthorityBaselineNodeWitness{NodeName: pod.Spec.NodeName,
			FrontPodUID: string(front.pod.UID), FrontResourceVersion: front.pod.ResourceVersion, WorkerPodUID: string(pod.UID), WorkerResourceVersion: pod.ResourceVersion,
			ActivationGeneration: front.health.Generation, BundleGeneration: health.BundleVersion, ServingGeneration: health.ServingGeneration,
			WorkerSourceSHA: source, WorkerImageDigest: image})
	}
	return workers, recordDigest, int64(epoch), witnesses, nil
}

func edgeRuntimeImageDigest(pod corev1.Pod) string {
	return containerRuntimeImageDigest(pod, "edge")
}

func containerRuntimeImageDigest(pod corev1.Pod, container string) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container && status.Ready && status.RestartCount == 0 {
			return imageDigest(status.ImageID)
		}
	}
	return ""
}

func readBoundedJSON(ctx context.Context, endpoint string, destination any) error {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("health endpoint rejected baseline observation")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("health endpoint has trailing data")
	}
	return nil
}

var readAuthorityBaselineJSON = readBoundedJSON
var requestAuthorityBaselineRoute = requestCandidateRoute
