package sourceimport

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	builderHostnameLabelKey          = "kubernetes.io/hostname"
	builderNodeLockLeaseDuration     = 20 * time.Second
	builderReservationLabelSelector  = "app.kubernetes.io/managed-by=fugue,app.kubernetes.io/component=builder-reservation"
	builderReservationComponentValue = "builder-reservation"
	builderNodeLockComponentValue    = "builder-node-lock"
	builderAnnotationNodeName        = "fugue.pro/builder-node-name"
	builderAnnotationCPUMilli        = "fugue.pro/builder-cpu-milli"
	builderAnnotationMemoryBytes     = "fugue.pro/builder-memory-bytes"
	builderAnnotationEphemeralBytes  = "fugue.pro/builder-ephemeral-bytes"
	builderLightEphemeralBufferBytes = int64(2 * 1024 * 1024 * 1024)
	builderHeavyEphemeralBufferBytes = int64(4 * 1024 * 1024 * 1024)
	builderLightMemoryBufferBytes    = int64(512 * 1024 * 1024)
	builderHeavyMemoryBufferBytes    = int64(1024 * 1024 * 1024)
	builderLightCPUBufferMilli       = int64(250)
	builderHeavyCPUBufferMilli       = int64(500)
	builderLightBufferPercent        = 0.10
	builderHeavyBufferPercent        = 0.15
)

var errNoBuilderPlacement = errors.New("no eligible builder nodes")

type builderJobPlacement struct {
	CandidateHostnames []string
	PreferredHostname  string
}

type builderResourceDemand struct {
	CPUMilli       int64
	MemoryBytes    int64
	EphemeralBytes int64
}

type builderCandidate struct {
	Node      builderNodeSnapshot
	Remaining builderResourceDemand
	Score     float64
}

type builderNodeSnapshot struct {
	Name                     string
	Hostname                 string
	Labels                   map[string]string
	Taints                   []builderKubeNodeTaint
	Ready                    bool
	DiskPressure             bool
	Allocatable              builderResourceDemand
	Used                     builderResourceDemand
	FilesystemAvailableBytes int64
}

type builderReservation struct {
	Name      string
	NodeName  string
	Demand    builderResourceDemand
	ExpiresAt time.Time
}

type builderScheduler struct {
	client                   *builderKubeClient
	namespace                string
	policy                   BuilderPodPolicy
	profile                  builderWorkloadProfile
	demand                   builderResourceDemand
	requiredNodeLabels       map[string]string
	candidateCount           int
	selectionTimeout         time.Duration
	retryInterval            time.Duration
	reservationLeaseDuration time.Duration
}

type builderKubeClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
	namespace   string
}

type builderKubeNodeList struct {
	Items []builderKubeNode `json:"items"`
}

type builderKubeNode struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Taints []builderKubeNodeTaint `json:"taints"`
	} `json:"spec"`
	Status struct {
		Conditions  []builderKubeNodeCondition `json:"conditions"`
		Allocatable map[string]string          `json:"allocatable"`
	} `json:"status"`
}

type builderKubeNodeTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect,omitempty"`
}

type builderKubeNodeCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type builderKubeNodeSummary struct {
	Node builderKubeSummaryNode  `json:"node"`
	Pods []builderKubeSummaryPod `json:"pods,omitempty"`
}

type builderKubeSummaryNode struct {
	CPU    builderKubeSummaryCPU `json:"cpu,omitempty"`
	Memory builderKubeSummaryMem `json:"memory,omitempty"`
	FS     builderKubeSummaryFS  `json:"fs,omitempty"`
}

type builderKubeSummaryCPU struct {
	UsageNanoCores *uint64 `json:"usageNanoCores,omitempty"`
}

type builderKubeSummaryMem struct {
	AvailableBytes  *uint64 `json:"availableBytes,omitempty"`
	UsageBytes      *uint64 `json:"usageBytes,omitempty"`
	WorkingSetBytes *uint64 `json:"workingSetBytes,omitempty"`
}

type builderKubeSummaryFS struct {
	AvailableBytes *uint64 `json:"availableBytes,omitempty"`
	CapacityBytes  *uint64 `json:"capacityBytes,omitempty"`
	UsedBytes      *uint64 `json:"usedBytes,omitempty"`
}

type builderKubeSummaryPod struct {
	PodRef           builderKubeSummaryPodRef `json:"podRef"`
	EphemeralStorage builderKubeSummaryFS     `json:"ephemeral-storage,omitempty"`
}

type builderKubeSummaryPodRef struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

type builderKubeLeaseList struct {
	Items []builderKubeLease `json:"items"`
}

type builderKubeLease struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Metadata   struct {
		Name            string            `json:"name,omitempty"`
		Namespace       string            `json:"namespace,omitempty"`
		ResourceVersion string            `json:"resourceVersion,omitempty"`
		Labels          map[string]string `json:"labels,omitempty"`
		Annotations     map[string]string `json:"annotations,omitempty"`
	} `json:"metadata"`
	Spec builderKubeLeaseSpec `json:"spec"`
}

type builderKubeLeaseSpec struct {
	HolderIdentity       string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds int    `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          string `json:"acquireTime,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
}

type builderKubeStatusError struct {
	Method     string
	APIPath    string
	StatusCode int
	Body       string
}

func (e *builderKubeStatusError) Error() string {
	return fmt.Sprintf(
		"kubernetes request %s %s failed: status=%d body=%s",
		e.Method,
		e.APIPath,
		e.StatusCode,
		strings.TrimSpace(e.Body),
	)
}

func acquireBuilderPlacement(ctx context.Context, namespace, jobName string, policy BuilderPodPolicy, profile builderWorkloadProfile, requiredNodeLabels map[string]string) (builderJobPlacement, func(), error) {
	scheduler, err := newBuilderScheduler(namespace, policy, profile, requiredNodeLabels)
	if err != nil {
		return builderJobPlacement{}, nil, err
	}
	placement, reservationName, err := scheduler.reservePlacement(ctx, jobName)
	if err != nil {
		return builderJobPlacement{}, nil, err
	}
	return placement, func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = scheduler.releaseReservation(releaseCtx, reservationName)
	}, nil
}

func newBuilderScheduler(namespace string, policy BuilderPodPolicy, profile builderWorkloadProfile, requiredNodeLabels map[string]string) (*builderScheduler, error) {
	client, err := newBuilderKubeClient(namespace)
	if err != nil {
		return nil, err
	}
	policy = normalizeBuilderPodPolicy(policy)
	demand, err := builderDemandForProfile(policy, profile)
	if err != nil {
		return nil, err
	}
	return &builderScheduler{
		client:                   client,
		namespace:                strings.TrimSpace(namespace),
		policy:                   policy,
		profile:                  profile,
		demand:                   demand,
		requiredNodeLabels:       cloneBuilderStringMap(requiredNodeLabels),
		candidateCount:           policy.CandidateCount,
		selectionTimeout:         time.Duration(policy.SelectionTimeoutSeconds) * time.Second,
		retryInterval:            time.Duration(policy.RetryIntervalSeconds) * time.Second,
		reservationLeaseDuration: time.Duration(policy.ReservationLeaseDurationSecs) * time.Second,
	}, nil
}

func (s *builderScheduler) reservePlacement(ctx context.Context, jobName string) (builderJobPlacement, string, error) {
	deadline := time.Now().Add(s.selectionTimeout)
	lastErr := errNoBuilderPlacement
	for {
		placement, reservationName, err := s.tryReservePlacement(ctx, jobName)
		if err == nil {
			return placement, reservationName, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return builderJobPlacement{}, "", ctx.Err()
		}
		if time.Now().After(deadline) {
			return builderJobPlacement{}, "", lastErr
		}
		timer := time.NewTimer(s.retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return builderJobPlacement{}, "", ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *builderScheduler) tryReservePlacement(ctx context.Context, jobName string) (builderJobPlacement, string, error) {
	snapshots, err := s.loadNodeSnapshots(ctx)
	if err != nil {
		return builderJobPlacement{}, "", err
	}
	now := time.Now()
	reservations, err := s.listActiveReservations(ctx, now)
	if err != nil {
		return builderJobPlacement{}, "", err
	}
	candidates := selectBuilderCandidates(s.policy, s.profile, s.demand, snapshots, reservations, s.requiredNodeLabels)
	if len(candidates) == 0 {
		return builderJobPlacement{}, "", fmt.Errorf("%w for profile %s", errNoBuilderPlacement, s.profile)
	}

	for _, candidate := range candidates {
		acquired, err := s.tryAcquireNodeLock(ctx, candidate.Node.Name, jobName)
		if err != nil {
			return builderJobPlacement{}, "", err
		}
		if !acquired {
			continue
		}

		lockReleased := false
		releaseLock := func() {
			if lockReleased {
				return
			}
			lockReleased = true
			releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = s.releaseNodeLock(releaseCtx, candidate.Node.Name)
		}

		refreshedSnapshots, err := s.loadNodeSnapshots(ctx)
		if err != nil {
			releaseLock()
			return builderJobPlacement{}, "", err
		}
		refreshedReservations, err := s.listActiveReservations(ctx, time.Now())
		if err != nil {
			releaseLock()
			return builderJobPlacement{}, "", err
		}
		refreshedCandidates := selectBuilderCandidates(s.policy, s.profile, s.demand, refreshedSnapshots, refreshedReservations, s.requiredNodeLabels)
		if !builderCandidatesContainNode(refreshedCandidates, candidate.Node.Name) {
			releaseLock()
			continue
		}

		reservationName := builderReservationLeaseName(jobName)
		if err := s.upsertReservation(ctx, reservationName, candidate.Node.Name); err != nil {
			releaseLock()
			return builderJobPlacement{}, "", err
		}
		releaseLock()

		return buildBuilderPlacement(refreshedCandidates, candidate.Node.Name, s.candidateCount), reservationName, nil
	}

	return builderJobPlacement{}, "", fmt.Errorf("%w after lock contention", errNoBuilderPlacement)
}

func (s *builderScheduler) loadNodeSnapshots(ctx context.Context) ([]builderNodeSnapshot, error) {
	nodes, err := s.client.listNodes(ctx)
	if err != nil {
		return nil, err
	}
	summaries := make(map[string]*builderKubeNodeSummary, len(nodes))
	for _, node := range nodes {
		name := strings.TrimSpace(node.Metadata.Name)
		if name == "" {
			continue
		}
		summary, err := s.client.getNodeSummary(ctx, name)
		if err != nil {
			return nil, err
		}
		summaries[name] = summary
	}
	snapshots := make([]builderNodeSnapshot, 0, len(nodes))
	for _, node := range nodes {
		snapshot := builderNodeSnapshot{
			Name:         strings.TrimSpace(node.Metadata.Name),
			Hostname:     strings.TrimSpace(node.Metadata.Labels[builderHostnameLabelKey]),
			Labels:       node.Metadata.Labels,
			Taints:       node.Spec.Taints,
			Ready:        builderNodeConditionStatus(node.Status.Conditions, "Ready"),
			DiskPressure: builderNodeConditionStatus(node.Status.Conditions, "DiskPressure"),
			Allocatable: builderResourceDemand{
				CPUMilli:       parseBuilderCPUMilli(node.Status.Allocatable["cpu"]),
				MemoryBytes:    parseBuilderBytes(node.Status.Allocatable["memory"]),
				EphemeralBytes: parseBuilderBytes(node.Status.Allocatable["ephemeral-storage"]),
			},
			Used: builderResourceDemand{},
		}
		if snapshot.Hostname == "" {
			snapshot.Hostname = snapshot.Name
		}
		if summary := summaries[snapshot.Name]; summary != nil {
			snapshot.Used = builderUsedResources(summary, snapshot.Allocatable.MemoryBytes)
			snapshot.FilesystemAvailableBytes = builderNodeFilesystemAvailableBytes(summary)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func (s *builderScheduler) listActiveReservations(ctx context.Context, now time.Time) ([]builderReservation, error) {
	leases, err := s.client.listLeases(ctx, builderReservationLabelSelector)
	if err != nil {
		return nil, err
	}
	reservations := make([]builderReservation, 0, len(leases))
	for _, lease := range leases {
		expiresAt, ok := builderLeaseExpiry(lease, now)
		if !ok || expiresAt.Before(now) {
			continue
		}
		nodeName := strings.TrimSpace(lease.Metadata.Annotations[builderAnnotationNodeName])
		if nodeName == "" {
			continue
		}
		reservations = append(reservations, builderReservation{
			Name:     strings.TrimSpace(lease.Metadata.Name),
			NodeName: nodeName,
			Demand: builderResourceDemand{
				CPUMilli:       parseBuilderInt64(lease.Metadata.Annotations[builderAnnotationCPUMilli]),
				MemoryBytes:    parseBuilderInt64(lease.Metadata.Annotations[builderAnnotationMemoryBytes]),
				EphemeralBytes: parseBuilderInt64(lease.Metadata.Annotations[builderAnnotationEphemeralBytes]),
			},
			ExpiresAt: expiresAt,
		})
	}
	return reservations, nil
}

func (s *builderScheduler) tryAcquireNodeLock(ctx context.Context, nodeName, holder string) (bool, error) {
	now := time.Now()
	lockName := builderNodeLockLeaseName(nodeName)
	lock, found, err := s.client.getLease(ctx, lockName)
	if err != nil {
		return false, err
	}
	if !found {
		record := newBuilderLease(lockName, builderNodeLockComponentValue, holder, builderNodeLockLeaseDuration, nil, now)
		record.Metadata.Annotations = map[string]string{
			builderAnnotationNodeName: nodeName,
		}
		if err := s.client.createLease(ctx, record); err != nil {
			if builderIsConflictError(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if !builderLeaseIsExpired(lock, now) && !strings.EqualFold(strings.TrimSpace(lock.Spec.HolderIdentity), strings.TrimSpace(holder)) {
		return false, nil
	}
	lock.Spec.HolderIdentity = holder
	lock.Spec.LeaseDurationSeconds = int(builderNodeLockLeaseDuration.Seconds())
	lock.Spec.AcquireTime = formatKubeTimestamp(now)
	lock.Spec.RenewTime = formatKubeTimestamp(now)
	if lock.Metadata.Labels == nil {
		lock.Metadata.Labels = map[string]string{}
	}
	lock.Metadata.Labels["app.kubernetes.io/managed-by"] = "fugue"
	lock.Metadata.Labels["app.kubernetes.io/component"] = builderNodeLockComponentValue
	if lock.Metadata.Annotations == nil {
		lock.Metadata.Annotations = map[string]string{}
	}
	lock.Metadata.Annotations[builderAnnotationNodeName] = nodeName
	if err := s.client.updateLease(ctx, lock); err != nil {
		if builderIsConflictError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *builderScheduler) releaseNodeLock(ctx context.Context, nodeName string) error {
	return s.client.deleteLease(ctx, builderNodeLockLeaseName(nodeName))
}

func (s *builderScheduler) upsertReservation(ctx context.Context, reservationName, nodeName string) error {
	now := time.Now()
	annotations := map[string]string{
		builderAnnotationNodeName:       nodeName,
		builderAnnotationCPUMilli:       strconv.FormatInt(s.demand.CPUMilli, 10),
		builderAnnotationMemoryBytes:    strconv.FormatInt(s.demand.MemoryBytes, 10),
		builderAnnotationEphemeralBytes: strconv.FormatInt(s.demand.EphemeralBytes, 10),
	}
	lease, found, err := s.client.getLease(ctx, reservationName)
	if err != nil {
		return err
	}
	if !found {
		record := newBuilderLease(reservationName, builderReservationComponentValue, reservationName, s.reservationLeaseDuration, annotations, now)
		return s.client.createLease(ctx, record)
	}
	lease.Spec.HolderIdentity = reservationName
	lease.Spec.LeaseDurationSeconds = int(s.reservationLeaseDuration.Seconds())
	lease.Spec.AcquireTime = formatKubeTimestamp(now)
	lease.Spec.RenewTime = formatKubeTimestamp(now)
	if lease.Metadata.Labels == nil {
		lease.Metadata.Labels = map[string]string{}
	}
	lease.Metadata.Labels["app.kubernetes.io/managed-by"] = "fugue"
	lease.Metadata.Labels["app.kubernetes.io/component"] = builderReservationComponentValue
	lease.Metadata.Annotations = annotations
	return s.client.updateLease(ctx, lease)
}

func (s *builderScheduler) releaseReservation(ctx context.Context, reservationName string) error {
	return s.client.deleteLease(ctx, reservationName)
}

func selectBuilderCandidates(policy BuilderPodPolicy, profile builderWorkloadProfile, demand builderResourceDemand, snapshots []builderNodeSnapshot, reservations []builderReservation, requiredNodeLabels map[string]string) []builderCandidate {
	policy = normalizeBuilderPodPolicy(policy)
	reservedByNode := make(map[string]builderResourceDemand)
	for _, reservation := range reservations {
		current := reservedByNode[reservation.NodeName]
		current.CPUMilli += reservation.Demand.CPUMilli
		current.MemoryBytes += reservation.Demand.MemoryBytes
		current.EphemeralBytes += reservation.Demand.EphemeralBytes
		reservedByNode[reservation.NodeName] = current
	}

	candidates := make([]builderCandidate, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if !snapshot.Ready || snapshot.DiskPressure || snapshot.Hostname == "" {
			continue
		}
		if !builderNodeEligibleForBuilders(policy, snapshot) {
			continue
		}
		if !builderNodeMatchesRequiredLabels(snapshot, requiredNodeLabels) {
			continue
		}
		sizeClass := builderNodeSizeClass(policy, snapshot)
		if profile == builderWorkloadProfileHeavy && sizeClass == policy.SmallNodeLabelValue {
			continue
		}
		available := builderAvailableResources(
			snapshot,
			reservedByNode[snapshot.Name],
			builderSafetyBuffer(snapshot.Allocatable, profile),
		)
		if !builderDemandFits(available, demand) {
			continue
		}
		remaining := builderSubtractDemand(available, demand)
		candidates = append(candidates, builderCandidate{
			Node:      snapshot,
			Remaining: remaining,
			Score:     builderCandidateScore(policy, profile, sizeClass, snapshot.Allocatable, remaining),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		if candidates[i].Remaining.EphemeralBytes != candidates[j].Remaining.EphemeralBytes {
			return candidates[i].Remaining.EphemeralBytes > candidates[j].Remaining.EphemeralBytes
		}
		if candidates[i].Remaining.MemoryBytes != candidates[j].Remaining.MemoryBytes {
			return candidates[i].Remaining.MemoryBytes > candidates[j].Remaining.MemoryBytes
		}
		if candidates[i].Remaining.CPUMilli != candidates[j].Remaining.CPUMilli {
			return candidates[i].Remaining.CPUMilli > candidates[j].Remaining.CPUMilli
		}
		return candidates[i].Node.Name < candidates[j].Node.Name
	})
	if len(candidates) > policy.CandidateCount {
		candidates = candidates[:policy.CandidateCount]
	}
	return candidates
}

func buildBuilderPlacement(candidates []builderCandidate, selectedNodeName string, candidateCount int) builderJobPlacement {
	if candidateCount <= 0 {
		candidateCount = len(candidates)
	}
	ordered := make([]string, 0, candidateCount)
	preferred := ""
	appendHostname := func(hostname string) {
		if hostname == "" {
			return
		}
		for _, existing := range ordered {
			if existing == hostname {
				return
			}
		}
		ordered = append(ordered, hostname)
	}
	for _, candidate := range candidates {
		if candidate.Node.Name == selectedNodeName {
			preferred = candidate.Node.Hostname
			appendHostname(candidate.Node.Hostname)
			break
		}
	}
	for _, candidate := range candidates {
		appendHostname(candidate.Node.Hostname)
		if len(ordered) >= candidateCount {
			break
		}
	}
	return builderJobPlacement{
		CandidateHostnames: ordered,
		PreferredHostname:  preferred,
	}
}

func builderNodeMatchesRequiredLabels(snapshot builderNodeSnapshot, requiredNodeLabels map[string]string) bool {
	for key, expected := range requiredNodeLabels {
		key = strings.TrimSpace(key)
		expected = strings.TrimSpace(expected)
		if key == "" || expected == "" {
			continue
		}
		if actual := strings.TrimSpace(snapshot.Labels[key]); actual != expected {
			return false
		}
	}
	return true
}

func builderCandidatesContainNode(candidates []builderCandidate, nodeName string) bool {
	for _, candidate := range candidates {
		if candidate.Node.Name == nodeName {
			return true
		}
	}
	return false
}

func builderDemandForProfile(policy BuilderPodPolicy, profile builderWorkloadProfile) (builderResourceDemand, error) {
	policy = normalizeBuilderPodPolicy(policy)
	workload := policy.Light
	if profile == builderWorkloadProfileHeavy {
		workload = policy.Heavy
	}
	return builderResourceDemand{
		CPUMilli: builderSchedulingDemand(
			parseBuilderCPUMilli(workload.Resources.Requests["cpu"]),
			parseBuilderCPUMilli(workload.Resources.Limits["cpu"]),
		),
		MemoryBytes: builderSchedulingDemand(
			parseBuilderBytes(workload.Resources.Requests["memory"]),
			parseBuilderBytes(workload.Resources.Limits["memory"]),
		),
		EphemeralBytes: builderSchedulingDemand(
			parseBuilderBytes(workload.Resources.Requests["ephemeral-storage"]),
			parseBuilderBytes(workload.Resources.Limits["ephemeral-storage"]),
		),
	}, nil
}

func builderSchedulingDemand(request, limit int64) int64 {
	if request > 0 {
		return request
	}
	return limit
}

func builderSafetyBuffer(allocatable builderResourceDemand, profile builderWorkloadProfile) builderResourceDemand {
	percent := builderLightBufferPercent
	ephemeralMin := builderLightEphemeralBufferBytes
	memoryMin := builderLightMemoryBufferBytes
	cpuMin := builderLightCPUBufferMilli
	if profile == builderWorkloadProfileHeavy {
		percent = builderHeavyBufferPercent
		ephemeralMin = builderHeavyEphemeralBufferBytes
		memoryMin = builderHeavyMemoryBufferBytes
		cpuMin = builderHeavyCPUBufferMilli
	}
	return builderResourceDemand{
		CPUMilli:       maxInt64(cpuMin, int64(math.Round(float64(allocatable.CPUMilli)*percent))),
		MemoryBytes:    maxInt64(memoryMin, int64(math.Round(float64(allocatable.MemoryBytes)*percent))),
		EphemeralBytes: maxInt64(ephemeralMin, int64(math.Round(float64(allocatable.EphemeralBytes)*percent))),
	}
}

func builderCandidateScore(policy BuilderPodPolicy, profile builderWorkloadProfile, sizeClass string, allocatable, remaining builderResourceDemand) float64 {
	sizeWeight := 0.0
	switch profile {
	case builderWorkloadProfileHeavy:
		switch sizeClass {
		case policy.LargeNodeLabelValue:
			sizeWeight = 120
		case policy.MediumNodeLabelValue, "":
			sizeWeight = 70
		default:
			sizeWeight = 20
		}
	default:
		switch sizeClass {
		case policy.SmallNodeLabelValue:
			sizeWeight = 120
		case policy.MediumNodeLabelValue, "":
			sizeWeight = 80
		case policy.LargeNodeLabelValue:
			sizeWeight = 20
		default:
			sizeWeight = 50
		}
	}
	return sizeWeight +
		500*builderRatioScore(remaining.EphemeralBytes, allocatable.EphemeralBytes) +
		250*builderRatioScore(remaining.MemoryBytes, allocatable.MemoryBytes) +
		150*builderRatioScore(remaining.CPUMilli, allocatable.CPUMilli)
}

func builderRatioScore(remaining, total int64) float64 {
	if total <= 0 {
		return 0
	}
	if remaining <= 0 {
		return 0
	}
	return float64(remaining) / float64(total)
}

func builderAnyNodeMatchesBuildLabel(policy BuilderPodPolicy, snapshots []builderNodeSnapshot) bool {
	for _, snapshot := range snapshots {
		if !snapshot.Ready || snapshot.DiskPressure || snapshot.Hostname == "" {
			continue
		}
		if !builderNodeTaintsTolerated(policy.Tolerations, snapshot.Taints) {
			continue
		}
		if builderMatchesBuildPool(policy, snapshot) {
			return true
		}
	}
	return false
}

func builderNodeEligibleForBuilders(policy BuilderPodPolicy, snapshot builderNodeSnapshot) bool {
	if !builderNodeTaintsTolerated(policy.Tolerations, snapshot.Taints) {
		return false
	}
	if builderMatchesBuildPool(policy, snapshot) {
		return true
	}
	return builderNodeIsSharedBuilderCandidate(snapshot)
}

func builderNodeIsSharedBuilderCandidate(snapshot builderNodeSnapshot) bool {
	if strings.TrimSpace(snapshot.Labels[runtime.TenantIDLabelKey]) != "" {
		return false
	}
	switch strings.TrimSpace(snapshot.Labels[runtime.NodeModeLabelKey]) {
	case "", model.RuntimeTypeManagedShared:
		return true
	default:
		return false
	}
}

func builderNodeTaintsTolerated(tolerations []BuilderToleration, taints []builderKubeNodeTaint) bool {
	tolerations = normalizeBuilderTolerations(tolerations)
	for _, taint := range taints {
		if !builderIsHardNodeTaintEffect(taint.Effect) {
			continue
		}
		if !builderToleratesNodeTaint(tolerations, taint) {
			return false
		}
	}
	return true
}

func builderIsHardNodeTaintEffect(effect string) bool {
	return strings.EqualFold(strings.TrimSpace(effect), "NoSchedule") ||
		strings.EqualFold(strings.TrimSpace(effect), "NoExecute")
}

func builderToleratesNodeTaint(tolerations []BuilderToleration, taint builderKubeNodeTaint) bool {
	for _, toleration := range tolerations {
		if builderTolerationMatchesNodeTaint(toleration, taint) {
			return true
		}
	}
	return false
}

func builderTolerationMatchesNodeTaint(toleration BuilderToleration, taint builderKubeNodeTaint) bool {
	effect := strings.TrimSpace(toleration.Effect)
	if effect != "" && !strings.EqualFold(effect, strings.TrimSpace(taint.Effect)) {
		return false
	}

	operator := strings.TrimSpace(toleration.Operator)
	if operator == "" {
		operator = "Equal"
	}
	switch {
	case strings.EqualFold(operator, "Exists"):
		key := strings.TrimSpace(toleration.Key)
		return key == "" || strings.EqualFold(key, strings.TrimSpace(taint.Key))
	default:
		return strings.EqualFold(strings.TrimSpace(toleration.Key), strings.TrimSpace(taint.Key)) &&
			strings.EqualFold(strings.TrimSpace(toleration.Value), strings.TrimSpace(taint.Value))
	}
}

func builderMatchesBuildPool(policy BuilderPodPolicy, snapshot builderNodeSnapshot) bool {
	key := strings.TrimSpace(policy.BuildNodeLabelKey)
	if key == "" {
		return true
	}
	value, ok := snapshot.Labels[key]
	if !ok {
		return false
	}
	expected := strings.TrimSpace(policy.BuildNodeLabelValue)
	if expected == "" {
		return strings.TrimSpace(value) != ""
	}
	return strings.EqualFold(strings.TrimSpace(value), expected)
}

func builderNodeSizeClass(policy BuilderPodPolicy, snapshot builderNodeSnapshot) string {
	key := strings.TrimSpace(policy.LargeNodeLabelKey)
	if key == "" {
		return ""
	}
	return strings.TrimSpace(snapshot.Labels[key])
}

func builderDemandFits(available, demand builderResourceDemand) bool {
	return available.CPUMilli >= demand.CPUMilli &&
		available.MemoryBytes >= demand.MemoryBytes &&
		available.EphemeralBytes >= demand.EphemeralBytes
}

func builderAvailableResources(snapshot builderNodeSnapshot, reserved, buffer builderResourceDemand) builderResourceDemand {
	available := builderSubtractDemand(snapshot.Allocatable, snapshot.Used, reserved, buffer)
	if snapshot.FilesystemAvailableBytes > 0 {
		filesystemAvailable := snapshot.FilesystemAvailableBytes - reserved.EphemeralBytes - buffer.EphemeralBytes
		available.EphemeralBytes = minInt64(available.EphemeralBytes, filesystemAvailable)
	}
	return available
}

func builderSubtractDemand(parts ...builderResourceDemand) builderResourceDemand {
	if len(parts) == 0 {
		return builderResourceDemand{}
	}
	out := parts[0]
	for _, part := range parts[1:] {
		out.CPUMilli -= part.CPUMilli
		out.MemoryBytes -= part.MemoryBytes
		out.EphemeralBytes -= part.EphemeralBytes
	}
	return out
}

func builderUsedResources(summary *builderKubeNodeSummary, memoryCapacity int64) builderResourceDemand {
	used := builderResourceDemand{}
	if summary == nil {
		return used
	}
	if summary.Node.CPU.UsageNanoCores != nil {
		used.CPUMilli = int64(math.Round(float64(*summary.Node.CPU.UsageNanoCores) / 1_000_000))
	}
	switch {
	case summary.Node.Memory.WorkingSetBytes != nil:
		used.MemoryBytes = int64(*summary.Node.Memory.WorkingSetBytes)
	case summary.Node.Memory.UsageBytes != nil:
		used.MemoryBytes = int64(*summary.Node.Memory.UsageBytes)
	case summary.Node.Memory.AvailableBytes != nil && memoryCapacity > 0 && *summary.Node.Memory.AvailableBytes <= uint64(memoryCapacity):
		used.MemoryBytes = memoryCapacity - int64(*summary.Node.Memory.AvailableBytes)
	}
	used.EphemeralBytes = builderPodEphemeralUsageBytes(summary)
	return used
}

func builderPodEphemeralUsageBytes(summary *builderKubeNodeSummary) int64 {
	if summary == nil || len(summary.Pods) == 0 {
		return 0
	}
	var total int64
	for _, pod := range summary.Pods {
		if pod.EphemeralStorage.UsedBytes == nil {
			continue
		}
		total += int64(*pod.EphemeralStorage.UsedBytes)
	}
	return total
}

func builderNodeFilesystemAvailableBytes(summary *builderKubeNodeSummary) int64 {
	if summary == nil {
		return 0
	}
	switch {
	case summary.Node.FS.AvailableBytes != nil:
		if summary.Node.FS.CapacityBytes != nil && *summary.Node.FS.AvailableBytes > *summary.Node.FS.CapacityBytes {
			return 0
		}
		return int64(*summary.Node.FS.AvailableBytes)
	case summary.Node.FS.CapacityBytes != nil && summary.Node.FS.UsedBytes != nil && *summary.Node.FS.UsedBytes <= *summary.Node.FS.CapacityBytes:
		return int64(*summary.Node.FS.CapacityBytes - *summary.Node.FS.UsedBytes)
	default:
		return 0
	}
}

func builderNodeConditionStatus(conditions []builderKubeNodeCondition, conditionType string) bool {
	for _, condition := range conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), strings.TrimSpace(conditionType)) {
			return strings.EqualFold(strings.TrimSpace(condition.Status), "True")
		}
	}
	return false
}

func parseBuilderCPUMilli(value string) int64 {
	quantity, err := resource.ParseQuantity(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return quantity.MilliValue()
}

func parseBuilderBytes(value string) int64 {
	quantity, err := resource.ParseQuantity(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return quantity.Value()
}

func parseBuilderInt64(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func builderReservationLeaseName(jobName string) string {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return "builder-reservation"
	}
	if len(jobName) <= 63 {
		return jobName
	}
	return jobName[:63]
}

func builderNodeLockLeaseName(nodeName string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(nodeName)))
	hash := hex.EncodeToString(sum[:])[:10]
	name := "builder-lock-" + hash
	if len(name) > 63 {
		name = name[:63]
	}
	return strings.TrimRight(name, "-")
}

func newBuilderLease(name, component, holder string, duration time.Duration, annotations map[string]string, now time.Time) builderKubeLease {
	var lease builderKubeLease
	lease.APIVersion = "coordination.k8s.io/v1"
	lease.Kind = "Lease"
	lease.Metadata.Name = name
	lease.Metadata.Labels = map[string]string{
		"app.kubernetes.io/managed-by": "fugue",
		"app.kubernetes.io/component":  component,
	}
	if len(annotations) > 0 {
		lease.Metadata.Annotations = annotations
	}
	lease.Spec.HolderIdentity = holder
	lease.Spec.LeaseDurationSeconds = int(duration.Seconds())
	lease.Spec.AcquireTime = formatKubeTimestamp(now)
	lease.Spec.RenewTime = formatKubeTimestamp(now)
	return lease
}

func builderLeaseIsExpired(lease builderKubeLease, now time.Time) bool {
	expiresAt, ok := builderLeaseExpiry(lease, now)
	if !ok {
		return true
	}
	return expiresAt.Before(now)
}

func builderLeaseExpiry(lease builderKubeLease, now time.Time) (time.Time, bool) {
	renewedAt := strings.TrimSpace(lease.Spec.RenewTime)
	if renewedAt == "" {
		renewedAt = strings.TrimSpace(lease.Spec.AcquireTime)
	}
	if renewedAt == "" || lease.Spec.LeaseDurationSeconds <= 0 {
		return time.Time{}, false
	}
	parsed := parseKubeTimestamp(renewedAt)
	if parsed.IsZero() {
		return time.Time{}, false
	}
	return parsed.Add(time.Duration(lease.Spec.LeaseDurationSeconds) * time.Second), true
}

func parseKubeTimestamp(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func formatKubeTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
}

func builderIsConflictError(err error) bool {
	var statusErr *builderKubeStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict
}

func newBuilderKubeClient(namespace string) (*builderKubeClient, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, fmt.Errorf("kubernetes service host/port is not available in the environment")
	}

	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("load service account CA")
	}

	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		if data, err := os.ReadFile(serviceAccountNamespacePath); err == nil {
			namespace = strings.TrimSpace(string(data))
		}
	}
	if namespace == "" {
		return nil, fmt.Errorf("resolve kubernetes namespace")
	}

	return &builderKubeClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: rootCAs},
			},
			Timeout: 15 * time.Second,
		},
		baseURL:     "https://" + host + ":" + port,
		bearerToken: strings.TrimSpace(string(token)),
		namespace:   namespace,
	}, nil
}

func (c *builderKubeClient) effectiveNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace != "" {
		return namespace
	}
	return c.namespace
}

func (c *builderKubeClient) listNodes(ctx context.Context) ([]builderKubeNode, error) {
	var nodeList builderKubeNodeList
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes", nil, &nodeList); err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

func (c *builderKubeClient) getNodeSummary(ctx context.Context, nodeName string) (*builderKubeNodeSummary, error) {
	var summary builderKubeNodeSummary
	apiPath := "/api/v1/nodes/" + url.PathEscape(strings.TrimSpace(nodeName)) + "/proxy/stats/summary"
	if err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

func (c *builderKubeClient) listLeases(ctx context.Context, labelSelector string) ([]builderKubeLease, error) {
	query := url.Values{}
	if strings.TrimSpace(labelSelector) != "" {
		query.Set("labelSelector", strings.TrimSpace(labelSelector))
	}
	apiPath := "/apis/coordination.k8s.io/v1/namespaces/" + c.namespace + "/leases"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	var leaseList builderKubeLeaseList
	if err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &leaseList); err != nil {
		return nil, err
	}
	return leaseList.Items, nil
}

func (c *builderKubeClient) getLease(ctx context.Context, name string) (builderKubeLease, bool, error) {
	var lease builderKubeLease
	status, err := c.doJSONStatus(ctx, http.MethodGet, "/apis/coordination.k8s.io/v1/namespaces/"+c.namespace+"/leases/"+url.PathEscape(strings.TrimSpace(name)), nil, &lease)
	if status == http.StatusNotFound {
		return builderKubeLease{}, false, nil
	}
	if err != nil {
		return builderKubeLease{}, false, err
	}
	return lease, true, nil
}

func (c *builderKubeClient) createLease(ctx context.Context, lease builderKubeLease) error {
	return c.doJSON(ctx, http.MethodPost, "/apis/coordination.k8s.io/v1/namespaces/"+c.namespace+"/leases", lease, nil)
}

func (c *builderKubeClient) updateLease(ctx context.Context, lease builderKubeLease) error {
	return c.doJSON(ctx, http.MethodPut, "/apis/coordination.k8s.io/v1/namespaces/"+c.namespace+"/leases/"+url.PathEscape(strings.TrimSpace(lease.Metadata.Name)), lease, nil)
}

func (c *builderKubeClient) deleteLease(ctx context.Context, name string) error {
	status, err := c.doJSONStatus(ctx, http.MethodDelete, "/apis/coordination.k8s.io/v1/namespaces/"+c.namespace+"/leases/"+url.PathEscape(strings.TrimSpace(name)), nil, nil)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}

func (c *builderKubeClient) doJSON(ctx context.Context, method, apiPath string, body any, out any) error {
	_, err := c.doJSONStatus(ctx, method, apiPath, body, out)
	return err
}

func (c *builderKubeClient) doJSONStatus(ctx context.Context, method, apiPath string, body any, out any) (int, error) {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshal kubernetes request: %w", err)
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, payload)
	if err != nil {
		return 0, fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, &builderKubeStatusError{
			Method:     method,
			APIPath:    apiPath,
			StatusCode: resp.StatusCode,
			Body:       string(responseBody),
		}
	}
	if out != nil && len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
