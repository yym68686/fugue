package sourceimport

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func InspectBuilderPlacement(ctx context.Context, namespace string, policy BuilderPodPolicy, buildStrategy string, stateful bool, requiredNodeLabels map[string]string) (model.BuilderPlacementInspection, error) {
	return InspectBuilderPlacementForProfile(ctx, namespace, policy, "", buildStrategy, stateful, requiredNodeLabels)
}

func InspectBuilderPlacementForProfile(ctx context.Context, namespace string, policy BuilderPodPolicy, profile, buildStrategy string, stateful bool, requiredNodeLabels map[string]string) (model.BuilderPlacementInspection, error) {
	resolvedProfile := normalizeBuilderInspectionProfile(profile, buildStrategy, stateful)
	scheduler, err := newBuilderScheduler(namespace, policy, resolvedProfile, requiredNodeLabels)
	if err != nil {
		return model.BuilderPlacementInspection{}, err
	}
	return scheduler.inspectPlacement(ctx, buildStrategy)
}

func normalizeBuilderInspectionProfile(profile, buildStrategy string, stateful bool) builderWorkloadProfile {
	switch strings.TrimSpace(strings.ToLower(profile)) {
	case string(builderWorkloadProfileHeavy):
		return builderWorkloadProfileHeavy
	case string(builderWorkloadProfileLight):
		return builderWorkloadProfileLight
	default:
		return builderWorkloadProfileFor(buildStrategy, stateful)
	}
}

func (s *builderScheduler) inspectPlacement(ctx context.Context, buildStrategy string) (model.BuilderPlacementInspection, error) {
	report := model.BuilderPlacementInspection{
		Profile:            string(s.profile),
		BuildStrategy:      strings.TrimSpace(buildStrategy),
		RequiredNodeLabels: cloneBuilderStringMap(s.requiredNodeLabels),
		Demand:             builderResourceSnapshot(s.demand),
	}

	nodes, err := s.client.listNodes(ctx)
	if err != nil {
		return report, err
	}
	snapshots := make([]builderNodeSnapshot, 0, len(nodes))
	summaryErrors := make(map[string]string)
	for _, node := range nodes {
		snapshot := builderSnapshotFromKubeNode(node)
		if s.nodeNeedsSummary(snapshot) {
			summary, summaryErr := s.client.getNodeSummary(ctx, snapshot.Name)
			if summaryErr != nil {
				summaryErrors[snapshot.Name] = fmt.Sprintf("stats summary unavailable: %v", summaryErr)
			} else {
				snapshot.Used = builderUsedResources(summary, snapshot.Allocatable.MemoryBytes)
				snapshot.FilesystemAvailableBytes = builderNodeFilesystemAvailableBytes(summary)
				snapshot.SummaryLoaded = true
			}
		}
		snapshots = append(snapshots, snapshot)
	}

	now := time.Now()
	reservations, err := s.listActiveReservations(ctx, now)
	if err != nil {
		return report, err
	}
	locks, err := s.listActiveNodeLocks(ctx, now)
	if err != nil {
		return report, err
	}

	report.Reservations = builderReservationInspections(reservations)
	report.Locks = builderLockInspections(locks)

	schedulingSnapshots := builderInspectionSchedulingSnapshots(s, snapshots, summaryErrors)
	rankedCandidates := sortedBuilderCandidates(s.policy, s.profile, s.demand, schedulingSnapshots, reservations, s.requiredNodeLabels)
	rankByNode := make(map[string]int, len(rankedCandidates))
	for index, candidate := range rankedCandidates {
		rankByNode[candidate.Node.Name] = index + 1
	}
	reservedByNode := builderReservationsByNode(reservations)

	report.Nodes = make([]model.BuilderPlacementNodeInspection, 0, len(snapshots))
	for _, snapshot := range snapshots {
		reasons := builderNodeInspectionReasons(s.policy, snapshot, s.requiredNodeLabels, summaryErrors[snapshot.Name])
		inspection := model.BuilderPlacementNodeInspection{
			NodeName:     strings.TrimSpace(snapshot.Name),
			Hostname:     strings.TrimSpace(snapshot.Hostname),
			NodeMode:     strings.TrimSpace(snapshot.Labels[runtime.NodeModeLabelKey]),
			Ready:        snapshot.Ready,
			DiskPressure: snapshot.DiskPressure,
			Eligible:     len(reasons) == 0,
			Rank:         rankByNode[strings.TrimSpace(snapshot.Name)],
			Reasons:      reasons,
			Allocatable:  builderResourceSnapshot(snapshot.Allocatable),
		}
		if snapshot.SummaryLoaded {
			reserved := reservedByNode[snapshot.Name]
			buffer := builderSafetyBuffer(snapshot.Allocatable, s.profile)
			available := builderAvailableResources(snapshot, reserved, buffer)
			inspection.Used = builderResourceSnapshot(snapshot.Used)
			inspection.Reserved = builderResourceSnapshot(reserved)
			inspection.SafetyBuffer = builderResourceSnapshot(buffer)
			inspection.Available = builderResourceSnapshot(available)
			inspection.Remaining = builderResourceSnapshot(builderSubtractDemand(available, s.demand))
		}
		report.Nodes = append(report.Nodes, inspection)
	}

	sort.Slice(report.Nodes, func(i, j int) bool {
		left := report.Nodes[i]
		right := report.Nodes[j]
		switch {
		case left.Rank > 0 && right.Rank > 0 && left.Rank != right.Rank:
			return left.Rank < right.Rank
		case left.Rank > 0 && right.Rank == 0:
			return true
		case left.Rank == 0 && right.Rank > 0:
			return false
		default:
			return left.NodeName < right.NodeName
		}
	})

	return report, nil
}

func builderInspectionSchedulingSnapshots(s *builderScheduler, snapshots []builderNodeSnapshot, summaryErrors map[string]string) []builderNodeSnapshot {
	out := make([]builderNodeSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if summaryErrors[snapshot.Name] != "" && s.nodeNeedsSummary(snapshot) {
			continue
		}
		out = append(out, snapshot)
	}
	return out
}

func builderReservationInspections(reservations []builderReservation) []model.BuilderPlacementReservationInspection {
	out := make([]model.BuilderPlacementReservationInspection, 0, len(reservations))
	for _, reservation := range reservations {
		out = append(out, model.BuilderPlacementReservationInspection{
			Name:      strings.TrimSpace(reservation.Name),
			NodeName:  strings.TrimSpace(reservation.NodeName),
			RenewedAt: builderTimePtr(reservation.RenewedAt),
			ExpiresAt: builderTimePtr(reservation.ExpiresAt),
			Demand:    builderResourceSnapshot(reservation.Demand),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeName != out[j].NodeName {
			return out[i].NodeName < out[j].NodeName
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func builderLockInspections(locks []builderNodeLock) []model.BuilderPlacementLockInspection {
	out := make([]model.BuilderPlacementLockInspection, 0, len(locks))
	for _, lock := range locks {
		out = append(out, model.BuilderPlacementLockInspection{
			Name:           strings.TrimSpace(lock.Name),
			NodeName:       strings.TrimSpace(lock.NodeName),
			HolderIdentity: strings.TrimSpace(lock.HolderIdentity),
			RenewedAt:      builderTimePtr(lock.RenewedAt),
			ExpiresAt:      builderTimePtr(lock.ExpiresAt),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeName != out[j].NodeName {
			return out[i].NodeName < out[j].NodeName
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func builderReservationsByNode(reservations []builderReservation) map[string]builderResourceDemand {
	out := make(map[string]builderResourceDemand)
	for _, reservation := range reservations {
		current := out[reservation.NodeName]
		current.CPUMilli += reservation.Demand.CPUMilli
		current.MemoryBytes += reservation.Demand.MemoryBytes
		current.EphemeralBytes += reservation.Demand.EphemeralBytes
		out[reservation.NodeName] = current
	}
	return out
}

func builderNodeInspectionReasons(policy BuilderPodPolicy, snapshot builderNodeSnapshot, requiredNodeLabels map[string]string, summaryErr string) []string {
	reasons := []string{}
	if !snapshot.Ready {
		reasons = append(reasons, "Ready=False")
	}
	if snapshot.DiskPressure {
		reasons = append(reasons, "DiskPressure=True")
	}
	if strings.TrimSpace(snapshot.Hostname) == "" {
		reasons = append(reasons, fmt.Sprintf("missing %s label", builderHostnameLabelKey))
	}
	reasons = append(reasons, builderUntoleratedTaintReasons(policy.Tolerations, snapshot.Taints)...)
	reasons = append(reasons, builderRequiredLabelReasons(snapshot, requiredNodeLabels)...)
	if !builderNodePoolEligible(policy, snapshot) {
		reasons = append(reasons, builderPoolEligibilityReasons(policy, snapshot)...)
	}
	if trimmed := strings.TrimSpace(summaryErr); trimmed != "" {
		reasons = append(reasons, trimmed)
	}
	return appendUniqueBuilderReasons(reasons)
}

func builderUntoleratedTaintReasons(tolerations []BuilderToleration, taints []builderKubeNodeTaint) []string {
	reasons := []string{}
	tolerations = normalizeBuilderTolerations(tolerations)
	for _, taint := range taints {
		if !builderIsHardNodeTaintEffect(taint.Effect) {
			continue
		}
		if builderToleratesNodeTaint(tolerations, taint) {
			continue
		}
		label := strings.TrimSpace(taint.Key)
		if value := strings.TrimSpace(taint.Value); value != "" {
			label += "=" + value
		}
		if effect := strings.TrimSpace(taint.Effect); effect != "" {
			label += ":" + effect
		}
		reasons = append(reasons, "untolerated taint "+label)
	}
	return reasons
}

func builderRequiredLabelReasons(snapshot builderNodeSnapshot, requiredNodeLabels map[string]string) []string {
	if len(requiredNodeLabels) == 0 {
		return nil
	}
	reasons := make([]string, 0, len(requiredNodeLabels))
	for key, expected := range requiredNodeLabels {
		key = strings.TrimSpace(key)
		expected = strings.TrimSpace(expected)
		if key == "" || expected == "" {
			continue
		}
		actual, ok := snapshot.Labels[key]
		switch {
		case !ok:
			reasons = append(reasons, fmt.Sprintf("required label %s=%q is missing", key, expected))
		case strings.TrimSpace(actual) != expected:
			reasons = append(reasons, fmt.Sprintf("required label %s=%q does not match actual %q", key, expected, strings.TrimSpace(actual)))
		}
	}
	sort.Strings(reasons)
	return reasons
}

func builderNodePoolEligible(policy BuilderPodPolicy, snapshot builderNodeSnapshot) bool {
	if builderNodeInSharedPool(snapshot) {
		return true
	}
	if builderMatchesBuildPool(policy, snapshot) {
		return true
	}
	return builderNodeIsSharedBuilderCandidate(snapshot)
}

func builderPoolEligibilityReasons(policy BuilderPodPolicy, snapshot builderNodeSnapshot) []string {
	reasons := []string{}
	buildKey := strings.TrimSpace(policy.BuildNodeLabelKey)
	buildExpected := strings.TrimSpace(policy.BuildNodeLabelValue)
	buildActual, buildLabelPresent := snapshot.Labels[buildKey]
	if buildKey != "" && !builderNodeInSharedPool(snapshot) {
		switch {
		case !buildLabelPresent || strings.TrimSpace(buildActual) == "":
			if buildExpected != "" {
				reasons = append(reasons, fmt.Sprintf("build label %s=%q is missing", buildKey, buildExpected))
			} else {
				reasons = append(reasons, fmt.Sprintf("build label %s is missing", buildKey))
			}
		case buildExpected != "" && !strings.EqualFold(strings.TrimSpace(buildActual), buildExpected):
			reasons = append(reasons, fmt.Sprintf("build label %s=%q does not match expected %q", buildKey, strings.TrimSpace(buildActual), buildExpected))
		}
	}
	if tenant := strings.TrimSpace(snapshot.Labels[runtime.TenantIDLabelKey]); tenant != "" {
		reasons = append(reasons, fmt.Sprintf("dedicated tenant label %s=%q", runtime.TenantIDLabelKey, tenant))
	}
	if mode := strings.TrimSpace(snapshot.Labels[runtime.NodeModeLabelKey]); mode != "" && mode != model.RuntimeTypeManagedShared {
		reasons = append(reasons, fmt.Sprintf("node mode %q is not shared-builder compatible", mode))
	}
	if !builderNodeInSharedPool(snapshot) && len(reasons) == 0 {
		reasons = append(reasons, "node is not in the build or shared pool")
	}
	return appendUniqueBuilderReasons(reasons)
}

func appendUniqueBuilderReasons(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func builderResourceSnapshot(demand builderResourceDemand) model.BuilderResourceSnapshot {
	return model.BuilderResourceSnapshot{
		CPUMilli:       demand.CPUMilli,
		MemoryBytes:    demand.MemoryBytes,
		EphemeralBytes: demand.EphemeralBytes,
	}
}

func builderTimePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}
