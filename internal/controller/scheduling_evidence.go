package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	runtimepkg "fugue/internal/runtime"
)

type schedulingOperationKey struct{}
type schedulingObservationKey struct{}

type schedulingObservation struct {
	decision                  model.SchedulingDecision
	diagnostics               map[string]any
	nodes                     []map[string]any
	resourceInventoryObserved bool
}

type schedulingObservationError struct{ cause error }

func (e schedulingObservationError) Error() string {
	return "scheduling observation failed: required placement inputs could not be read; inspect this operation's evidence"
}
func (e schedulingObservationError) Unwrap() error { return e.cause }

func newSchedulingObservation(app model.App, stage string) *schedulingObservation {
	policy := managedAppPlacementPolicy(app)
	return &schedulingObservation{
		decision: model.SchedulingDecision{
			ObservedAt: time.Now().UTC(), Stage: stage,
			Demand:             schedulingResources(managedAppPlacementRequest(app)),
			MemoryRequestRatio: policy.memoryRequestRatio, CPUOvercommitRatio: policy.cpuOvercommitRatio,
			RequiredPodAddresses: int64(app.Spec.Replicas),
		},
		diagnostics: map[string]any{"runtime_id": app.Spec.RuntimeID},
	}
}

func schedulingResources(r managedSharedNodeRequests) model.SchedulingResources {
	return model.SchedulingResources{CPUMilli: r.cpuMilli, MemoryBytes: r.memoryBytes, EphemeralBytes: r.ephemeralBytes}
}

func (o *schedulingObservation) storage(p *storagePlacement) {
	if p == nil {
		return
	}
	var details []map[string]any
	for i, c := range p.claims {
		v := model.SchedulingVolumeDecision{VolumeID: fmt.Sprintf("volume-%d", i+1), Bound: c.pv != nil, SingleNode: c.singleNode, RequestedBytes: c.requestedBytes}
		detail := map[string]any{"volume_id": v.VolumeID, "claim_name": c.name, "storage_class": c.class.Name, "driver": c.driver, "selected_node": c.selectedNode}
		if c.pvc != nil {
			detail["pvc_phase"], detail["pvc_resource_version"] = c.pvc.Status.Phase, c.pvc.ResourceVersion
			detail["pvc_access_modes"], detail["pvc_volume_mode"] = c.pvc.Spec.AccessModes, c.pvc.Spec.VolumeMode
		}
		if c.pv != nil {
			v.NodeAffinityRequired = c.pv.Spec.NodeAffinity != nil && c.pv.Spec.NodeAffinity.Required != nil
			detail["pv_name"] = c.pv.Name
			detail["node_affinity"] = c.pv.Spec.NodeAffinity
			detail["pv_phase"], detail["pv_resource_version"] = c.pv.Status.Phase, c.pv.ResourceVersion
			detail["pv_access_modes"], detail["pv_volume_mode"] = c.pv.Spec.AccessModes, c.pv.Spec.VolumeMode
		}
		var attached []string
		for name := range c.attachedNodes {
			attached = append(attached, name)
		}
		sort.Strings(attached)
		detail["attached_nodes"] = attached
		detail["allowed_topologies"] = c.class.AllowedTopologies
		details = append(details, detail)
		o.decision.Volumes = append(o.decision.Volumes, v)
	}
	o.diagnostics["volumes"] = details
	o.diagnostics["csi_registrations"] = p.csiNodes
	o.diagnostics["ready_storage_attachment_nodes"] = p.longhornNodes
	var capacities []map[string]any
	for _, capacity := range p.capacities {
		item := map[string]any{"storage_class": capacity.StorageClassName, "node_topology": capacity.NodeTopology}
		if capacity.Capacity != nil {
			item["capacity_bytes"] = capacity.Capacity.Value()
		}
		if capacity.MaximumVolumeSize != nil {
			item["maximum_volume_bytes"] = capacity.MaximumVolumeSize.Value()
		}
		capacities = append(capacities, item)
	}
	o.diagnostics["storage_capacities"] = capacities
}

func (o *schedulingObservation) observeNode(name string, node kubeNode, found bool, requested, existing managedSharedNodeRequests, selector map[string]string, tolerations []runtimepkg.Toleration, pods []kubePod, storage *storagePlacement) *model.SchedulingNodeDecision {
	n := model.SchedulingNodeDecision{CandidateID: fmt.Sprintf("candidate-%d", len(o.decision.Nodes)+1), RuntimeMatches: found && nodeLabelsMatchSelector(node.Metadata.Labels, selector), ResourceInventoryObserved: o.resourceInventoryObserved}
	n.Requested, n.ExistingApp = schedulingResources(requested), schedulingResources(existing)
	n.EffectiveRequested = model.SchedulingResources{
		CPUMilli:       maxInt64(0, requested.cpuMilli-existing.cpuMilli),
		MemoryBytes:    maxInt64(0, requested.memoryBytes-existing.memoryBytes),
		EphemeralBytes: maxInt64(0, requested.ephemeralBytes-existing.ephemeralBytes),
	}
	n.Allocatable = model.SchedulingResources{CPUMilli: parseKubeResourceMilli(node.Status.Allocatable["cpu"]), MemoryBytes: parseKubeResourceBytes(node.Status.Allocatable["memory"]), EphemeralBytes: parseKubeResourceBytes(node.Status.Allocatable["ephemeral-storage"])}
	n.Budget = model.SchedulingResources{CPUMilli: resourceCapacityWithRatio(n.Allocatable.CPUMilli, o.decision.CPUOvercommitRatio), MemoryBytes: resourceCapacityWithRatio(n.Allocatable.MemoryBytes, o.decision.MemoryRequestRatio), EphemeralBytes: n.Allocatable.EphemeralBytes}
	add := func(code, message string) {
		n.Rejections = append(n.Rejections, model.SchedulingRejection{Code: code, Message: message})
	}
	if !found {
		add("node_not_found", "node disappeared from the scheduling inventory")
	} else {
		if !n.RuntimeMatches {
			add("runtime_selector_mismatch", "node does not match the required runtime selector")
		}
		if !kubeNodeReady(node) {
			add("node_not_ready", "node is not Ready")
		}
		if node.Spec.Unschedulable {
			add("node_unschedulable", "node is marked unschedulable")
		}
		if kubeNodeConditionTrue(node.Status.Conditions, "DiskPressure") {
			add("node_disk_pressure", "node reports DiskPressure")
		}
		if !kubeTaintsTolerated(node.Spec.Taints, tolerations) {
			add("node_taint_not_tolerated", "node has a scheduling taint not tolerated by this workload")
		}
		if free, known := availablePodAddresses(node, pods); known && o.resourceInventoryObserved {
			n.AvailablePodAddresses = &free
		}
	}
	var storageDetails []string
	if storage != nil && found {
		for i, c := range storage.claims {
			v := model.SchedulingVolumeMatch{VolumeID: fmt.Sprintf("volume-%d", i+1)}
			if c.pv != nil && c.pv.Spec.NodeAffinity != nil && c.pv.Spec.NodeAffinity.Required != nil {
				matches := storageNodeSelectorMatches(node, *c.pv.Spec.NodeAffinity.Required)
				v.AffinityMatches = &matches
			}
			one := *storage
			one.claims = []storagePlacementClaim{c}
			if r := one.rejectionDetail(node); r != nil {
				rejection := model.SchedulingRejection{Code: r.code, Message: v.VolumeID + ": " + r.message}
				v.Rejections = append(v.Rejections, rejection)
				n.Rejections = append(n.Rejections, rejection)
				storageDetails = append(storageDetails, r.detail)
			}
			n.Volumes = append(n.Volumes, v)
		}
	}
	o.nodes = append(o.nodes, map[string]any{"candidate_id": n.CandidateID, "node_name": name, "found": found, "ready": kubeNodeReady(node), "unschedulable": node.Spec.Unschedulable, "disk_pressure": kubeNodeConditionTrue(node.Status.Conditions, "DiskPressure"), "taints": node.Spec.Taints, "storage_rejections": storageDetails})
	o.decision.Nodes = append(o.decision.Nodes, n)
	return &o.decision.Nodes[len(o.decision.Nodes)-1]
}

func (o *schedulingObservation) capacity(n *model.SchedulingNodeDecision) {
	add := func(code, message string) {
		n.Rejections = append(n.Rejections, model.SchedulingRejection{Code: code, Message: message})
	}
	if n.AvailablePodAddresses != nil && *n.AvailablePodAddresses < o.decision.RequiredPodAddresses {
		add("pod_addresses_insufficient", fmt.Sprintf("available=%d, required=%d Pod IP addresses", *n.AvailablePodAddresses, o.decision.RequiredPodAddresses))
	}
	if (n.Allocatable.MemoryBytes > 0 || o.decision.Stage == "surge_capacity") && n.EffectiveRequested.MemoryBytes+o.decision.Demand.MemoryBytes > n.Budget.MemoryBytes {
		add("memory_request_budget_exceeded", fmt.Sprintf("requested=%dB + demand=%dB > budget=%dB (allocatable=%dB, ratio=%g)", n.EffectiveRequested.MemoryBytes, o.decision.Demand.MemoryBytes, n.Budget.MemoryBytes, n.Allocatable.MemoryBytes, o.decision.MemoryRequestRatio))
	}
	if (n.Allocatable.EphemeralBytes > 0 || o.decision.Stage == "surge_capacity" && o.decision.Demand.EphemeralBytes > 0) && n.EffectiveRequested.EphemeralBytes+o.decision.Demand.EphemeralBytes > n.Budget.EphemeralBytes {
		add("ephemeral_storage_insufficient", fmt.Sprintf("requested=%dB + demand=%dB > allocatable=%dB ephemeral-storage", n.EffectiveRequested.EphemeralBytes, o.decision.Demand.EphemeralBytes, n.Budget.EphemeralBytes))
	}
}

func (s *Service) recordSchedulingObservation(ctx context.Context, app model.App, o *schedulingObservation, err error) {
	operationID, _ := ctx.Value(schedulingOperationKey{}).(string)
	if apply := managedAppApplySourceFromContext(ctx); apply.Source == managedAppApplySourceOperation && apply.OperationID != "" {
		operationID = apply.OperationID
	}
	if operationID == "" {
		return
	} // Reconciliation without an operation must not invent one.
	if o.decision.Outcome == "" {
		if err != nil {
			o.decision.Outcome = "observation_failed"
			o.decision.MissingEvidence = append(o.decision.MissingEvidence, "scheduling_inputs")
		} else {
			o.decision.Outcome = "skipped"
		}
	}
	if err != nil && o.decision.Outcome == "observation_failed" {
		o.diagnostics["observation_error"] = observability.RedactDiagnosticText(err.Error()).Text
	}
	severity, confidence := model.OperationEvidenceSeverityInfo, model.OperationEvidenceConfidenceConfirmed
	if len(o.decision.MissingEvidence) != 0 {
		confidence = model.OperationEvidenceConfidenceInsufficientEvidence
	}
	if o.decision.Outcome == "rejected" {
		severity = model.OperationEvidenceSeverityError
	}
	if o.decision.Outcome == "observation_failed" {
		severity = model.OperationEvidenceSeverityWarning
	}
	// Chunk node inventories to stay below the evidence store's 128 KiB row
	// limit. All parts retain a common observation ID; readers require a full
	// set before treating a reconstructed snapshot as complete.
	const perPart = 24
	parts := maxInt(1, (len(o.decision.Nodes)+perPart-1)/perPart)
	observationID := model.NewID("sched")
	for part := 0; part < parts; part++ {
		decision := o.decision
		start, end := part*perPart, min((part+1)*perPart, len(decision.Nodes))
		decision.Nodes = append([]model.SchedulingNodeDecision(nil), decision.Nodes[start:end]...)
		diagnostics := map[string]any{}
		for k, v := range o.diagnostics {
			diagnostics[k] = v
		}
		var nodeDetails []map[string]any
		for i := start; i < end; i++ {
			detail := map[string]any{}
			for k, v := range o.nodes[i] {
				detail[k] = v
			}
			detail["decision"] = o.decision.Nodes[i]
			nodeDetails = append(nodeDetails, detail)
			// A tenant needs capacity for its allowed runtime, not an inventory
			// of unrelated nodes in other tenants' locations.
			if !decision.Nodes[i-start].RuntimeMatches {
				decision.Nodes[i-start] = model.SchedulingNodeDecision{CandidateID: decision.Nodes[i-start].CandidateID, Rejections: decision.Nodes[i-start].Rejections}
			}
		}
		diagnostics["nodes"] = nodeDetails
		s.recordOperationEvidenceBestEffort(model.OperationEvidence{
			TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: operationID,
			Type: model.OperationEvidenceTypeSchedulingDecision, Source: model.OperationEvidenceSourceController,
			Severity: severity, Confidence: confidence, ObservedAt: decision.ObservedAt,
			Summary: o.decision.Summary(), Reason: "scheduling_" + decision.Outcome,
			RedactionStatus: model.OperationEvidenceRedactionRedacted,
			Payload:         map[string]any{"scheduling_decision_id": observationID, "scheduling_decision_part": part + 1, "scheduling_decision_parts": parts, "scheduling_decision": decision, "scheduling_diagnostics": diagnostics},
		})
	}
}
