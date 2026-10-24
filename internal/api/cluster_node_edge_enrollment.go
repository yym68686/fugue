package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

type edgeEnrollmentAuthority struct {
	PreviousSlot   string `json:"previousWorkerSlot"`
	PreviousFence  uint64 `json:"previousFrontGeneration"`
	PreviousBundle string `json:"previousBundleGeneration"`
	PreviousSource string `json:"previousWorkerSourceSha"`
	PreviousImage  string `json:"previousWorkerImageDigest"`
	PreviousRecord string `json:"previousRecordDigest"`
	GroupID        string `json:"groupId"`
	Record         string `json:"currentRecordDigest"`
	Slot           string `json:"currentWorkerSlot"`
	Fence          uint64 `json:"currentFrontGeneration"`
	Bundle         string `json:"currentBundleGeneration"`
	Source         string `json:"currentWorkerSourceSha"`
	Image          string `json:"currentWorkerImageDigest"`
}

func (s *Server) prepareManagedEdgeEnrollment(ctx context.Context, machine model.Machine, group string) (model.EdgeNodeEnrollment, error) {
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return model.EdgeNodeEnrollment{}, err
	}
	defer client.closeIdleConnections()
	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		return model.EdgeNodeEnrollment{}, errors.New("Edge enrollment namespace is unavailable")
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/configmaps/" + url.PathEscape("fugue-current-authority-"+group)
	if err := client.doJSON(ctx, http.MethodGet, path, &cm); err != nil {
		return model.EdgeNodeEnrollment{}, err
	}
	var current edgeEnrollmentAuthority
	if json.Unmarshal([]byte(cm.Data["authority.json"]), &current) != nil || current.GroupID != group {
		return model.EdgeNodeEnrollment{}, errors.New("Edge enrollment current authority is unavailable")
	}
	now := time.Now().UTC()
	enrollment := model.EdgeNodeEnrollment{ID: model.NewID("edgeenroll"), NodeID: machine.ClusterNodeName, EdgeGroupID: group, AuthorityRecordDigest: current.Record, Slot: current.Slot, FenceSequence: current.Fence, BundleVersion: current.Bundle, WorkerSourceSHA: current.Source, WorkerImageDigest: current.Image, IssuedAt: now, ExpiresAt: now.Add(15 * time.Minute)}
	if err := enrollment.Validate(machine.ClusterNodeName, group, now); err != nil {
		return model.EdgeNodeEnrollment{}, err
	}
	// Idempotent reads/retries never renew a still-valid admission lease.
	var previous model.EdgeNodeEnrollment
	if json.Unmarshal([]byte(machine.Labels[model.MachineEdgeEnrollmentLabel]), &previous) == nil && previous.Validate(machine.ClusterNodeName, group, now) == nil && previous.AuthorityRecordDigest == enrollment.AuthorityRecordDigest && previous.Slot == enrollment.Slot && previous.FenceSequence == enrollment.FenceSequence && previous.WorkerSourceSHA == enrollment.WorkerSourceSHA && previous.WorkerImageDigest == enrollment.WorkerImageDigest {
		return previous, nil
	}
	return enrollment, nil
}

func (s *Server) edgeNodeEnrollment(ctx context.Context, node model.EdgeNode) *model.EdgeNodeEnrollment {
	machine, err := s.store.GetMachineByClusterNodeName(node.ID)
	if err != nil || !machine.Policy.AllowEdge {
		return nil
	}
	var intent model.EdgeNodeEnrollment
	if json.Unmarshal([]byte(machine.Labels[model.MachineEdgeEnrollmentLabel]), &intent) != nil || intent.Validate(node.ID, node.EdgeGroupID, time.Now().UTC()) != nil {
		return nil
	}
	current, err := s.prepareManagedEdgeEnrollment(ctx, machine, node.EdgeGroupID)
	if err != nil || current.ID != intent.ID {
		return nil
	}
	return &intent
}

func (s *Server) managedEdgeCredentialInstalled(machine model.Machine) (bool, error) {
	node, _, err := s.store.GetEdgeNode(machine.ClusterNodeName)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if node.EdgeGroupID != machine.Labels[runtimepkg.EdgeGroupIDLabelKey] || node.WorkloadMode != model.EdgeWorkloadModeDynamic || node.TokenPrefix == "" {
		return false, nil
	}
	updaters, err := s.store.ListNodeUpdaters("", true)
	if err != nil {
		return false, err
	}
	for _, u := range updaters {
		if u.MachineID == machine.ID && u.ClusterNodeName == machine.ClusterNodeName && nodeUpdaterEdgeEnvGenerationTokenPrefix(u.EdgeEnvGeneration) == node.TokenPrefix {
			return true, nil
		}
	}
	return false, nil
}

// Validate all enrollment prerequisites before changing a role or pool mode.
// A role change cannot migrate an established Edge authority or its credentials.
func (s *Server) validateManagedEdgeEnrollment(machine model.Machine, groupID string) error {
	if previous := machine.Labels[runtimepkg.EdgeGroupIDLabelKey]; previous != "" && previous != groupID {
		return fmt.Errorf("%w: node is already bound to another Edge authority", store.ErrInvalidInput)
	}
	exists, err := s.store.EdgeGroupExists(groupID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: Edge authority does not exist", store.ErrInvalidInput)
	}
	edge, _, err := s.store.GetEdgeNode(machine.ClusterNodeName)
	if err == nil {
		if edge.EdgeGroupID != groupID {
			return fmt.Errorf("%w: existing Edge identity belongs to another authority", store.ErrInvalidInput)
		}
		if edge.WorkloadMode != model.EdgeWorkloadModeDynamic && (edge.Healthy || edge.LastHeartbeatAt != nil || edge.RouteBundleVersion != "") {
			return fmt.Errorf("%w: an established static Edge requires an explicit migration", store.ErrInvalidInput)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	updaters, err := s.store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	for _, updater := range updaters {
		if updater.ClusterNodeName != machine.ClusterNodeName || updater.MachineID != machine.ID || updater.Status != "active" {
			continue
		}
		seen := updater.LastHeartbeatAt
		if seen == nil || time.Since(*seen) > 15*time.Minute || seen.After(time.Now().Add(30*time.Second)) {
			continue
		}
		for _, capability := range updater.Capabilities {
			if strings.TrimSpace(capability) == model.NodeUpdateTaskTypeRefreshDesiredState {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: a fresh node updater with refresh-desired-state capability is required for Edge enrollment", store.ErrInvalidInput)
}
