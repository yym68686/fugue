package declarativerelease

import (
	"errors"
	"fmt"
	"io"
	"sort"
)

const (
	EdgeGroupRegistryAPIVersion = "release.fugue.dev/v1"
	EdgeGroupRegistryKind       = "EdgeGroupRegistry"
)

// EdgeGroupRegistry is the only repository data document that enumerates
// Edge fault domains. Release code and CI consume the expanded components and
// never branch on a country, region, or a fixed group count.
type EdgeGroupRegistry struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Groups     []EdgeGroup `json:"groups"`
}

// EdgeGroup binds one group-local control plane and one A/B worker lane. The
// component records intentionally remain ordinary declarative Component data:
// adding a group adds data, not a new execution path.
type EdgeGroup struct {
	ID      string    `json:"id"`
	GroupID string    `json:"groupId"`
	Control Component `json:"control"`
	Worker  Component `json:"worker"`
}

func DecodeEdgeGroupRegistry(reader io.Reader) (EdgeGroupRegistry, error) {
	var registry EdgeGroupRegistry
	if err := decodeStrict(reader, &registry); err != nil {
		return EdgeGroupRegistry{}, fmt.Errorf("decode edge group registry: %w", err)
	}
	if err := registry.Validate(); err != nil {
		return EdgeGroupRegistry{}, err
	}
	return registry, nil
}

func (registry EdgeGroupRegistry) Validate() error {
	if registry.APIVersion != EdgeGroupRegistryAPIVersion || registry.Kind != EdgeGroupRegistryKind {
		return fmt.Errorf("unsupported edge group registry identity %q/%q", registry.APIVersion, registry.Kind)
	}
	if len(registry.Groups) == 0 || len(registry.Groups) > 100 {
		return errors.New("edge group registry group count is invalid")
	}
	seenGroupIDs := make(map[string]struct{}, len(registry.Groups))
	for index, group := range registry.Groups {
		if err := group.validate(); err != nil {
			return fmt.Errorf("edge group %d: %w", index, err)
		}
		if index > 0 && registry.Groups[index-1].ID >= group.ID {
			return errors.New("edge groups must be strictly ordered by id")
		}
		if _, exists := seenGroupIDs[group.GroupID]; exists {
			return fmt.Errorf("duplicate edge group id %q", group.GroupID)
		}
		seenGroupIDs[group.GroupID] = struct{}{}
	}
	return nil
}

func (group EdgeGroup) validate() error {
	if !componentIDPattern.MatchString(group.ID) || !edgeGroupIDPattern.MatchString(group.GroupID) {
		return errors.New("edge group identity is invalid")
	}
	wantControl := "edge-control-" + group.ID
	wantWorker := "edge-worker-" + group.ID
	if group.Control.ID != wantControl || group.Worker.ID != wantWorker {
		return fmt.Errorf("edge group components must be %q and %q", wantControl, wantWorker)
	}
	if group.Control.Family != "edge" || group.Worker.Family != "edge" {
		return errors.New("edge group component family must be edge")
	}
	if err := group.Control.Validate(); err != nil {
		return fmt.Errorf("control component: %w", err)
	}
	if err := group.Worker.Validate(); err != nil {
		return fmt.Errorf("worker component: %w", err)
	}
	transition := group.Worker.Transition
	if transition == nil || transition.EdgeGroupAB == nil || transition.EdgeGroupAB.GroupID != group.GroupID {
		return errors.New("edge worker transition is not bound to the registry group")
	}
	readyPath := "/v1/authority/groups/" + group.GroupID + "/readyz"
	controlProcessReady := false
	legacyControlPublicationReady := false
	for _, probe := range group.Control.Health {
		if probe.Type == "deployment" && probe.Name == group.Control.Workload.Name {
			controlProcessReady = true
		}
		if probe.Type == "service-http" && probe.Name == group.Control.Workload.Name && probe.Path == "/readyz" {
			controlProcessReady = true
		}
		if probe.Type == "service-http" && probe.Name == group.Control.Workload.Name && probe.Path == readyPath {
			legacyControlPublicationReady = true
		}
	}
	publicationReady := false
	for _, probe := range group.Worker.Health {
		if probe.Type == "service-http" && probe.Name == group.Control.Workload.Name && probe.Path == readyPath {
			publicationReady = true
		}
		if probe.Type == "edge-group-authority" && probe.Name == group.GroupID {
			publicationReady = true
		}
	}
	if !legacyControlPublicationReady && (!controlProcessReady || !publicationReady) {
		return errors.New("edge control process and worker publication health must be group-bound")
	}
	return nil
}

// MergeEdgeGroupRegistry expands data-defined Edge groups into the ordinary
// component inventory used by the planner and executor.
func MergeEdgeGroupRegistry(base Registry, edge EdgeGroupRegistry) (Registry, error) {
	if err := base.Validate(); err != nil {
		return Registry{}, err
	}
	if base.EdgeGroupRegistryPath == "" {
		return Registry{}, errors.New("production component registry does not declare an edge group registry")
	}
	if err := edge.Validate(); err != nil {
		return Registry{}, err
	}
	merged := base
	merged.Components = append([]Component(nil), base.Components...)
	for _, group := range edge.Groups {
		merged.Components = append(merged.Components, group.Control, group.Worker)
	}
	sort.Slice(merged.Components, func(i, j int) bool { return merged.Components[i].ID < merged.Components[j].ID })
	if err := merged.Validate(); err != nil {
		return Registry{}, fmt.Errorf("validate expanded edge group registry: %w", err)
	}
	return merged, nil
}

// ValidateEdgeGroupRegistryUpdate makes the data registry an atomic control
// surface. Introducing groups is configuration-only until an intent selects
// one component; changing an existing group is allowed only for the component selected
// by the same production plan.
func ValidateEdgeGroupRegistryUpdate(previous *EdgeGroupRegistry, current EdgeGroupRegistry, plan Plan, changedPaths []string) error {
	if err := current.Validate(); err != nil {
		return err
	}
	changed := make(map[string]struct{}, len(changedPaths))
	for _, path := range changedPaths {
		changed[path] = struct{}{}
	}
	selected := ""
	if len(plan.Releases) > 1 {
		return errors.New("edge group registry update selected multiple production components")
	}
	if len(plan.Releases) == 1 {
		selected = plan.Releases[0].ComponentID
	}
	if previous == nil {
		if selected != "" {
			return errors.New("initial edge group registry must not activate a production component")
		}
		for _, group := range current.Groups {
			if _, exists := changed[group.Control.IntentPath]; exists {
				return fmt.Errorf("new edge group %q control intent must be a later production atom", group.ID)
			}
			if _, exists := changed[group.Worker.IntentPath]; exists {
				return fmt.Errorf("new edge group %q worker intent must be a later production atom", group.ID)
			}
		}
		return nil
	}
	if err := previous.Validate(); err != nil {
		return fmt.Errorf("previous edge group registry: %w", err)
	}
	before := make(map[string]EdgeGroup, len(previous.Groups))
	for _, group := range previous.Groups {
		before[group.ID] = group
	}
	for _, group := range current.Groups {
		prior, exists := before[group.ID]
		if !exists {
			continue
		}
		delete(before, group.ID)
		if edgeGroupUsesStagedHealth(prior) && !edgeGroupUsesStagedHealth(group) {
			return fmt.Errorf("edge group %q cannot regress staged publication health", group.ID)
		}
		for _, pair := range []struct {
			id             string
			previous, next Component
		}{{group.Control.ID, prior.Control, group.Control}, {group.Worker.ID, prior.Worker, group.Worker}} {
			priorJSON, err := CanonicalJSON(pair.previous)
			if err != nil {
				return err
			}
			nextJSON, err := CanonicalJSON(pair.next)
			if err != nil {
				return err
			}
			if string(priorJSON) != string(nextJSON) && selected != pair.id {
				return fmt.Errorf("edge group registry changed unselected component %q", pair.id)
			}
		}
	}
	if len(before) != 0 {
		return errors.New("edge group registry cannot remove an existing group")
	}
	return nil
}

func edgeGroupUsesStagedHealth(group EdgeGroup) bool {
	readyPath := "/v1/authority/groups/" + group.GroupID + "/readyz"
	controlReady := false
	publicationReady := false
	for _, probe := range group.Control.Health {
		controlReady = controlReady || (probe.Type == "deployment" && probe.Name == group.Control.Workload.Name)
		controlReady = controlReady || (probe.Type == "service-http" && probe.Name == group.Control.Workload.Name && probe.Path == "/readyz")
	}
	for _, probe := range group.Worker.Health {
		publicationReady = publicationReady || (probe.Type == "service-http" && probe.Name == group.Control.Workload.Name && probe.Path == readyPath)
		publicationReady = publicationReady || (probe.Type == "edge-group-authority" && probe.Name == group.GroupID)
	}
	return controlReady && publicationReady
}
