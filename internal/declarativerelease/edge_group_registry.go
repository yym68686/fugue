package declarativerelease

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
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

// EdgeGroup binds one group-local client, control plane, and A/B worker lane. The
// component records intentionally remain ordinary declarative Component data:
// adding a group adds data, not a new execution path.
type EdgeGroup struct {
	ID            string            `json:"id"`
	GroupID       string            `json:"groupId"`
	FaultDomainID string            `json:"faultDomainId"`
	EdgePoolID    string            `json:"edgePoolId"`
	Labels        map[string]string `json:"labels,omitempty"`
	Client        Component         `json:"client"`
	Control       Component         `json:"control"`
	Worker        Component         `json:"worker"`
}

func DecodeEdgeGroupRegistry(reader io.Reader) (EdgeGroupRegistry, error) {
	var registry EdgeGroupRegistry
	if err := decodeStrict(reader, &registry); err != nil {
		return EdgeGroupRegistry{}, fmt.Errorf("decode edge group registry: %w", err)
	}
	if err := registry.validate(true); err != nil {
		return EdgeGroupRegistry{}, err
	}
	return registry, nil
}

// DecodeHistoricalEdgeGroupRegistry reads the predecessor shape that predates
// the data-owned client lane. It is used only to compare an exact Git parent;
// current registries must still use DecodeEdgeGroupRegistry and include client.
func DecodeHistoricalEdgeGroupRegistry(reader io.Reader) (EdgeGroupRegistry, error) {
	var legacy struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Groups     []struct {
			ID            string            `json:"id"`
			GroupID       string            `json:"groupId"`
			FaultDomainID string            `json:"faultDomainId,omitempty"`
			EdgePoolID    string            `json:"edgePoolId,omitempty"`
			Labels        map[string]string `json:"labels,omitempty"`
			Client        Component         `json:"client,omitempty"`
			Control       Component         `json:"control"`
			Worker        Component         `json:"worker"`
		} `json:"groups"`
	}
	if err := decodeStrict(reader, &legacy); err != nil {
		return EdgeGroupRegistry{}, fmt.Errorf("decode historical edge group registry: %w", err)
	}
	registry := EdgeGroupRegistry{APIVersion: legacy.APIVersion, Kind: legacy.Kind, Groups: make([]EdgeGroup, 0, len(legacy.Groups))}
	for _, group := range legacy.Groups {
		registry.Groups = append(registry.Groups, EdgeGroup{
			ID: group.ID, GroupID: group.GroupID, FaultDomainID: group.FaultDomainID, EdgePoolID: group.EdgePoolID, Labels: group.Labels,
			Client: group.Client, Control: group.Control, Worker: group.Worker,
		})
	}
	if err := registry.validate(false); err != nil {
		return EdgeGroupRegistry{}, err
	}
	return registry, nil
}

func (registry EdgeGroupRegistry) Validate() error {
	return registry.validate(true)
}

func (registry EdgeGroupRegistry) validate(requireClient bool) error {
	if registry.APIVersion != EdgeGroupRegistryAPIVersion || registry.Kind != EdgeGroupRegistryKind {
		return fmt.Errorf("unsupported edge group registry identity %q/%q", registry.APIVersion, registry.Kind)
	}
	if len(registry.Groups) == 0 || len(registry.Groups) > 100 {
		return errors.New("edge group registry group count is invalid")
	}
	seenGroupIDs := make(map[string]struct{}, len(registry.Groups))
	for index, group := range registry.Groups {
		if err := group.validate(requireClient); err != nil {
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

func (group EdgeGroup) validate(requireClient bool) error {
	if !componentIDPattern.MatchString(group.ID) || !edgeGroupIDPattern.MatchString(group.GroupID) {
		return errors.New("edge group identity is invalid")
	}
	if requireClient && (!componentIDPattern.MatchString(group.FaultDomainID) || !componentIDPattern.MatchString(group.EdgePoolID)) {
		return errors.New("edge group fault domain and pool identity are invalid")
	}
	for key, value := range group.Labels {
		if !componentIDPattern.MatchString(key) || strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n") {
			return errors.New("edge group labels are invalid")
		}
	}
	wantControl := "edge-control-" + group.ID
	wantClient := "edge-client-" + group.ID
	wantWorker := "edge-worker-" + group.ID
	if (requireClient && group.Client.ID != wantClient) || group.Control.ID != wantControl || group.Worker.ID != wantWorker {
		return fmt.Errorf("edge group components must be %q, %q, and %q", wantClient, wantControl, wantWorker)
	}
	if (requireClient && group.Client.Family != "edge-client") || group.Control.Family != "edge" || group.Worker.Family != "edge" {
		return errors.New("edge group component families are invalid")
	}
	if group.Client.ID != "" {
		if err := group.Client.Validate(); err != nil {
			return fmt.Errorf("client component: %w", err)
		}
	}
	if err := group.Control.Validate(); err != nil {
		return fmt.Errorf("control component: %w", err)
	}
	if err := group.Worker.Validate(); err != nil {
		return fmt.Errorf("worker component: %w", err)
	}
	publicRoute, err := edgeGroupPublicRouteProbe(group)
	if err != nil {
		return err
	}
	if publicRoute.Name != group.GroupID {
		return errors.New("edge worker public route canary is not group-bound")
	}
	readyPath := "/v1/authority/groups/" + group.GroupID + "/readyz"
	for _, probe := range group.Control.Health {
		if probe.Type == "public-route-http" {
			return errors.New("edge control public route canary must derive from worker group data")
		}
		if probe.Type == "service-http" && probe.Name == group.Control.Workload.Name && probe.Path == readyPath {
			return errors.New("edge control authority health must derive from group identity")
		}
	}
	transition := group.Worker.Transition
	if transition == nil || transition.EdgeGroupAB == nil || transition.EdgeGroupAB.GroupID != group.GroupID {
		return errors.New("edge worker transition is not bound to the registry group")
	}
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

func edgeGroupPublicRouteProbe(group EdgeGroup) (HealthProbe, error) {
	var found HealthProbe
	count := 0
	for _, probe := range group.Worker.Health {
		if probe.Type != "public-route-http" {
			continue
		}
		found = probe
		count++
	}
	if count != 1 {
		return HealthProbe{}, fmt.Errorf("edge group %q must define exactly one worker public route canary", group.ID)
	}
	return found, nil
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
	apiIndex := -1
	for index := range merged.Components {
		component := &merged.Components[index]
		component.Health = append([]HealthProbe(nil), component.Health...)
		if component.ID != "api" {
			continue
		}
		apiIndex = index
		for _, probe := range component.Health {
			if probe.Type == "public-route-http" {
				return Registry{}, errors.New("API public route canaries must derive from edge group data")
			}
		}
	}
	if apiIndex < 0 {
		return Registry{}, errors.New("production component registry omits API")
	}
	api := merged.Components[apiIndex]
	if api.Workload.Kind != "Deployment" || api.Workload.Name == "" || api.Workload.Container == "" {
		return Registry{}, errors.New("edge group health requires the declarative API workload")
	}
	for _, group := range edge.Groups {
		publicRoute, err := edgeGroupPublicRouteProbe(group)
		if err != nil {
			return Registry{}, err
		}
		control := group.Control
		// A Control release is installed as an inactive candidate publisher. Its
		// candidate endpoint is therefore allowed to be unavailable while the
		// previously verified authority continues to serve users. Binding the
		// Control workload's LKG health to the candidate/authority ready endpoint
		// creates a circular dependency: the new publisher cannot be installed
		// until it publishes, while it cannot publish until it is installed.
		// Keep the process-local probe and the independent public route canary;
		// candidate readiness is verified later by the candidate-bound canary.
		control.Health = append(append([]HealthProbe(nil), control.Health...), publicRoute)
		apiProbe := publicRoute
		apiProbe.Name = "api-via-" + group.GroupID
		apiProbe.Host = "api.fugue.pro"
		apiProbe.Expected = "\"status\":\"ok\""
		merged.Components[apiIndex].Health = append(merged.Components[apiIndex].Health, apiProbe)
		merged.Components = append(merged.Components, group.Client, control, group.Worker)
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
			if _, exists := changed[group.Client.IntentPath]; exists {
				return fmt.Errorf("new edge group %q client intent must be a later production atom", group.ID)
			}
			if _, exists := changed[group.Control.IntentPath]; exists {
				return fmt.Errorf("new edge group %q control intent must be a later production atom", group.ID)
			}
			if _, exists := changed[group.Worker.IntentPath]; exists {
				return fmt.Errorf("new edge group %q worker intent must be a later production atom", group.ID)
			}
		}
		return nil
	}
	if err := previous.validate(false); err != nil {
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
		}{{group.Client.ID, prior.Client, group.Client}, {group.Control.ID, prior.Control, group.Control}, {group.Worker.ID, prior.Worker, group.Worker}} {
			if pair.id == group.Client.ID && pair.previous.ID == "" {
				continue
			}
			priorJSON, err := CanonicalJSON(pair.previous)
			if err != nil {
				return err
			}
			nextJSON, err := CanonicalJSON(pair.next)
			if err != nil {
				return err
			}
			if string(priorJSON) != string(nextJSON) && selected != pair.id && !componentSourceRootsStrictlyNarrowed(pair.previous, pair.next) {
				return fmt.Errorf("edge group registry changed unselected component %q", pair.id)
			}
		}
	}
	if len(before) != 0 {
		return errors.New("edge group registry cannot remove an existing group")
	}
	return nil
}

// componentSourceRootsStrictlyNarrowed permits correcting an over-broad
// release dependency without manufacturing a release for the affected data
// plane. Every runtime field must remain byte-identical and new dependency
// roots remain fail-closed.
func componentSourceRootsStrictlyNarrowed(previous, next Component) bool {
	if len(next.SourceRoots) >= len(previous.SourceRoots) {
		return false
	}
	previousRoots := make(map[string]struct{}, len(previous.SourceRoots))
	for _, root := range previous.SourceRoots {
		previousRoots[root] = struct{}{}
	}
	for _, root := range next.SourceRoots {
		if _, exists := previousRoots[root]; !exists {
			return false
		}
	}
	previous.SourceRoots = nil
	next.SourceRoots = nil
	previousJSON, previousErr := CanonicalJSON(previous)
	nextJSON, nextErr := CanonicalJSON(next)
	return previousErr == nil && nextErr == nil && string(previousJSON) == string(nextJSON)
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
