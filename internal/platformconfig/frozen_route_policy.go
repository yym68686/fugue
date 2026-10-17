package platformconfig

import (
	"encoding/json"
	"fmt"
	"sync"

	"fugue/internal/model"
)

type routeProjectionPolicy struct {
	minimumHealthy  int
	compiledTraffic map[string]TrafficPolicyConstraint
}

func decodeRouteProjectionPolicy(raw []byte) (routeProjectionPolicy, error) {
	var policy PolicySnapshot
	if err := json.Unmarshal(raw, &policy); err != nil {
		return routeProjectionPolicy{}, err
	}
	if err := ValidatePolicySnapshot(policy); err != nil {
		return routeProjectionPolicy{}, err
	}
	out := routeProjectionPolicy{minimumHealthy: 1, compiledTraffic: map[string]TrafficPolicyConstraint{}}
	if policy.MinimumHealthyEdges > 0 {
		out.minimumHealthy = policy.MinimumHealthyEdges
	}
	for _, rule := range policy.TrafficConstraints {
		out.compiledTraffic[rule.AppID] = rule
	}
	return out, nil
}

// NewFrozenPolicyRouteProjector snapshots one complete policy for a pure
// compilation phase. Each artifact must omit policy: callers must construct a
// new projector after a policy change, rather than overriding its frozen input.
// Route/cache/TLS validation still runs for every artifact. This is not an
// artifact integrity check, authority decision or runtime-evidence cache.
func NewFrozenPolicyRouteProjector(policy any) func(model.PlatformArtifact) (model.EdgeRouteIntentSnapshot, error) {
	return newFrozenPolicyRouteProjector(policy, decodeRouteProjectionPolicy)
}

func newFrozenPolicyRouteProjector(policy any, decode func([]byte) (routeProjectionPolicy, error)) func(model.PlatformArtifact) (model.EdgeRouteIntentSnapshot, error) {
	raw, marshalErr := json.Marshal(policy)
	var once sync.Once
	var prepared routeProjectionPolicy
	var decodeErr error
	read := func() (routeProjectionPolicy, error) {
		once.Do(func() {
			decodeErr = marshalErr
			if decodeErr == nil {
				prepared, decodeErr = decode(raw)
			}
			// The validated projection needs only minimum edge count and traffic
			// constraints; discard the potentially large DNS policy encoding.
			raw = nil
		})
		return prepared, decodeErr
	}
	return func(artifact model.PlatformArtifact) (model.EdgeRouteIntentSnapshot, error) {
		if _, exists := artifact.Content["policy"]; exists {
			return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("frozen route projector cannot override artifact policy")
		}
		return projectRouteArtifact(artifact, func() (routeProjectionPolicy, error) {
			if _, exists := artifact.Content["policy"]; exists {
				return routeProjectionPolicy{}, fmt.Errorf("frozen route projector cannot override artifact policy")
			}
			return read()
		})
	}
}
