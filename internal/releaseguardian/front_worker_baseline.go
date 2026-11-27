package releaseguardian

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FrontWorkerBaselineBoundary struct {
	Policy      string                        `json:"policyDigest"`
	Leases      []FrontWorkerLeaseObservation `json:"leases"`
	Projections []FrontWorkerLeaseObservation `json:"projections"`
}

// ObserveFrontWorkerBaselineBoundary verifies that the native provider
// boundary is installed and its known write paths are idle. Endpoint
// enforcement, revocation and drainage require separately retained evidence.
func (store *AuthorityStore) ObserveFrontWorkerBaselineBoundary(ctx context.Context, config FrontWorkerCutover, nodes []string) (FrontWorkerBaselineBoundary, error) {
	var boundary FrontWorkerBaselineBoundary
	if config.Namespace != store.namespace || config.Phase != KubeCutoverNative || len(nodes) == 0 {
		return boundary, errors.New("Front/Worker baseline requires an enrolled native group and Nodes")
	}
	if err := store.CheckNodeWorkerShadowFence(ctx, config.Group); err != nil {
		return boundary, err
	}
	policy, err := ObserveFrontWorkerCutover(ctx, store.client, config)
	if err != nil {
		return boundary, err
	}
	boundary.Policy, err = FrontWorkerPolicyDigest(config, policy)
	if err != nil {
		return boundary, err
	}
	boundary.Leases, err = observeReleasedFrontWorkerLeases(ctx, store.client, config)
	if err != nil {
		return boundary, err
	}
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	for _, node := range nodes {
		name := NodeWorkerProjectionName(config.Group, node)
		if name == "" {
			return boundary, errors.New("Front/Worker baseline has an invalid Node projection")
		}
		projection := FrontWorkerLeaseObservation{Name: name}
		cm, err := configs.Get(ctx, name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			if err != nil {
				return boundary, err
			}
			if err := validateNodeWorkerProjection(cm, name, config.Group, node); err != nil || len(cm.Data) != 0 {
				return boundary, errors.Join(fmt.Errorf("Front/Worker journal remains projected to Node %s", node), err)
			}
			projection = workerTerminalRef(cm)
		}
		boundary.Projections = append(boundary.Projections, projection)
	}
	if err := store.CheckNodeWorkerShadowFence(ctx, config.Group); err != nil {
		return boundary, err
	}
	return boundary, nil
}
