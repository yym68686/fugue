package controller

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"fugue/internal/model"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// A move preserves the source class when it fits. Otherwise only classes
// explicitly approved by the platform for durable RWO data are candidates.
// Existing claims are never rewritten; the migration creates a fresh claim.
func migrationStorageClass(ctx context.Context, client *kubeClient, nodeName, preferred, size string, postgres bool) (string, error) {
	if nodeName == "" || preferred == "" {
		return preferred, nil
	}
	node, found, err := client.getNode(ctx, nodeName)
	if err != nil || !found {
		return "", fmt.Errorf("observe migration destination %s: found=%t error=%v", nodeName, found, err)
	}
	quantity, err := resource.ParseQuantity(size)
	if err != nil || quantity.Sign() <= 0 {
		return "", fmt.Errorf("invalid migration storage size %q", size)
	}
	var classes storagev1.StorageClassList
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/storageclasses", nil, &classes); err != nil {
		return "", err
	}
	var drivers storagev1.CSINodeList
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/csinodes", nil, &drivers); err != nil {
		return "", err
	}
	var capacities storagev1.CSIStorageCapacityList
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/csistoragecapacities", nil, &capacities); err != nil {
		return "", err
	}
	p := &storagePlacement{capacities: capacities.Items, csiNodes: map[string]map[string]bool{}}
	registered := map[string]bool{}
	for _, n := range drivers.Items {
		p.csiNodes[n.Name] = map[string]bool{}
		for _, driver := range n.Spec.Drivers {
			p.csiNodes[n.Name][driver.Name] = true
			registered[driver.Name] = true
		}
	}
	for _, sc := range classes.Items {
		if sc.Provisioner == longhornCSIDriver {
			p.longhornNodes, err = readyLonghornAttachmentNodes(ctx, client)
			if err != nil {
				return "", err
			}
			break
		}
	}
	sort.Slice(classes.Items, func(i, j int) bool { return classes.Items[i].Name < classes.Items[j].Name })
	reason := "storage class is absent"
	candidates := []string{}
	for _, sc := range classes.Items {
		approved := sc.Labels["fugue.pro/managed-postgres"] == "true"
		if !postgres {
			approved = approved || sc.Labels["fugue.pro/movable-rwo"] == "true"
		}
		if sc.Name != preferred && !approved {
			continue
		}
		driver := ""
		if registered[sc.Provisioner] || sc.Provisioner == longhornCSIDriver {
			driver = sc.Provisioner
		}
		p.claims = []storagePlacementClaim{{name: "new-migration-volume", class: sc, driver: driver, requestedBytes: quantity.Value()}}
		rejected := p.rejection(node)
		if sc.Name == preferred {
			if rejected == "" {
				return preferred, nil
			}
			reason = rejected
		} else if rejected == "" && driver != "" && sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion {
			candidates = append(candidates, sc.Name)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("destination %s cannot provision storage class %s (%s), and no approved compatible class is available", nodeName, preferred, reason)
	}
	return candidates[0], nil
}

func (s *Service) prepareMigrationStoragePlacement(ctx context.Context, op model.Operation, current, desired model.App) (model.App, bool, error) {
	if op.Type != model.OperationTypeMigrate || !model.AppPersistentStorageSpecUsesMovableRWO(desired.Spec.PersistentStorage) || !movableRWONeedsFreshClaim(op, current, desired) {
		return desired, false, nil
	}
	_, planned, changed, err := s.buildMovableRWOCopyPlan(ctx, op, current, desired)
	if err != nil {
		return desired, false, err
	}
	target, err := s.Store.GetRuntime(planned.Spec.RuntimeID)
	if err != nil || target.ClusterNodeName == "" {
		return planned, changed, err
	}
	storage := *planned.Spec.PersistentStorage
	// An explicit replacement class is already user intent.
	if current.Spec.PersistentStorage == nil || strings.TrimSpace(storage.StorageClassName) != strings.TrimSpace(current.Spec.PersistentStorage.StorageClassName) {
		return planned, changed, nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return desired, false, err
	}
	class, err := migrationStorageClass(ctx, client, target.ClusterNodeName, storage.StorageClassName, storage.StorageSize, false)
	if err != nil {
		return desired, false, err
	}
	if class != storage.StorageClassName {
		storage.StorageClassName = class
		planned.Spec.PersistentStorage = &storage
		changed = true
	}
	return planned, changed, nil
}

func (s *Service) selectDatabaseMigrationStorage(ctx context.Context, client *kubeClient, op model.Operation, current, desired *model.AppPostgresSpec, source, target string) error {
	if op.Type != model.OperationTypeDatabaseLocalize || source == target || current == nil || desired == nil || current.StorageClassName != desired.StorageClassName {
		return nil
	}
	runtime, err := s.Store.GetRuntime(target)
	if err != nil {
		return err
	}
	if runtime.ClusterNodeName == "" {
		return nil
	}
	class, err := migrationStorageClass(ctx, client, runtime.ClusterNodeName, desired.StorageClassName, desired.StorageSize, true)
	if err != nil {
		return err
	}
	desired.StorageClassName = class
	return nil
}
