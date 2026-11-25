package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/localpvsafety"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Cold recovery never allocates capacity on the source node. Healthy sources
// use normal localization; durable cold state resumes regardless of CNPG phase.
type recoveryOperationContextKey struct{}

func (s *Service) executeManagedDatabaseRecoveryOperation(ctx context.Context, op model.Operation, app model.App) error {
	ctx = context.WithValue(ctx, recoveryOperationContextKey{}, true)
	timeout := s.Config.ManagedAppRolloutTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil {
		return err
	}
	if target == nil {
		return fmt.Errorf("storage recovery requires a managed database")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	namespace, name := runtime.NamespaceForTenant(app.TenantID), target.Postgres.ServiceName
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, name)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("recovery cluster %s/%s not found", namespace, name)
	}
	if cluster.Status.CurrentPrimary == "" {
		return fmt.Errorf("recovery requires an observed primary; refusing to choose a data source")
	}
	pg := model.CloneAppPostgresSpec(&target.Postgres)
	if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		pg = model.CloneAppPostgresSpec(op.DesiredSpec.Postgres)
	}
	// The CNPG admission webhook forbids shrinking even a failed staged intent.
	liveSize, err := managedPostgresLiveStorageSize(ctx, client, namespace, name)
	if err != nil {
		return err
	}
	pg.StorageSize, err = maximumRecoveryStorageSize(pg.StorageSize, cluster.Spec.Storage.Size, liveSize)
	if err != nil {
		return err
	}
	if strings.TrimSpace(pg.StorageClassName) == "" {
		pg.StorageClassName = cluster.Spec.Storage.StorageClass
	}
	desired := app.Spec
	desired.Postgres = pg
	op.DesiredSpec = &desired

	targetRuntime, err := s.Store.GetRuntime(pg.RuntimeID)
	if err != nil {
		return err
	}
	pg.PrimaryNodeName, err = recoveryStorageTargetNode(ctx, client, targetRuntime, pg.StorageClassName, pg.PrimaryNodeName)
	if err != nil {
		return err
	}
	coldState, _, err := loadColdState(ctx, client, namespace, op.ServiceID)
	if err != nil {
		return err
	}
	primary, primaryFound, err := client.getPod(ctx, namespace, cluster.Status.CurrentPrimary)
	if err != nil {
		return err
	}
	if coldState != nil || (primaryFound && coldPrimaryUnstarted(primary)) || pg.Instances > 1 || (cluster.Status.ReadyInstances == 0 && (strings.EqualFold(strings.TrimSpace(cluster.Status.Phase), "Not enough disk space") || cluster.Metadata.Annotations["cnpg.io/fencedInstances"] == `["*"]`)) {
		// A disk-full source cannot be rescued by expanding its source LocalPV
		// when the requested operation is an offline move. Copy the fenced source
		// directly to the declared target storage class instead. The old source
		// rescue path is intentionally not reachable from this operation: it would
		// mutate the source node before the target database exists.
		return s.executeManagedDatabaseColdMigration(ctx, op, app, cluster, *pg, targetRuntime)
	}
	return s.executeManagedDatabaseLocalizeOperation(ctx, op, app)
}

func maximumRecoveryStorageSize(values ...string) (string, error) {
	var largest resource.Quantity
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		q, err := resource.ParseQuantity(value)
		if err != nil || q.Sign() <= 0 {
			return "", fmt.Errorf("invalid observed recovery storage size %q", value)
		}
		if q.Cmp(largest) > 0 {
			largest = q
		}
	}
	if largest.Sign() <= 0 {
		return "", fmt.Errorf("no observed recovery storage capacity")
	}
	return largest.String(), nil
}

func recoveryPoolTarget(inventory model.LocalPVInventory, growth int64) (int64, error) {
	if inventory.ImageSizeBytes <= 0 || inventory.PVSizeBytes <= 0 || inventory.PVFreeBytes < 0 || inventory.PVFreeBytes > inventory.PVSizeBytes || growth < 0 {
		return 0, fmt.Errorf("invalid LocalPV capacity observation")
	}
	if inventory.PVFreeBytes >= growth+localpvsafety.RequiredFreeBytes(inventory.PVSizeBytes) {
		return inventory.ImageSizeBytes, nil
	}
	// Account for the reserve growing with the pool. Round to GiB for auditable
	// host tasks and fail instead of exceeding the task's bounded growth policy.
	for extra := int64(1 << 30); extra <= 64<<30; extra += 1 << 30 {
		if inventory.PVFreeBytes+extra >= growth+localpvsafety.RequiredFreeBytes(inventory.PVSizeBytes+extra) {
			return inventory.ImageSizeBytes + extra, nil
		}
	}
	return 0, fmt.Errorf("required LocalPV pool growth exceeds 64 GiB recovery bound")
}

func waitRecoveryCapability(ctx context.Context, observe func() (bool, error), wait func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		supported, err := observe()
		if err != nil {
			return err
		}
		if supported {
			return nil
		}
		if err := wait(ctx); err != nil {
			return err
		}
	}
}

func runRecoveryPoolStages(run func(dryRun bool) error) error {
	if err := run(true); err != nil {
		return fmt.Errorf("source pool capacity preflight failed; no pool expansion queued: %w", err)
	}
	return run(false)
}
