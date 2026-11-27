package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/pgimage"
	migration "fugue/internal/pgimagemigration"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

var errPostgresImageMigrationUnsettled = migration.ErrUnsettled

func (s *Service) executePostgresImageMigration(ctx context.Context, op model.Operation, app model.App) error {
	return migration.Execute(ctx, imageMigrationLedger{s: s, op: op}, func(ctx context.Context) (migration.Target, migration.Platform, error) {
		var zero migration.Target
		if !s.Config.KubectlApply {
			return zero, nil, fmt.Errorf("database image migration requires Kubernetes apply mode")
		}
		target, err := store.PostgresImageMigrationTargetForApp(app, op.ServiceID)
		if err != nil {
			return zero, nil, err
		}
		if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
			return zero, nil, fmt.Errorf("migration target specification is missing")
		}
		image, err := pgimage.Target(op.DesiredSpec.Postgres.Image)
		if err != nil {
			return zero, nil, err
		}
		budget := op.DesiredSpec.Postgres.ImageMigrationDrainBudgetSeconds
		if !pgimage.ValidDrainBudget(budget) || store.PostgresImageMigrationPlanHash(app, *target.Service, image, budget) != op.DesiredSpec.Postgres.ImageMigrationConfigHash {
			return zero, nil, fmt.Errorf("configuration changed since migration admission")
		}
		catalog, err := pgimage.ApprovedCatalog()
		if err != nil {
			return zero, nil, err
		}
		client, err := s.kubeClient()
		if err != nil {
			return zero, nil, err
		}
		if err = client.verifyImageMigrationOperator(ctx, catalog); err != nil {
			return zero, nil, err
		}
		ns, name := runtime.NamespaceForTenant(app.TenantID), target.Postgres.ServiceName
		spec := migration.Target{OperationID: op.ID, Namespace: ns, Cluster: name, Image: image, Budget: budget, Resources: target.Postgres.RuntimeResources}
		return spec, imageMigrationPlatform{s: s, client: client, app: app, op: op, spec: spec, postgres: target.Postgres, catalog: catalog}, nil
	})
}

type imageMigrationLedger struct {
	s  *Service
	op model.Operation
}

func (a imageMigrationLedger) ReadIntent() (intent *model.OperationEvidence, aborted bool, err error) {
	evidence, err := a.s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: a.op.TenantID, OperationID: a.op.ID, Types: []string{pgimage.IntentEvidence, pgimage.DrainAbortEvidence}, IncludeMigrationLedger: true, Limit: 3})
	if err != nil {
		return nil, false, err
	}
	for i := range evidence {
		switch evidence[i].Type {
		case pgimage.IntentEvidence:
			intent = &evidence[i]
		case pgimage.DrainAbortEvidence:
			aborted = true
		}
	}
	return intent, aborted, nil
}

func (a imageMigrationLedger) WriteWitness(kind string, w pgimage.Witness) error {
	return a.s.imageMigrationEvidence(a.op, kind, w)
}

func (a imageMigrationLedger) CheckOwnership() error {
	return a.s.ensureManagedPostgresLifecycleOperationOwned(a.op)
}

func (a imageMigrationLedger) IsInactive(err error) bool {
	return errors.Is(err, errOperationNoLongerActive)
}

func (a imageMigrationLedger) Progress(message string) {
	_, _ = a.s.Store.UpdateOperationProgress(a.op.ID, message)
}

func (a imageMigrationLedger) Complete() error {
	_, err := a.s.Store.CompleteManagedOperationGuarded(a.op, nil, "", "database image migrated; original storage, placement and resource envelope verified; bootstrap init removed", nil, nil, nil)
	return err
}

type imageMigrationPlatform struct {
	s        *Service
	client   *kubeClient
	app      model.App
	op       model.Operation
	spec     migration.Target
	postgres model.AppPostgresSpec
	catalog  pgimage.Catalog
}

func (a imageMigrationPlatform) Capture(ctx context.Context) (pgimage.Witness, error) {
	return a.s.captureImageWitness(ctx, a.client, a.app, a.spec.Namespace, a.op.ServiceID, a.spec.Image, a.postgres, a.catalog)
}

func (a imageMigrationPlatform) Observe(ctx context.Context) (migration.Observation, error) {
	cluster, pod, snapshot, err := a.client.observeImageMigration(ctx, a.spec.Namespace, a.op.ServiceID, a.postgres)
	if err != nil {
		return migration.Observation{}, err
	}
	spec := imageObject(cluster["spec"])
	currentImage := imageString(spec, "imageName")
	probe := a.s.postgresImageMigrationProbe
	if probe == nil {
		probe = probePostgresImageMigration
	}
	return migration.Observation{
		Cluster: cluster, CurrentImage: currentImage, PodUID: snapshot.Observation.PodUID, PodImage: imageString(imageContainer(pod), "image"),
		Verify: func(w pgimage.Witness) error { return validateImageWitness(w, cluster, pod, snapshot) },
		VerifyVolumes: func(ctx context.Context, w pgimage.Witness) error {
			var sharedNode *kubeNode
			if w.SharedNodeUID != "" {
				placement, err := a.s.Store.GetRuntime(a.postgres.RuntimeID)
				if err != nil {
					return fmt.Errorf("recheck configured shared database runtime: %w", err)
				}
				node, err := a.client.imageMigrationSharedNode(ctx, placement, a.postgres, spec, w.Node, w.SharedNodeUID)
				if err != nil {
					return err
				}
				if currentImage == w.SourceImage {
					if err := validateImageMigrationNode(node, true); err != nil {
						return err
					}
				}
				sharedNode = &node
			}
			volumes, err := a.client.imageMigrationVolumes(ctx, a.spec.Namespace, pod, sharedNode)
			if err != nil {
				return err
			}
			if pgimage.Hash(volumes) != pgimage.Hash(w.Volumes) {
				return fmt.Errorf("database PVC/PV identity or specification changed")
			}
			return nil
		},
		VerifyReplacement: func(image string) error {
			for _, status := range imageObjects(imageObject(pod["status"])["containerStatuses"]) {
				if imageString(status, "name") == managedPostgresMainContainerName && strings.HasSuffix(imageString(status, "imageID"), strings.Split(image, "@")[1]) {
					if len(imageObjects(imageObject(pod["spec"])["initContainers"])) != 0 {
						return fmt.Errorf("baked-manager primary unexpectedly has init containers")
					}
					command, _ := imageContainer(pod)["command"].([]any)
					if len(command) == 0 || command[0] != "/usr/local/bin/fugue-cnpg-manager" {
						return fmt.Errorf("primary does not execute the baked manager")
					}
					return nil
				}
			}
			return fmt.Errorf("replacement database image digest does not match the verified target")
		},
		Probe: func(ctx context.Context) error {
			return probe(ctx, a.client, a.spec.Namespace, a.postgres, snapshot, pod)
		},
	}, nil
}

func (a imageMigrationPlatform) RecoverBootstrap(ctx context.Context, w pgimage.Witness) (bool, error) {
	repaired, err := a.client.recoverLegacyImageMigrationPodPatch(ctx, w, a.postgres.RuntimeID)
	if err == nil && repaired && a.s.Logger != nil {
		a.s.Logger.Printf("database image migration %s removed a stale bootstrap-only CNPG podPatch during recovery", a.op.ID)
	}
	return repaired, err
}

func (a imageMigrationPlatform) ConfigureDrain(w *pgimage.Witness, cluster map[string]any) error {
	return configurePostgresImageDrain(w, cluster, a.op.ID, a.spec.Budget)
}

func (a imageMigrationPlatform) ObserveDrain(ctx context.Context, w pgimage.Witness) (migration.DrainState, error) {
	cluster, err := a.client.imageMigrationObject(ctx, cloudNativePGClusterAPIPath(a.spec.Namespace, a.spec.Cluster))
	if err != nil {
		return migration.DrainState{}, err
	}
	staged, err := verifyImageDrainCluster(w, cluster)
	return migration.DrainState{Cluster: cluster, Staged: staged, OnSource: imageString(imageObject(cluster["spec"]), "imageName") == w.SourceImage}, err
}

func (a imageMigrationPlatform) DrainStaged(w pgimage.Witness, cluster map[string]any) (bool, error) {
	return verifyImageDrainCluster(w, cluster)
}

func (a imageMigrationPlatform) ReplacementExists(ctx context.Context, w pgimage.Witness) bool {
	pod, err := a.client.imageMigrationObject(ctx, imageDrainPodPath(w))
	return err == nil && imageDrainReplacement(pod, w)
}

func (a imageMigrationPlatform) SourceStillPresent(ctx context.Context, w pgimage.Witness) bool {
	pod, err := a.client.imageMigrationObject(ctx, imageDrainPodPath(w))
	return err == nil && imageString(imageObject(pod["metadata"]), "uid") == w.PodUID
}

func (a imageMigrationPlatform) SampleDrain(ctx context.Context, w pgimage.Witness) pgimage.DrainObservation {
	return a.s.imageDrainSample(ctx, a.client, w)
}

func (a imageMigrationPlatform) RecordDrain(sample pgimage.DrainObservation) {
	a.s.recordImageDrainSample(a.op, sample)
}

func (a imageMigrationPlatform) StageDrain(ctx context.Context, w pgimage.Witness, cluster map[string]any) error {
	return a.client.patchImageDrain(ctx, w, cluster, true)
}

func (a imageMigrationPlatform) AbortDrain(ctx context.Context, w pgimage.Witness, cluster map[string]any, recorded bool, reason string) error {
	return a.s.abortImageDrain(ctx, a.client, a.op, w, cluster, a.postgres, recorded, reason)
}

func (a imageMigrationPlatform) RestoreDrain(ctx context.Context, w pgimage.Witness, cluster map[string]any) error {
	return a.s.restoreImageDrain(ctx, a.client, a.op, w, cluster)
}

func (a imageMigrationPlatform) BeginEndpointTiming(ctx context.Context, w pgimage.Witness, resuming bool) migration.EndpointTiming {
	return a.s.observeImageEndpoint(ctx, a.op, a.client, w, a.postgres, resuming)
}

func (a imageMigrationPlatform) PatchImage(ctx context.Context, w pgimage.Witness, cluster map[string]any) error {
	return a.client.patchImageMigration(ctx, w, cluster)
}
