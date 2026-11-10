package controller

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestInternalImageImportVerifiesSourceAndPreparesDeployTarget(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"source-only", "target-present", "no-source", "wrong-digest", "missing-blob", "foreign-source", "pull-base", "unresolved-shared"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			state := store.New(filepath.Join(t.TempDir(), "store.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("Tenant")
			if err != nil {
				t.Fatal(err)
			}
			project, err := state.CreateProject(tenant.ID, "project", "")
			if err != nil {
				t.Fatal(err)
			}
			_, secret, err := state.CreateNodeKey(tenant.ID, "default")
			if err != nil {
				t.Fatal(err)
			}
			var updaters []model.NodeUpdater
			for i, node := range []string{"worker-a", "worker-b"} {
				updater, _, err := state.EnrollNodeUpdater(secret, node, []string{"https://203.0.113.10:9443", "https://203.0.113.20:9443"}[i], nil, node, "machine-"+node, "v2", "join-v2", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReplicateAppImage})
				if err != nil {
					t.Fatal(err)
				}
				updaters = append(updaters, updater)
			}
			const ref = "registry.fugue.internal:5000/fugue-apps/demo:build-example"
			const sourceEndpoint = "http://203.0.113.10:5000"
			const targetEndpoint = "http://203.0.113.20:5000"
			const digest = imageCacheVerifyTestDigest
			imageTenant := tenant.ID
			if scenario == "foreign-source" {
				other, err := state.CreateTenant("Other tenant")
				if err != nil {
					t.Fatal(err)
				}
				imageTenant = other.ID
			}
			_, err = state.UpsertImage(model.Image{TenantID: imageTenant, AppID: "template", ImageRef: ref, CanonicalDigest: digest, LifecycleState: model.ImageLifecycleAvailable})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ { // Duplicate physical evidence must not repeat graph hashing.
				_, err := state.UpsertImageLocation(model.ImageLocation{TenantID: imageTenant, ImageRef: ref, Digest: digest, NodeID: []string{"", "machine-worker-a"}[i], RuntimeID: updaters[0].RuntimeID, ClusterNodeName: "worker-a", CacheEndpoint: sourceEndpoint, Status: model.ImageLocationStatusPresent})
				if err != nil {
					t.Fatal(err)
				}
			}
			queuedRef, pullBase := ref, "registry.fugue.internal:5000"
			if scenario == "pull-base" {
				pullBase = "registry.pull.example"
				queuedRef = strings.Replace(ref, "registry.fugue.internal:5000", pullBase, 1)
			}
			runtimeID := updaters[1].RuntimeID
			if scenario == "unresolved-shared" {
				runtimeID = model.DefaultManagedRuntimeID
			}
			app, err := state.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Replicas: 1, RuntimeID: runtimeID, Ports: []int{7777}, Env: map[string]string{"EXAMPLE": "preserved"}}, model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: queuedRef}, model.AppRoute{})
			if err != nil {
				t.Fatal(err)
			}
			spec, source := app.Spec, *app.Source
			op, err := state.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: &spec, DesiredSource: &source})
			if err != nil {
				t.Fatal(err)
			}
			importer := &recordingImporter{}
			targetReady := scenario == "target-present"
			var calls []string
			svc := &Service{Store: state, Logger: log.New(io.Discard, "", 0), Config: config.ControllerConfig{ImageStoreMode: "distributed"}, importer: importer, registryPushBase: "registry.fugue.internal:5000", registryPullBase: pullBase,
				inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
					t.Fatal("internal image must not use the central registry")
					return false, nil, nil
				},
				resolveManagedImageDigestRef: func(context.Context, string) (string, error) {
					t.Fatal("internal image must not use the central registry")
					return "", nil
				},
			}
			if scenario == "pull-base" {
				svc.builderRegistryPushBase = "127.0.0.1:5000"
			}
			svc.verifyDestinationImageCache = func(_ context.Context, endpoint, imageRef string) (destinationImageCacheVerification, error) {
				calls = append(calls, endpoint)
				if imageRef != imageRefWithDigest(ref, digest) {
					t.Fatalf("verification was not pinned: %s", imageRef)
				}
				if endpoint == targetEndpoint && !targetReady || scenario == "no-source" {
					return destinationImageCacheVerification{}, &destinationImageCacheHTTPError{StatusCode: 404, Detail: "MANIFEST_UNKNOWN"}
				}
				if scenario == "missing-blob" {
					return destinationImageCacheVerification{}, &destinationImageCacheHTTPError{StatusCode: 404, Detail: "local blob sha256:bbbb status=404"}
				}
				_, target, _ := managedImageRepoTarget(imageRef)
				actualDigest := digest
				if scenario == "wrong-digest" {
					actualDigest = "sha256:" + strings.Repeat("c", 64)
				}
				return destinationImageCacheVerification{Repo: "fugue-apps/demo", Target: target, Available: true, CanonicalDigest: actualDigest, ReferencedBlobs: []string{imageCacheVerifyTestBlob}, ReferencedBlobBytes: 128}, nil
			}
			err = svc.executeManagedImportOperation(context.Background(), op, app)
			wantSuccess := scenario == "source-only" || scenario == "target-present" || scenario == "pull-base" || scenario == "unresolved-shared"
			if (err == nil) != wantSuccess {
				t.Fatalf("success=%t error=%v", wantSuccess, err)
			}
			if importer.dockerImageReq != nil {
				t.Fatal("internal reference sent to external importer")
			}
			ops, err := state.ListOperationsByApp(tenant.ID, false, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			var deploy model.Operation
			for _, candidate := range ops {
				if candidate.Type == model.OperationTypeDeploy {
					if deploy.ID != "" {
						t.Fatal("duplicate deploy")
					}
					deploy = candidate
				}
			}
			if !wantSuccess {
				if deploy.ID != "" {
					t.Fatal("unverified image queued deploy")
				}
				if scenario == "foreign-source" && len(calls) != 0 {
					t.Fatal("cross-tenant cache was accessed")
				}
				if scenario == "no-source" && !reflect.DeepEqual(calls, []string{targetEndpoint, sourceEndpoint}) {
					t.Fatalf("duplicate source checks: %v", calls)
				}
				return
			}
			if deploy.DesiredSpec == nil || deploy.DesiredSource == nil {
				t.Fatalf("missing deploy: %+v", deploy)
			}
			if deploy.DesiredSpec.RuntimeID != spec.RuntimeID || deploy.DesiredSpec.Replicas != spec.Replicas || !reflect.DeepEqual(deploy.DesiredSpec.Ports, spec.Ports) || !reflect.DeepEqual(deploy.DesiredSpec.Env, spec.Env) {
				t.Fatalf("placement or spec changed: %+v", deploy.DesiredSpec)
			}
			if deploy.DesiredSpec.Image != imageRefWithDigest(strings.Replace(ref, "registry.fugue.internal:5000", pullBase, 1), digest) || deploy.DesiredSource.ImageRef != queuedRef {
				t.Fatalf("unpinned or changed source: %+v %+v", deploy.DesiredSpec, deploy.DesiredSource)
			}
			if scenario == "target-present" {
				if !reflect.DeepEqual(calls, []string{targetEndpoint}) {
					t.Fatalf("unnecessary source access: %v", calls)
				}
				return
			}
			evidence, err := state.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, OperationID: op.ID})
			if err != nil {
				t.Fatal(err)
			}
			var failedTarget, verifiedSource bool
			for _, fact := range evidence {
				if fact.Type == "image_cache_verification" && fact.Payload["outcome"] == "failed" && fact.NodeName == "worker-b" {
					failedTarget = true
				}
				if fact.Type == "image_cache_verification" && fact.Payload["outcome"] == "completed" && fact.NodeName == "worker-a" {
					verifiedSource = true
				}
			}
			if (!failedTarget && scenario != "unresolved-shared") || !verifiedSource {
				t.Fatalf("missing durable cache evidence: %+v", evidence)
			}
			replicas, err := state.ListImageReplicas(model.ImageReplicaFilter{TenantID: tenant.ID, Status: model.ImageReplicaStatusPresent})
			if err != nil {
				t.Fatal(err)
			}
			for _, replica := range replicas {
				if replica.ClusterNodeName == "worker-b" {
					t.Fatal("missing target falsely recorded present")
				}
			}
			deployedApp := app
			deployedApp.Spec, deployedApp.Source = *deploy.DesiredSpec, deploy.DesiredSource
			target := deployImageTarget{RuntimeID: updaters[1].RuntimeID, ClusterNodeName: "worker-b"}
			for i := 0; i < 2; i++ {
				available, err := svc.deployImageRefAvailable(context.Background(), deployedApp, target, deploy.DesiredSource.ResolvedImageRef)
				if available || !errors.Is(err, errDeployImageReplicationPending) {
					t.Fatalf("deployment must wait for target copy: available=%t err=%v", available, err)
				}
			}
			tasks, err := state.ListImageReplicationTasks(model.ImageReplicationTaskFilter{TenantID: tenant.ID})
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 || tasks[0].Priority != model.ImageReplicationPriorityDeployBlocking || tasks[0].TargetClusterNodeName != "worker-b" || tasks[0].ImageID == "" {
				t.Fatalf("replication not deduplicated or wrong target: %+v", tasks)
			}
			targetReady = true
			available, err := svc.deployImageRefAvailable(context.Background(), deployedApp, target, deploy.DesiredSource.ResolvedImageRef)
			if err != nil || !available {
				t.Fatalf("verified target did not unblock deployment: %t %v", available, err)
			}
		})
	}
}
