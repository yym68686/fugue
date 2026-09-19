package controller

import (
	"context"
	"errors"
	"fugue/internal/config"
	"fugue/internal/store"
	"path/filepath"
	"testing"

	"fugue/internal/model"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

func TestResolveCompanionLauncherOverrideUsesSiblingLauncher(t *testing.T) {
	configFile := &v1.ConfigFile{
		Config: v1.Config{
			Entrypoint: []string{"/opt/runtime/process/web"},
		},
	}

	command, args, ok := resolveCompanionLauncherOverride(
		configFile,
		[]string{"sh", "-lc", "run-service --port 8080"},
		[]string{"--graceful"},
	)
	if !ok {
		t.Fatal("expected companion launcher override")
	}
	if len(command) != 1 || command[0] != "/opt/runtime/lifecycle/launcher" {
		t.Fatalf("unexpected companion launcher command: %#v", command)
	}
	if len(args) != 4 || args[0] != "sh" || args[1] != "-lc" || args[2] != "run-service --port 8080" || args[3] != "--graceful" {
		t.Fatalf("unexpected companion launcher args: %#v", args)
	}
}

func TestResolveCompanionLauncherOverrideSkipsNonProcessEntrypoint(t *testing.T) {
	configFile := &v1.ConfigFile{
		Config: v1.Config{
			Entrypoint: []string{"/usr/local/bin/entrypoint"},
		},
	}

	command, args, ok := resolveCompanionLauncherOverride(
		configFile,
		[]string{"sh", "-lc", "run-service --port 8080"},
		nil,
	)
	if ok {
		t.Fatalf("expected override to be skipped, got command=%#v args=%#v", command, args)
	}
}

func TestResolveCompanionLauncherOverrideSkipsAlreadyWrappedCommand(t *testing.T) {
	configFile := &v1.ConfigFile{
		Config: v1.Config{
			Entrypoint: []string{"/cnb/process/web"},
		},
	}

	command, args, ok := resolveCompanionLauncherOverride(
		configFile,
		[]string{"/cnb/lifecycle/launcher", "sh", "-lc", "python app.py"},
		nil,
	)
	if ok {
		t.Fatalf("expected already wrapped command to be skipped, got command=%#v args=%#v", command, args)
	}
}

func TestCompanionLauncherPathForEntrypointRejectsRelativePaths(t *testing.T) {
	launcherPath, ok := companionLauncherPathForEntrypoint("process/web")
	if ok {
		t.Fatalf("expected relative entrypoint to be rejected, got %q", launcherPath)
	}
}

func TestAppWithResolvedLaunchOverridePrefersManagedRegistryRef(t *testing.T) {
	const (
		pushRef    = "registry.internal.example/fugue-apps/demo:git-abc123"
		runtimeRef = "10.128.0.2:30500/fugue-apps/demo:git-abc123"
	)

	var inspected []string
	svc := &Service{
		registryPushBase: "registry.internal.example",
		registryPullBase: "10.128.0.2:30500",
		inspectManagedImageConfig: func(ctx context.Context, imageRef string) (*v1.ConfigFile, error) {
			inspected = append(inspected, imageRef)
			if imageRef != pushRef {
				t.Fatalf("expected inspect ref %q, got %q", pushRef, imageRef)
			}
			return &v1.ConfigFile{
				Config: v1.Config{
					Entrypoint: []string{"/cnb/process/web"},
				},
			}, nil
		},
	}

	app := model.App{
		Spec: model.AppSpec{
			Image:   runtimeRef,
			Command: []string{"sh", "-lc", "python app.py"},
		},
	}

	resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
	if len(inspected) != 1 || inspected[0] != pushRef {
		t.Fatalf("expected managed registry ref to be inspected first, got %#v", inspected)
	}
	if len(resolved.Spec.Command) != 1 || resolved.Spec.Command[0] != "/cnb/lifecycle/launcher" {
		t.Fatalf("expected launcher command, got %#v", resolved.Spec.Command)
	}
	if len(resolved.Spec.Args) != 3 || resolved.Spec.Args[0] != "sh" || resolved.Spec.Args[1] != "-lc" || resolved.Spec.Args[2] != "python app.py" {
		t.Fatalf("unexpected launcher args: %#v", resolved.Spec.Args)
	}
}

func TestAppWithResolvedLaunchOverrideFallsBackToExternalRuntimeRef(t *testing.T) {
	const (
		sourceRef  = "registry.internal.example/fugue-apps/demo:git-abc123"
		runtimeRef = "ghcr.io/example/runtime:latest"
	)

	var inspected []string
	svc := &Service{
		registryPushBase: "registry.internal.example",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImageConfig: func(ctx context.Context, imageRef string) (*v1.ConfigFile, error) {
			inspected = append(inspected, imageRef)
			switch imageRef {
			case sourceRef:
				return nil, errors.New("dial tcp: i/o timeout")
			case runtimeRef:
				return &v1.ConfigFile{
					Config: v1.Config{
						Entrypoint: []string{"/cnb/process/web"},
					},
				}, nil
			default:
				t.Fatalf("unexpected inspect ref %q", imageRef)
				return nil, nil
			}
		},
	}

	app := model.App{
		Spec: model.AppSpec{
			Image:   runtimeRef,
			Command: []string{"sh", "-lc", "python app.py"},
		},
		Source: &model.AppSource{
			ResolvedImageRef: sourceRef,
		},
	}

	resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
	if len(inspected) != 2 || inspected[0] != sourceRef || inspected[1] != runtimeRef {
		t.Fatalf("expected managed ref fallback order, got %#v", inspected)
	}
	if len(resolved.Spec.Command) != 1 || resolved.Spec.Command[0] != "/cnb/lifecycle/launcher" {
		t.Fatalf("expected launcher command after fallback, got %#v", resolved.Spec.Command)
	}
}

func TestAppWithResolvedLaunchOverrideSkipsPullBaseRuntimeFallback(t *testing.T) {
	const (
		pushRef    = "registry.internal.example/fugue-apps/demo:git-abc123"
		runtimeRef = "registry.fugue.internal:5000/fugue-apps/demo:git-abc123"
	)

	var inspected []string
	svc := &Service{
		registryPushBase: "registry.internal.example",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImageConfig: func(ctx context.Context, imageRef string) (*v1.ConfigFile, error) {
			inspected = append(inspected, imageRef)
			switch imageRef {
			case pushRef:
				return nil, errors.New("manifest unknown")
			case runtimeRef:
				t.Fatalf("controller should not inspect node-only registry pull ref %q", imageRef)
			default:
				t.Fatalf("unexpected inspect ref %q", imageRef)
			}
			return nil, nil
		},
	}

	app := model.App{
		Spec: model.AppSpec{
			Image:   runtimeRef,
			Command: []string{"sh", "-lc", "python app.py"},
		},
	}

	resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
	if len(inspected) != 1 || inspected[0] != pushRef {
		t.Fatalf("expected only managed registry ref to be inspected, got %#v", inspected)
	}
	if len(resolved.Spec.Command) != 3 || resolved.Spec.Command[0] != "sh" {
		t.Fatalf("expected command to remain unchanged, got %#v", resolved.Spec.Command)
	}
}

func TestAppWithResolvedLaunchOverrideUsesBuildpacksFallbackWhenInspectionFails(t *testing.T) {
	svc := &Service{
		registryPushBase: "registry.internal.example",
		inspectManagedImageConfig: func(ctx context.Context, imageRef string) (*v1.ConfigFile, error) {
			return nil, errors.New("manifest unknown")
		},
	}

	app := model.App{
		Spec: model.AppSpec{
			Image:   "registry.internal.example/fugue-apps/demo:upload-abc123",
			Command: []string{"sh", "-lc", "node server.js"},
		},
		BuildSource: &model.AppSource{
			Type:          model.AppSourceTypeUpload,
			BuildStrategy: model.AppBuildStrategyBuildpacks,
		},
	}

	resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
	if len(resolved.Spec.Command) != 1 || resolved.Spec.Command[0] != defaultCNBLauncherPath {
		t.Fatalf("expected buildpacks fallback launcher command, got %#v", resolved.Spec.Command)
	}
	if len(resolved.Spec.Args) != 3 || resolved.Spec.Args[0] != "sh" || resolved.Spec.Args[1] != "-lc" || resolved.Spec.Args[2] != "node server.js" {
		t.Fatalf("unexpected buildpacks fallback launcher args: %#v", resolved.Spec.Args)
	}
}

func TestAppWithResolvedLaunchOverrideUsesBuildpacksFallbackWhenEntrypointIsNotProcess(t *testing.T) {
	svc := &Service{
		registryPushBase: "registry.internal.example",
		inspectManagedImageConfig: func(ctx context.Context, imageRef string) (*v1.ConfigFile, error) {
			return &v1.ConfigFile{
				Config: v1.Config{
					Entrypoint: []string{"/usr/local/bin/entrypoint"},
				},
			}, nil
		},
	}

	app := model.App{
		Spec: model.AppSpec{
			Image:   "registry.internal.example/fugue-apps/demo:upload-abc123",
			Command: []string{"sh", "-lc", "node server.js"},
		},
		BuildSource: &model.AppSource{
			Type:          model.AppSourceTypeUpload,
			BuildStrategy: model.AppBuildStrategyBuildpacks,
		},
	}

	resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
	if len(resolved.Spec.Command) != 1 || resolved.Spec.Command[0] != defaultCNBLauncherPath {
		t.Fatalf("expected buildpacks fallback launcher command, got %#v", resolved.Spec.Command)
	}
}

func TestAppWithResolvedLaunchOverrideDoesNotUseFallbackForDockerImageSource(t *testing.T) {
	svc := &Service{
		registryPushBase: "registry.internal.example",
		inspectManagedImageConfig: func(ctx context.Context, imageRef string) (*v1.ConfigFile, error) {
			return nil, errors.New("manifest unknown")
		},
	}

	app := model.App{
		Spec: model.AppSpec{
			Image:   "registry.internal.example/fugue-apps/demo:image-abc123",
			Command: []string{"sh", "-lc", "node server.js"},
		},
		BuildSource: &model.AppSource{
			Type: model.AppSourceTypeDockerImage,
		},
	}

	resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
	if len(resolved.Spec.Command) != 3 || resolved.Spec.Command[0] != "sh" || resolved.Spec.Command[2] != "node server.js" {
		t.Fatalf("expected docker image source command to remain unchanged, got %#v", resolved.Spec.Command)
	}
	if len(resolved.Spec.Args) != 0 {
		t.Fatalf("expected docker image source args to remain empty, got %#v", resolved.Spec.Args)
	}
}

func TestLaunchOverrideUsesDistributedCacheWithoutLogicalDNSFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   string
		cached bool
		want   []string
	}{
		{name: "physical replica", mode: "distributed", cached: true, want: []string{"192.0.2.10:5000/apps/demo:tag"}},
		{name: "no reachable evidence", mode: "distributed"},
		{name: "explicit registry fallback", mode: "distributed-with-registry-fallback", cached: true, want: []string{"192.0.2.10:5000/apps/demo:tag", "logical.example:5000/apps/demo:tag"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "store.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			app := model.App{ID: "app-demo", TenantID: "tenant-demo", Spec: model.AppSpec{Image: "logical.example:5000/apps/demo:tag", Command: []string{"sh", "-lc", "run-service"}}, BuildSource: &model.AppSource{BuildStrategy: model.AppBuildStrategyBuildpacks}}
			if tc.cached {
				_, err := state.UpsertImageLocation(model.ImageLocation{TenantID: app.TenantID, AppID: app.ID, ImageRef: app.Spec.Image, RuntimeID: "runtime-demo", CacheEndpoint: "http://192.0.2.10:5000", Status: model.ImageLocationStatusPresent})
				if err != nil {
					t.Fatal(err)
				}
			}
			svc := &Service{Store: state, Config: config.ControllerConfig{ImageStoreMode: tc.mode}, registryPushBase: "logical.example:5000", registryPullBase: "logical.example:5000"}
			got := svc.launchOverrideInspectionImageRefs(app)
			if len(got) != len(tc.want) {
				t.Fatalf("refs=%v want=%v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("refs=%v want=%v", got, tc.want)
				}
			}
			calls := 0
			svc.inspectManagedImageConfig = func(_ context.Context, ref string) (*v1.ConfigFile, error) {
				calls++
				if !tc.cached || ref != "192.0.2.10:5000/apps/demo:tag" {
					t.Fatalf("unexpected inspection: %s", ref)
				}
				return &v1.ConfigFile{Config: v1.Config{Entrypoint: []string{"/cnb/process/web"}}}, nil
			}
			resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
			if resolved.Spec.Command[0] != defaultCNBLauncherPath {
				t.Fatalf("launcher lost: %v", resolved.Spec.Command)
			}
			if !tc.cached && calls != 0 {
				t.Fatal("unknown cache was inspected")
			}
		})
	}
}
