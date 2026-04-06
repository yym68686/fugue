package controller

import (
	"context"
	"errors"
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

func TestAppWithResolvedLaunchOverrideFallsBackToRuntimeRef(t *testing.T) {
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
			switch imageRef {
			case pushRef:
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
	}

	resolved := svc.appWithResolvedLaunchOverride(context.Background(), app)
	if len(inspected) != 2 || inspected[0] != pushRef || inspected[1] != runtimeRef {
		t.Fatalf("expected managed ref fallback order, got %#v", inspected)
	}
	if len(resolved.Spec.Command) != 1 || resolved.Spec.Command[0] != "/cnb/lifecycle/launcher" {
		t.Fatalf("expected launcher command after fallback, got %#v", resolved.Spec.Command)
	}
}
