package controller

import (
	"testing"

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

func TestCompanionLauncherPathForEntrypointRejectsRelativePaths(t *testing.T) {
	launcherPath, ok := companionLauncherPathForEntrypoint("process/web")
	if ok {
		t.Fatalf("expected relative entrypoint to be rejected, got %q", launcherPath)
	}
}
