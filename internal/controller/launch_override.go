package controller

import (
	"context"
	"path"
	"strings"

	"fugue/internal/model"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type imageConfigInspector func(ctx context.Context, imageRef string) (*v1.ConfigFile, error)

func (s *Service) appWithResolvedLaunchOverride(ctx context.Context, app model.App) model.App {
	if len(app.Spec.Command) == 0 || strings.TrimSpace(app.Spec.Image) == "" {
		return app
	}

	inspect := s.inspectManagedImageConfig
	if inspect == nil {
		return app
	}

	configFile, err := inspect(ctx, app.Spec.Image)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("skip launch override inspection for image %s: %v", app.Spec.Image, err)
		}
		return app
	}

	command, args, ok := resolveCompanionLauncherOverride(configFile, app.Spec.Command, app.Spec.Args)
	if !ok {
		return app
	}

	app.Spec.Command = command
	app.Spec.Args = args
	return app
}

func resolveCompanionLauncherOverride(configFile *v1.ConfigFile, command, args []string) ([]string, []string, bool) {
	if len(command) == 0 {
		return nil, nil, false
	}

	launcherPath, ok := companionLauncherPathFromConfig(configFile)
	if !ok {
		return nil, nil, false
	}

	launcherArgs := append([]string(nil), command...)
	launcherArgs = append(launcherArgs, args...)
	return []string{launcherPath}, launcherArgs, true
}

func companionLauncherPathFromConfig(configFile *v1.ConfigFile) (string, bool) {
	if configFile == nil || len(configFile.Config.Entrypoint) == 0 {
		return "", false
	}
	return companionLauncherPathForEntrypoint(configFile.Config.Entrypoint[0])
}

func companionLauncherPathForEntrypoint(entrypoint string) (string, bool) {
	clean := path.Clean(strings.TrimSpace(entrypoint))
	if clean == "" || clean == "." || clean == "/" || !strings.HasPrefix(clean, "/") {
		return "", false
	}

	dir := path.Dir(clean)
	if path.Base(dir) != "process" {
		return "", false
	}

	root := path.Dir(dir)
	if root == "." || root == "/" {
		return "", false
	}

	launcherPath := path.Join(root, "lifecycle", "launcher")
	if launcherPath == clean {
		return "", false
	}
	return launcherPath, true
}
