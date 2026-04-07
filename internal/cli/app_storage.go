package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode/utf8"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appStorageView struct {
	AppID             string                            `json:"app_id"`
	Enabled           bool                              `json:"enabled"`
	StorageMode       string                            `json:"storage_mode"`
	Workspace         *model.AppWorkspaceSpec           `json:"workspace,omitempty"`
	PersistentStorage *model.AppPersistentStorageSpec   `json:"persistent_storage,omitempty"`
	Mounts            []model.AppPersistentStorageMount `json:"mounts,omitempty"`
}

func (c *CLI) newAppStorageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "storage",
		Short: "Inspect and manage an app's persistent storage",
	}
	cmd.AddCommand(
		c.newAppStorageShowCommand(),
		c.newAppStorageSetCommand(),
		c.newAppStorageResetCommand(),
		c.newAppStorageDisableCommand(),
	)
	return cmd
}

func (c *CLI) newAppStorageShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show the app persistent storage configuration",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			return c.renderAppStorageState(app, nil, false)
		},
	}
}

func (c *CLI) newAppStorageSetCommand() *cobra.Command {
	opts := struct {
		StorageSize  string
		StorageClass string
		Mounts       []string
		MountFiles   []string
		Wait         bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "set <app>",
		Short: "Create or update persistent storage mounts for an app",
		Long: strings.TrimSpace(`
Use --mount for directories and --mount-file for persisted files seeded from a
local file.

If the app still uses the older workspace model, Fugue migrates that config to
the new persistent_storage representation before applying your changes.
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}

			workingDir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("get working directory: %w", err)
			}

			spec := cloneAppSpec(app.Spec)
			storage, _ := appStorageForMutation(spec)
			storage = cloneAppPersistentStorageSpec(storage)
			if storage == nil {
				storage = &model.AppPersistentStorageSpec{}
			}
			if strings.TrimSpace(opts.StorageSize) != "" {
				storage.StorageSize = strings.TrimSpace(opts.StorageSize)
			}
			if strings.TrimSpace(opts.StorageClass) != "" {
				storage.StorageClassName = strings.TrimSpace(opts.StorageClass)
			}

			if len(opts.Mounts) > 0 || len(opts.MountFiles) > 0 {
				existing := cloneAppPersistentStorageMounts(storage.Mounts)
				nextMounts, err := buildUpdatedAppStorageMounts(workingDir, existing, opts.Mounts, opts.MountFiles)
				if err != nil {
					return err
				}
				storage.Mounts = nextMounts
			}

			if len(storage.Mounts) == 0 {
				storage.Mounts = []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: model.DefaultAppWorkspaceMountPath,
						Mode: 0o755,
					},
				}
			}

			spec.Workspace = nil
			spec.PersistentStorage = storage
			response, err := client.DeployApp(app.ID, &spec)
			if err != nil {
				return err
			}
			finalApp := app
			if opts.Wait {
				waitedApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			} else {
				finalApp.Spec = spec
			}
			return c.renderAppStorageState(finalApp, &response.Operation, false)
		},
	}
	cmd.Flags().StringVar(&opts.StorageSize, "size", "", "Persistent storage size, for example 10Gi")
	cmd.Flags().StringVar(&opts.StorageClass, "class", "", "Persistent storage class")
	cmd.Flags().StringArrayVar(&opts.Mounts, "mount", nil, "Directory mount path to persist, for example /data")
	cmd.Flags().StringArrayVar(&opts.MountFiles, "mount-file", nil, "Persist one file from a local source: <absolute-path>[:mode]=<local-file>")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppStorageResetCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "reset <app>",
		Short: "Reset the app persistent storage on the next deploy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}

			spec := cloneAppSpec(app.Spec)
			resetToken, err := randomResetToken()
			if err != nil {
				return err
			}

			switch {
			case spec.PersistentStorage != nil:
				spec.PersistentStorage = cloneAppPersistentStorageSpec(spec.PersistentStorage)
				spec.PersistentStorage.ResetToken = resetToken
			case spec.Workspace != nil:
				spec.Workspace = cloneAppWorkspaceSpec(spec.Workspace)
				spec.Workspace.ResetToken = resetToken
			default:
				return fmt.Errorf("app does not have persistent storage configured")
			}

			response, err := client.DeployApp(app.ID, &spec)
			if err != nil {
				return err
			}
			finalApp := app
			if opts.Wait {
				waitedApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			} else {
				finalApp.Spec = spec
			}
			return c.renderAppStorageState(finalApp, &response.Operation, true)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppStorageDisableCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "disable <app>",
		Aliases: []string{"off"},
		Short:   "Disable the app persistent storage",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			if app.Spec.PersistentStorage == nil && app.Spec.Workspace == nil {
				return c.renderAppStorageState(app, nil, false)
			}

			spec := cloneAppSpec(app.Spec)
			spec.PersistentStorage = nil
			spec.Workspace = nil
			response, err := client.DeployApp(app.ID, &spec)
			if err != nil {
				return err
			}
			finalApp := app
			if opts.Wait {
				waitedApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			} else {
				finalApp.Spec = spec
			}
			return c.renderAppStorageState(finalApp, &response.Operation, false)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func appStorageForMutation(spec model.AppSpec) (*model.AppPersistentStorageSpec, string) {
	switch {
	case spec.PersistentStorage != nil:
		return cloneAppPersistentStorageSpec(spec.PersistentStorage), "persistent_storage"
	case spec.Workspace != nil:
		storage := legacyWorkspacePersistentStorageSpec(spec.Workspace)
		return storage, "workspace"
	default:
		return nil, "none"
	}
}

func appStorageViewFromSpec(app model.App) appStorageView {
	mode := "disabled"
	workspace := cloneAppWorkspaceSpec(app.Spec.Workspace)
	storage := cloneAppPersistentStorageSpec(app.Spec.PersistentStorage)
	mounts := []model.AppPersistentStorageMount(nil)
	switch {
	case storage != nil:
		mode = "persistent_storage"
		mounts = cloneAppPersistentStorageMounts(storage.Mounts)
	case workspace != nil:
		mode = "workspace"
		converted := legacyWorkspacePersistentStorageSpec(workspace)
		if converted != nil {
			mounts = converted.Mounts
		}
	}
	return appStorageView{
		AppID:             app.ID,
		Enabled:           mode != "disabled",
		StorageMode:       mode,
		Workspace:         workspace,
		PersistentStorage: storage,
		Mounts:            mounts,
	}
}

func (c *CLI) renderAppStorageState(app model.App, operation *model.Operation, resetRequested bool) error {
	view := appStorageViewFromSpec(app)
	if c.wantsJSON() {
		payload := map[string]any{
			"app":     app,
			"storage": view,
		}
		if operation != nil {
			payload["operation"] = operation
		}
		if resetRequested {
			payload["reset_requested"] = true
		}
		return writeJSON(c.stdout, payload)
	}

	pairs := []kvPair{
		{Key: "app_id", Value: view.AppID},
		{Key: "storage_enabled", Value: fmt.Sprintf("%t", view.Enabled)},
		{Key: "storage_mode", Value: view.StorageMode},
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if resetRequested {
		pairs = append(pairs, kvPair{Key: "reset_requested", Value: "true"})
	}

	switch view.StorageMode {
	case "persistent_storage":
		pairs = append(pairs,
			kvPair{Key: "storage_size", Value: strings.TrimSpace(view.PersistentStorage.StorageSize)},
			kvPair{Key: "storage_class", Value: strings.TrimSpace(view.PersistentStorage.StorageClassName)},
			kvPair{Key: "mount_count", Value: fmt.Sprintf("%d", len(view.PersistentStorage.Mounts))},
		)
	case "workspace":
		pairs = append(pairs,
			kvPair{Key: "storage_size", Value: strings.TrimSpace(view.Workspace.StorageSize)},
			kvPair{Key: "storage_class", Value: strings.TrimSpace(view.Workspace.StorageClassName)},
			kvPair{Key: "mount_count", Value: fmt.Sprintf("%d", len(view.Mounts))},
		)
		if view.Workspace != nil {
			pairs = append(pairs, kvPair{Key: "mount_path", Value: strings.TrimSpace(view.Workspace.MountPath)})
		}
	default:
		pairs = append(pairs, kvPair{Key: "mount_count", Value: "0"})
	}

	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if len(view.Mounts) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(c.stdout); err != nil {
		return err
	}
	return writeAppStorageMountTable(c.stdout, view.Mounts)
}

func writeAppStorageMountTable(w io.Writer, mounts []model.AppPersistentStorageMount) error {
	sorted := append([]model.AppPersistentStorageMount(nil), mounts...)
	sort.Slice(sorted, func(i, j int) bool {
		if compare := strings.Compare(sorted[i].Path, sorted[j].Path); compare != 0 {
			return compare < 0
		}
		return strings.Compare(sorted[i].Kind, sorted[j].Kind) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PATH\tKIND\tSECRET\tMODE"); err != nil {
		return err
	}
	for _, mount := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%t\t%s\n", mount.Path, mount.Kind, mount.Secret, formatFileMode(mount.Mode)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func buildUpdatedAppStorageMounts(workingDir string, existing []model.AppPersistentStorageMount, mountSpecs, mountFileSpecs []string) ([]model.AppPersistentStorageMount, error) {
	mounts := cloneAppPersistentStorageMounts(existing)
	for _, raw := range mountSpecs {
		mount, err := parseAppStorageDirectoryMount(raw)
		if err != nil {
			return nil, err
		}
		mounts, err = upsertAppStorageMount(mounts, mount)
		if err != nil {
			return nil, err
		}
	}
	for _, raw := range mountFileSpecs {
		mount, err := parseAppStorageFileMount(workingDir, raw)
		if err != nil {
			return nil, err
		}
		mounts, err = upsertAppStorageMount(mounts, mount)
		if err != nil {
			return nil, err
		}
	}
	return mounts, nil
}

func parseAppStorageDirectoryMount(raw string) (model.AppPersistentStorageMount, error) {
	pathValue, err := model.NormalizeAppPersistentStorageMountPath(model.AppPersistentStorageMountKindDirectory, strings.TrimSpace(raw))
	if err != nil {
		return model.AppPersistentStorageMount{}, err
	}
	return model.AppPersistentStorageMount{
		Kind: model.AppPersistentStorageMountKindDirectory,
		Path: pathValue,
		Mode: 0o755,
	}, nil
}

func parseAppStorageFileMount(workingDir, raw string) (model.AppPersistentStorageMount, error) {
	target, localFile, ok := strings.Cut(strings.TrimSpace(raw), "=")
	if !ok || strings.TrimSpace(localFile) == "" {
		return model.AppPersistentStorageMount{}, fmt.Errorf("mount file %q must use <absolute-path>[:mode]=<local-file>", raw)
	}
	pathValue, modeValue, err := parsePathWithOptionalMode(target, 0o644)
	if err != nil {
		return model.AppPersistentStorageMount{}, err
	}
	content, err := readUTF8LocalFile(workingDir, localFile)
	if err != nil {
		return model.AppPersistentStorageMount{}, err
	}
	return model.AppPersistentStorageMount{
		Kind:        model.AppPersistentStorageMountKindFile,
		Path:        pathValue,
		SeedContent: content,
		Mode:        modeValue,
	}, nil
}

func upsertAppStorageMount(existing []model.AppPersistentStorageMount, mount model.AppPersistentStorageMount) ([]model.AppPersistentStorageMount, error) {
	next := make([]model.AppPersistentStorageMount, 0, len(existing)+1)
	replaced := false
	for _, current := range existing {
		if sameAppStorageMount(current, mount) {
			next = append(next, mount)
			replaced = true
			continue
		}
		if model.AppPersistentStorageMountPathConflict(current, mount) {
			return nil, fmt.Errorf("mount path %s overlaps existing mount %s", mount.Path, current.Path)
		}
		next = append(next, current)
	}
	if !replaced {
		next = append(next, mount)
	}
	sort.Slice(next, func(i, j int) bool {
		if compare := strings.Compare(next[i].Path, next[j].Path); compare != 0 {
			return compare < 0
		}
		return strings.Compare(next[i].Kind, next[j].Kind) < 0
	})
	return next, nil
}

func sameAppStorageMount(left, right model.AppPersistentStorageMount) bool {
	return strings.EqualFold(strings.TrimSpace(left.Kind), strings.TrimSpace(right.Kind)) &&
		strings.EqualFold(strings.TrimSpace(left.Path), strings.TrimSpace(right.Path))
}

func parsePathWithOptionalMode(raw string, defaultMode int32) (string, int32, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return "", 0, fmt.Errorf("target path is required")
	}
	modeValue := defaultMode
	pathValue := target
	if strings.HasPrefix(target, "/") {
		lastColon := strings.LastIndex(target, ":")
		if lastColon > 0 {
			candidatePath := strings.TrimSpace(target[:lastColon])
			candidateMode := strings.TrimSpace(target[lastColon+1:])
			if strings.HasPrefix(candidatePath, "/") && candidateMode != "" {
				parsedMode, err := parseFileMode(candidateMode)
				if err != nil {
					return "", 0, fmt.Errorf("invalid mode %q: %w", candidateMode, err)
				}
				pathValue = candidatePath
				modeValue = parsedMode
			}
		}
	}
	normalizedPath, err := model.NormalizeAppPersistentStorageMountPath(model.AppPersistentStorageMountKindFile, pathValue)
	if err != nil {
		return "", 0, err
	}
	return normalizedPath, modeValue, nil
}

func parseFileMode(raw string) (int32, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 8, 32)
	if err != nil {
		return 0, err
	}
	if value < 0 || value > 0o777 {
		return 0, fmt.Errorf("mode must be between 000 and 777")
	}
	return int32(value), nil
}

func readUTF8LocalFile(workingDir, rawPath string) (string, error) {
	localPath := strings.TrimSpace(rawPath)
	if localPath == "" {
		return "", fmt.Errorf("local file is required")
	}
	if !filepath.IsAbs(localPath) {
		localPath = filepath.Join(workingDir, localPath)
	}
	data, err := os.ReadFile(localPath)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", localPath, err)
	}
	if !utf8.Valid(data) {
		return "", fmt.Errorf("file %s is not valid UTF-8", localPath)
	}
	return string(data), nil
}
