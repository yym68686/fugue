package cli

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"unicode/utf8"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type workspaceCommonOptions struct {
	Component string
	Pod       string
	Source    string
}

type workspaceReadOptions struct {
	workspaceCommonOptions
	MaxBytes int
}

type workspaceWriteOptions struct {
	workspaceCommonOptions
	Content  string
	FromFile string
	Mode     int32
	Parents  bool
}

type workspaceMkdirOptions struct {
	workspaceCommonOptions
	Mode    int32
	Parents bool
}

type workspaceRemoveOptions struct {
	workspaceCommonOptions
	Recursive bool
}

func (c *CLI) newWorkspaceCommand() *cobra.Command {
	cmd := c.newFilesystemCommand()
	cmd.Use = "workspace"
	cmd.Aliases = nil
	cmd.Short = "Compatibility alias for app fs"
	return cmd
}

func (c *CLI) newFilesystemCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "fs",
		Aliases: []string{"filesystem"},
		Short:   "Browse persisted storage or the live runtime filesystem",
	}
	cmd.AddCommand(
		c.newWorkspaceListCommand(),
		c.newWorkspaceReadCommand(),
		c.newWorkspaceWriteCommand(),
		c.newWorkspaceMkdirCommand(),
		c.newWorkspaceRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newWorkspaceCompatCommand() *cobra.Command {
	return hideCompatCommand(c.newWorkspaceCommand(), "fugue app fs")
}

func (c *CLI) newWorkspaceListCommand() *cobra.Command {
	opts := workspaceCommonOptions{}
	cmd := &cobra.Command{
		Use:     "ls <app> [path]",
		Aliases: []string{"list", "tree"},
		Short:   "List files in persisted storage or the live runtime filesystem",
		Args:    cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			component, err := normalizeFilesystemComponent(opts.Component)
			if err != nil {
				return err
			}
			requestPath, err := resolveFilesystemPathForCLI(app, optionalArg(args, 1), true, opts.Source)
			if err != nil {
				return err
			}
			response, err := client.GetAppFilesystemTree(app.ID, component, requestPath, opts.Pod)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeWorkspaceTree(c.stdout, response)
		},
	}
	bindWorkspaceCommonFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newWorkspaceReadCommand() *cobra.Command {
	opts := workspaceReadOptions{MaxBytes: 256 * 1024}
	cmd := &cobra.Command{
		Use:     "get <app> <path>",
		Aliases: []string{"read", "cat"},
		Short:   "Read a file from persisted storage or the live runtime filesystem",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			component, err := normalizeFilesystemComponent(opts.Component)
			if err != nil {
				return err
			}
			requestPath, err := resolveFilesystemPathForCLI(app, args[1], false, opts.Source)
			if err != nil {
				return err
			}
			response, err := client.GetAppFilesystemFile(app.ID, component, requestPath, opts.Pod, opts.MaxBytes)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if response.Encoding != "" && response.Encoding != "utf-8" {
				c.progressf("encoding=%s", response.Encoding)
			}
			if response.Truncated {
				c.progressf("truncated=true")
			}
			_, err = io.WriteString(c.stdout, response.Content)
			return err
		},
	}
	cmd.Flags().IntVar(&opts.MaxBytes, "max-bytes", opts.MaxBytes, "Maximum bytes to read before truncation")
	bindWorkspaceCommonFlags(cmd, &opts.workspaceCommonOptions)
	return cmd
}

func (c *CLI) newWorkspaceWriteCommand() *cobra.Command {
	opts := workspaceWriteOptions{Parents: true}
	cmd := &cobra.Command{
		Use:     "put <app> <path>",
		Aliases: []string{"write"},
		Short:   "Write a file into persisted storage or the live runtime filesystem",
		Long: strings.TrimSpace(`
Provide file content with --content or --from-file.

Use --from-file - to read bytes from stdin.
`),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			component, err := normalizeFilesystemComponent(opts.Component)
			if err != nil {
				return err
			}
			requestPath, err := resolveFilesystemPathForCLI(app, args[1], false, opts.Source)
			if err != nil {
				return err
			}
			contentBytes, err := loadWorkspaceWriteContent(opts)
			if err != nil {
				return err
			}
			encoding, content := encodeWorkspaceWriteContent(contentBytes)
			response, err := client.PutAppFilesystemFile(app.ID, component, requestPath, content, encoding, opts.Pod, opts.Mode, opts.Parents)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			pairs := []kvPair{
				{Key: "path", Value: response.Path},
				{Key: "size", Value: fmt.Sprintf("%d", response.Size)},
			}
			if value := response.Kind; value != "" {
				pairs = append(pairs, kvPair{Key: "kind", Value: value})
			}
			if value := formatFileMode(response.Mode); value != "" {
				pairs = append(pairs, kvPair{Key: "mode", Value: value})
			}
			if value := formatModeTime(response.ModifiedAt); value != "" {
				pairs = append(pairs, kvPair{Key: "modified_at", Value: value})
			}
			if value := response.Pod; value != "" {
				pairs = append(pairs, kvPair{Key: "pod", Value: value})
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
	cmd.Flags().StringVar(&opts.Content, "content", "", "Inline file content")
	cmd.Flags().StringVar(&opts.FromFile, "from-file", "", "Read file content from a local path or '-' for stdin")
	cmd.Flags().Int32Var(&opts.Mode, "mode", 0, "Optional file mode")
	cmd.Flags().BoolVar(&opts.Parents, "parents", opts.Parents, "Create parent directories if needed")
	bindWorkspaceCommonFlags(cmd, &opts.workspaceCommonOptions)
	return cmd
}

func (c *CLI) newWorkspaceMkdirCommand() *cobra.Command {
	opts := workspaceMkdirOptions{Parents: true}
	cmd := &cobra.Command{
		Use:   "mkdir <app> <path>",
		Short: "Create a directory in persisted storage or the live runtime filesystem",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			component, err := normalizeFilesystemComponent(opts.Component)
			if err != nil {
				return err
			}
			requestPath, err := resolveFilesystemPathForCLI(app, args[1], false, opts.Source)
			if err != nil {
				return err
			}
			response, err := client.CreateAppFilesystemDirectory(app.ID, component, requestPath, opts.Pod, opts.Mode, opts.Parents)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			pairs := []kvPair{
				{Key: "path", Value: response.Path},
			}
			if value := response.Kind; value != "" {
				pairs = append(pairs, kvPair{Key: "kind", Value: value})
			}
			if value := formatFileMode(response.Mode); value != "" {
				pairs = append(pairs, kvPair{Key: "mode", Value: value})
			}
			if value := formatModeTime(response.ModifiedAt); value != "" {
				pairs = append(pairs, kvPair{Key: "modified_at", Value: value})
			}
			if value := response.Pod; value != "" {
				pairs = append(pairs, kvPair{Key: "pod", Value: value})
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
	cmd.Flags().Int32Var(&opts.Mode, "mode", 0, "Optional directory mode")
	cmd.Flags().BoolVar(&opts.Parents, "parents", opts.Parents, "Create parent directories if needed")
	bindWorkspaceCommonFlags(cmd, &opts.workspaceCommonOptions)
	return cmd
}

func (c *CLI) newWorkspaceRemoveCommand() *cobra.Command {
	opts := workspaceRemoveOptions{}
	cmd := &cobra.Command{
		Use:     "delete <app> <path>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a file or directory from persisted storage or the live runtime filesystem",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			component, err := normalizeFilesystemComponent(opts.Component)
			if err != nil {
				return err
			}
			requestPath, err := resolveFilesystemPathForCLI(app, args[1], false, opts.Source)
			if err != nil {
				return err
			}
			response, err := client.DeleteAppFilesystemPath(app.ID, component, requestPath, opts.Pod, opts.Recursive)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			pairs := []kvPair{
				{Key: "path", Value: response.Path},
				{Key: "deleted", Value: fmt.Sprintf("%t", response.Deleted)},
			}
			if value := response.Pod; value != "" {
				pairs = append(pairs, kvPair{Key: "pod", Value: value})
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
	cmd.Flags().BoolVar(&opts.Recursive, "recursive", false, "Delete directories recursively")
	bindWorkspaceCommonFlags(cmd, &opts.workspaceCommonOptions)
	return cmd
}

func bindWorkspaceCommonFlags(cmd *cobra.Command, opts *workspaceCommonOptions) {
	cmd.Flags().StringVar(&opts.Source, "source", "auto", "Filesystem source: auto, persistent, or live")
	cmd.Flags().StringVar(&opts.Component, "component", "app", "Runtime component. Currently only 'app' is supported")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name")
}

func (c *CLI) resolveWorkspaceApp(client *Client, appRef string) (model.App, error) {
	app, err := c.resolveNamedApp(client, appRef)
	if err != nil {
		return model.App{}, err
	}
	return client.GetApp(app.ID)
}

func optionalArg(args []string, index int) string {
	if index >= len(args) {
		return ""
	}
	return args[index]
}

func resolveFilesystemPathForCLI(app model.App, raw string, allowRoot bool, source string) (string, error) {
	source, err := normalizeFilesystemSource(source)
	if err != nil {
		return "", err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		switch source {
		case "persistent":
			root, err := workspaceRoot(app)
			if err != nil {
				return "", err
			}
			if allowRoot {
				return root, nil
			}
			return "", fmt.Errorf("path is required")
		case "live":
			if allowRoot {
				return "/", nil
			}
			return "", fmt.Errorf("path is required")
		default:
			root, err := workspaceRoot(app)
			if err == nil {
				if allowRoot {
					return root, nil
				}
				return "", fmt.Errorf("path is required")
			}
			if allowRoot {
				return "/", nil
			}
			return "", fmt.Errorf("path is required")
		}
	}

	root, rootErr := workspaceRoot(app)
	switch source {
	case "persistent":
		if rootErr != nil {
			return "", rootErr
		}
		return resolvePersistentCLIPath(root, raw, allowRoot)
	case "live":
		return resolveLiveCLIPath(raw, allowRoot)
	default:
		if strings.HasPrefix(raw, "/") {
			if rootErr == nil && isPathWithinFilesystemRootForCLI(root, raw) {
				return resolvePersistentCLIPath(root, raw, allowRoot)
			}
			return resolveLiveCLIPath(raw, allowRoot)
		}
		if rootErr == nil {
			return resolvePersistentCLIPath(root, raw, allowRoot)
		}
		return resolveLiveCLIPath(raw, allowRoot)
	}
}

func normalizeFilesystemSource(raw string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		value = "auto"
	}
	switch value {
	case "auto", "persistent", "live":
		return value, nil
	default:
		return "", fmt.Errorf("source must be auto, persistent, or live")
	}
}

func normalizeFilesystemComponent(raw string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		value = "app"
	}
	if value != "app" {
		return "", fmt.Errorf("component must be app")
	}
	return value, nil
}

func resolvePersistentCLIPath(root, raw string, allowRoot bool) (string, error) {
	if !strings.HasPrefix(raw, "/") {
		raw = path.Join(root, raw)
	}
	if !isPathWithinFilesystemRootForCLI(root, raw) {
		return "", fmt.Errorf("path must be inside the persistent workspace root %s", root)
	}
	if !allowRoot && path.Clean(raw) == path.Clean(root) {
		return "", fmt.Errorf("path must not be the workspace root")
	}
	return raw, nil
}

func resolveLiveCLIPath(raw string, allowRoot bool) (string, error) {
	if !strings.HasPrefix(raw, "/") {
		raw = path.Join("/", raw)
	}
	if !allowRoot && path.Clean(raw) == "/" {
		return "", fmt.Errorf("path must not be the filesystem root")
	}
	return raw, nil
}

func workspaceRoot(app model.App) (string, error) {
	if app.Spec.Workspace == nil {
		return "", fmt.Errorf("app does not have a persistent workspace")
	}
	root, err := model.NormalizeAppWorkspaceMountPath(app.Spec.Workspace.MountPath)
	if err != nil {
		return "", fmt.Errorf("app workspace mount_path is invalid: %w", err)
	}
	return root, nil
}

func isPathWithinFilesystemRootForCLI(rootPath, targetPath string) bool {
	rootPath = path.Clean(strings.TrimSpace(rootPath))
	targetPath = path.Clean(strings.TrimSpace(targetPath))
	if rootPath == "" || targetPath == "" || rootPath == "." || targetPath == "." {
		return false
	}
	if rootPath == "/" {
		return path.IsAbs(targetPath)
	}
	return model.PathWithinBase(rootPath, targetPath)
}

func loadWorkspaceWriteContent(opts workspaceWriteOptions) ([]byte, error) {
	hasContent := opts.Content != ""
	hasFile := strings.TrimSpace(opts.FromFile) != ""
	switch {
	case hasContent && hasFile:
		return nil, fmt.Errorf("--content and --from-file cannot be used together")
	case !hasContent && !hasFile:
		return nil, fmt.Errorf("either --content or --from-file is required")
	case hasContent:
		return []byte(opts.Content), nil
	}

	fromFile := strings.TrimSpace(opts.FromFile)
	if fromFile == "-" {
		return io.ReadAll(os.Stdin)
	}
	data, err := os.ReadFile(fromFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", fromFile, err)
	}
	return data, nil
}

func encodeWorkspaceWriteContent(content []byte) (string, string) {
	if utf8.Valid(content) {
		return "utf-8", string(content)
	}
	return "base64", base64.StdEncoding.EncodeToString(content)
}
