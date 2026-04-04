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
	Pod string
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
	cmd := &cobra.Command{
		Use:     "workspace",
		Aliases: []string{"fs"},
		Short:   "Browse and edit an app's persistent workspace",
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
	return hideCompatCommand(c.newWorkspaceCommand(), "fugue app workspace")
}

func (c *CLI) newWorkspaceListCommand() *cobra.Command {
	opts := workspaceCommonOptions{}
	cmd := &cobra.Command{
		Use:     "ls <app> [path]",
		Aliases: []string{"list", "tree"},
		Short:   "List files in the persistent workspace",
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
			requestPath, err := resolveWorkspacePath(app, optionalArg(args, 1), true)
			if err != nil {
				return err
			}
			response, err := client.GetAppFilesystemTree(app.ID, requestPath, opts.Pod)
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
		Short:   "Read a file from the persistent workspace",
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
			requestPath, err := resolveWorkspacePath(app, args[1], false)
			if err != nil {
				return err
			}
			response, err := client.GetAppFilesystemFile(app.ID, requestPath, opts.Pod, opts.MaxBytes)
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
		Short:   "Write a file into the persistent workspace",
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
			requestPath, err := resolveWorkspacePath(app, args[1], false)
			if err != nil {
				return err
			}
			contentBytes, err := loadWorkspaceWriteContent(opts)
			if err != nil {
				return err
			}
			encoding, content := encodeWorkspaceWriteContent(contentBytes)
			response, err := client.PutAppFilesystemFile(app.ID, requestPath, content, encoding, opts.Pod, opts.Mode, opts.Parents)
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
		Short: "Create a directory in the persistent workspace",
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
			requestPath, err := resolveWorkspacePath(app, args[1], false)
			if err != nil {
				return err
			}
			response, err := client.CreateAppFilesystemDirectory(app.ID, requestPath, opts.Pod, opts.Mode, opts.Parents)
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
		Short:   "Delete a file or directory from the persistent workspace",
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
			requestPath, err := resolveWorkspacePath(app, args[1], false)
			if err != nil {
				return err
			}
			response, err := client.DeleteAppFilesystemPath(app.ID, requestPath, opts.Pod, opts.Recursive)
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
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name")
	_ = cmd.Flags().MarkHidden("pod")
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

func resolveWorkspacePath(app model.App, raw string, allowRoot bool) (string, error) {
	root, err := workspaceRoot(app)
	if err != nil {
		return "", err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if allowRoot {
			return root, nil
		}
		return "", fmt.Errorf("path is required")
	}
	if !strings.HasPrefix(raw, "/") {
		raw = path.Join(root, raw)
	}
	if !allowRoot && path.Clean(raw) == path.Clean(root) {
		return "", fmt.Errorf("path must not be the workspace root")
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
