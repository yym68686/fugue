package cli

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type filesMutationOptions struct {
	Wait bool
}

type fileWriteOptions struct {
	filesMutationOptions
	Content  string
	FromFile string
	Secret   bool
	Mode     int32
}

type filesResult struct {
	AppID          string           `json:"app_id,omitempty"`
	Files          []model.AppFile  `json:"files"`
	Operation      *model.Operation `json:"operation,omitempty"`
	AlreadyCurrent bool             `json:"already_current,omitempty"`
}

func (c *CLI) newFilesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Inspect and update declarative app config files",
		Long: strings.TrimSpace(`
Use "config" for declarative files that are applied on the next deploy.

Use "workspace" for direct reads and writes inside a persistent runtime workspace.
`),
	}
	cmd.AddCommand(
		c.newFilesListCommand(),
		c.newFilesReadCommand(),
		c.newFilesWriteCommand(),
		c.newFilesRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newFilesCompatCommand() *cobra.Command {
	cmd := c.newFilesCommand()
	cmd.Use = "files"
	cmd.Short = "Compatibility alias for app config"
	cmd.Long = strings.TrimSpace(`
Compatibility alias for declarative app config files.

Prefer "fugue app config" for the primary UX.
`)
	return hideCompatCommand(cmd, "fugue app config")
}

func (c *CLI) newFilesListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <app>",
		Aliases: []string{"list"},
		Short:   "List declarative files configured on an app",
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
			response, err := client.GetAppFiles(app.ID)
			if err != nil {
				return err
			}
			return c.renderFilesListResult(filesResult{
				AppID: app.ID,
				Files: response.Files,
			})
		},
	}
}

func (c *CLI) newFilesReadCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "get <app> <absolute-path>",
		Aliases: []string{"read", "cat"},
		Short:   "Read one declarative app file",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			filePath, err := normalizeAppFilePath(args[1])
			if err != nil {
				return err
			}
			response, err := client.GetAppFiles(app.ID)
			if err != nil {
				return err
			}
			appFile, ok := findAppFileByPath(response.Files, filePath)
			if !ok {
				return fmt.Errorf("file %q not found", filePath)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id": app.ID,
					"file":   appFile,
				})
			}
			if appFile.Secret {
				c.progressf("secret=true")
			}
			_, err = fmt.Fprint(c.stdout, appFile.Content)
			return err
		},
	}
}

func (c *CLI) newFilesWriteCommand() *cobra.Command {
	opts := fileWriteOptions{
		filesMutationOptions: filesMutationOptions{Wait: true},
	}
	cmd := &cobra.Command{
		Use:     "put <app> <absolute-path>",
		Aliases: []string{"write"},
		Short:   "Create or update one declarative app file",
		Long: strings.TrimSpace(`
Provide file content with --content or --from-file.

This updates the app spec and triggers a deploy operation rather than writing
directly into a running container.
`),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			filePath, err := normalizeAppFilePath(args[1])
			if err != nil {
				return err
			}
			content, err := loadDeclarativeFileContent(opts.Content, opts.FromFile)
			if err != nil {
				return err
			}
			appFile := model.AppFile{
				Path:    filePath,
				Content: content,
				Secret:  opts.Secret,
				Mode:    opts.Mode,
			}
			response, err := client.UpsertAppFiles(app.ID, []model.AppFile{appFile})
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, response.Operation, opts.Wait); err != nil {
				return err
			}
			return c.renderFilesMutationResult(filesResult{
				AppID:          app.ID,
				Files:          response.Files,
				Operation:      response.Operation,
				AlreadyCurrent: response.AlreadyCurrent,
			}, filePath)
		},
	}
	cmd.Flags().StringVar(&opts.Content, "content", "", "Inline file content")
	cmd.Flags().StringVar(&opts.FromFile, "from-file", "", "Read file content from a local path")
	cmd.Flags().BoolVar(&opts.Secret, "secret", false, "Mark the file as secret and default the mode to 0600")
	cmd.Flags().Int32Var(&opts.Mode, "mode", 0, "Optional file mode")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newFilesRemoveCommand() *cobra.Command {
	opts := filesMutationOptions{Wait: true}
	cmd := &cobra.Command{
		Use:     "rm <app> <absolute-path...>",
		Aliases: []string{"remove", "delete"},
		Short:   "Remove one or more declarative app files",
		Args:    cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			paths, err := normalizeAppFilePaths(args[1:])
			if err != nil {
				return err
			}
			response, err := client.DeleteAppFiles(app.ID, paths)
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, response.Operation, opts.Wait); err != nil {
				return err
			}
			return c.renderFilesMutationResult(filesResult{
				AppID:          app.ID,
				Files:          response.Files,
				Operation:      response.Operation,
				AlreadyCurrent: response.AlreadyCurrent,
			}, paths...)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func normalizeAppFilePath(raw string) (string, error) {
	filePath := strings.TrimSpace(raw)
	if filePath == "" {
		return "", fmt.Errorf("file path is required")
	}
	if !strings.HasPrefix(filePath, "/") {
		return "", fmt.Errorf("file path must be absolute")
	}
	if filePath == "/" || strings.HasSuffix(filePath, "/") {
		return "", fmt.Errorf("file path must point to a file")
	}
	return filePath, nil
}

func normalizeAppFilePaths(paths []string) ([]string, error) {
	out := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, raw := range paths {
		filePath, err := normalizeAppFilePath(raw)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[filePath]; ok {
			continue
		}
		seen[filePath] = struct{}{}
		out = append(out, filePath)
	}
	sort.Strings(out)
	return out, nil
}

func loadDeclarativeFileContent(inline, fromFile string) (string, error) {
	hasInline := inline != ""
	fromFile = strings.TrimSpace(fromFile)
	hasFile := fromFile != ""
	switch {
	case hasInline && hasFile:
		return "", fmt.Errorf("--content and --from-file cannot be used together")
	case !hasInline && !hasFile:
		return "", fmt.Errorf("either --content or --from-file is required")
	case hasInline:
		return inline, nil
	}
	data, err := os.ReadFile(fromFile)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", fromFile, err)
	}
	if !utf8.Valid(data) {
		return "", fmt.Errorf("file %s is not valid UTF-8; declarative app files must be text", fromFile)
	}
	return string(data), nil
}

func findAppFileByPath(files []model.AppFile, filePath string) (model.AppFile, bool) {
	for _, appFile := range files {
		if appFile.Path == filePath {
			return appFile, true
		}
	}
	return model.AppFile{}, false
}

func (c *CLI) renderFilesListResult(result filesResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	if len(result.Files) == 0 {
		if strings.TrimSpace(result.AppID) == "" {
			return nil
		}
		return writeKeyValues(c.stdout, kvPair{Key: "app_id", Value: result.AppID})
	}
	if strings.TrimSpace(result.AppID) != "" {
		if err := writeKeyValues(c.stdout, kvPair{Key: "app_id", Value: result.AppID}); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
	}
	return writeAppFileTable(c.stdout, result.Files)
}

func (c *CLI) renderFilesMutationResult(result filesResult, focusPaths ...string) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	pairs := make([]kvPair, 0, 3)
	if strings.TrimSpace(result.AppID) != "" {
		pairs = append(pairs, kvPair{Key: "app_id", Value: result.AppID})
	}
	if result.Operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: result.Operation.ID})
	}
	if result.AlreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}

	focus := filterAppFilesByPath(result.Files, focusPaths)
	if len(focus) == 0 {
		return nil
	}
	if len(pairs) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
	}
	return writeAppFileTable(c.stdout, focus)
}

func filterAppFilesByPath(files []model.AppFile, paths []string) []model.AppFile {
	if len(paths) == 0 {
		return append([]model.AppFile(nil), files...)
	}
	index := make(map[string]model.AppFile, len(files))
	for _, appFile := range files {
		index[appFile.Path] = appFile
	}
	out := make([]model.AppFile, 0, len(paths))
	for _, filePath := range paths {
		if appFile, ok := index[filePath]; ok {
			out = append(out, appFile)
		}
	}
	return out
}
