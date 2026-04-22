package cli

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const diagnosticBundleSchemaVersion = "fugue.debug-bundle.v1"
const diagnosticBundleManifestSchemaVersion = "fugue.debug-bundle.manifest.v1"

type debugBundleOptions struct {
	diagnosticCollectOptions
	Archive string
}

type diagnosticBundleFile struct {
	Path  string `json:"path"`
	Kind  string `json:"kind"`
	Bytes int64  `json:"bytes"`
}

type diagnosticBundleManifest struct {
	SchemaVersion string                 `json:"schema_version"`
	App           string                 `json:"app"`
	AppID         string                 `json:"app_id"`
	CreatedAt     time.Time              `json:"created_at"`
	Redacted      bool                   `json:"redacted"`
	Summary       string                 `json:"summary"`
	Files         []diagnosticBundleFile `json:"files"`
}

type debugBundleResult struct {
	SchemaVersion string                   `json:"schema_version"`
	Archive       string                   `json:"archive"`
	Summary       string                   `json:"summary"`
	Redacted      bool                     `json:"redacted"`
	Manifest      diagnosticBundleManifest `json:"manifest"`
}

func (c *CLI) newDebugCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "Export shareable investigation bundles and related evidence",
	}
	cmd.AddCommand(c.newDebugBundleCommand())
	return cmd
}

func (c *CLI) newDebugBundleCommand() *cobra.Command {
	opts := debugBundleOptions{
		diagnosticCollectOptions: diagnosticCollectOptions{TailLines: 200},
	}
	cmd := &cobra.Command{
		Use:   "bundle <app>",
		Short: "Export a single-file investigation bundle with logs, timeline, and snapshots",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			evidence := sanitizeDiagnosticEvidenceResult(c.collectDiagnosticEvidence(client, app, opts.diagnosticCollectOptions), c.shouldRedact())
			archivePath := strings.TrimSpace(opts.Archive)
			if archivePath == "" {
				archivePath = defaultDiagnosticBundlePath(app.Name)
			}
			result, err := writeDiagnosticBundleArchive(archivePath, evidence)
			if err != nil {
				return withExitCode(err, ExitCodeSystemFault)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderDebugBundleResult(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.Since, "since", "", "Requested time window label for the investigation, for example 1h")
	cmd.Flags().StringVar(&opts.RequestID, "request-id", "", "Request or trace identifier used to filter log fragments")
	cmd.Flags().StringVar(&opts.ResourceID, "resource-id", "", "Resource identifier used to filter log fragments")
	cmd.Flags().StringVar(&opts.OperationID, "operation", "", "Operation identifier to correlate build/deploy evidence")
	cmd.Flags().StringVar(&opts.WorkflowFile, "workflow-file", "", "Optional workflow file to execute and include as part of the bundle")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Maximum lines to request from each log source before local filtering")
	cmd.Flags().StringVar(&opts.Archive, "archive", "", "Output zip path for the investigation bundle")
	return cmd
}

func defaultDiagnosticBundlePath(appName string) string {
	slug := model.Slugify(strings.TrimSpace(appName))
	if slug == "" {
		slug = "fugue"
	}
	return fmt.Sprintf("%s-debug-bundle-%s.zip", slug, time.Now().UTC().Format("20060102T150405Z"))
}

func writeDiagnosticBundleArchive(archivePath string, evidence diagnosticEvidenceResult) (debugBundleResult, error) {
	if strings.TrimSpace(archivePath) == "" {
		return debugBundleResult{}, fmt.Errorf("archive path is required")
	}
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o755); err != nil && filepath.Dir(archivePath) != "." {
		return debugBundleResult{}, fmt.Errorf("create archive directory: %w", err)
	}
	file, err := os.Create(archivePath)
	if err != nil {
		return debugBundleResult{}, fmt.Errorf("create archive %s: %w", archivePath, err)
	}
	defer file.Close()

	writer := zip.NewWriter(file)
	manifest := diagnosticBundleManifest{
		SchemaVersion: diagnosticBundleManifestSchemaVersion,
		App:           evidence.App,
		AppID:         evidence.AppID,
		CreatedAt:     time.Now().UTC(),
		Redacted:      evidence.Redacted,
		Summary:       evidence.Summary,
	}

	addJSON := func(path string, value any, kind string) error {
		payload, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal %s: %w", path, err)
		}
		entry, err := writer.Create(path)
		if err != nil {
			return fmt.Errorf("create archive entry %s: %w", path, err)
		}
		if _, err := entry.Write(append(payload, '\n')); err != nil {
			return fmt.Errorf("write archive entry %s: %w", path, err)
		}
		manifest.Files = append(manifest.Files, diagnosticBundleFile{Path: path, Kind: kind, Bytes: int64(len(payload) + 1)})
		return nil
	}
	addText := func(path, content, kind string) error {
		entry, err := writer.Create(path)
		if err != nil {
			return fmt.Errorf("create archive entry %s: %w", path, err)
		}
		if _, err := io.WriteString(entry, content); err != nil {
			return fmt.Errorf("write archive entry %s: %w", path, err)
		}
		manifest.Files = append(manifest.Files, diagnosticBundleFile{Path: path, Kind: kind, Bytes: int64(len(content))})
		return nil
	}

	if err := addJSON("evidence.json", evidence, "evidence"); err != nil {
		return debugBundleResult{}, err
	}
	if evidence.AppOverview != nil {
		if err := addJSON("app-overview.json", evidence.AppOverview, "snapshot"); err != nil {
			return debugBundleResult{}, err
		}
	}
	if evidence.RuntimeDiagnosis != nil {
		if err := addJSON("runtime-diagnosis.json", evidence.RuntimeDiagnosis, "diagnosis"); err != nil {
			return debugBundleResult{}, err
		}
	}
	if evidence.OperationDiagnosis != nil {
		if err := addJSON("operation-diagnosis.json", evidence.OperationDiagnosis, "diagnosis"); err != nil {
			return debugBundleResult{}, err
		}
	}
	if evidence.PodInventory != nil {
		if err := addJSON("pod-inventory.json", evidence.PodInventory, "inventory"); err != nil {
			return debugBundleResult{}, err
		}
	}
	if evidence.Workflow != nil {
		if err := addJSON("workflow.json", evidence.Workflow, "workflow"); err != nil {
			return debugBundleResult{}, err
		}
	}
	if len(evidence.Timeline) > 0 {
		if err := addJSON("timeline.json", evidence.Timeline, "timeline"); err != nil {
			return debugBundleResult{}, err
		}
	}
	if len(evidence.Warnings) > 0 {
		if err := addText("warnings.txt", strings.Join(evidence.Warnings, "\n")+"\n", "warnings"); err != nil {
			return debugBundleResult{}, err
		}
	}
	for _, source := range evidence.Logs {
		content := strings.Join(source.Lines, "\n")
		if strings.TrimSpace(content) == "" {
			content = strings.TrimSpace(source.Summary)
		}
		if content == "" && len(source.Warnings) > 0 {
			content = strings.Join(source.Warnings, "\n")
		}
		if strings.TrimSpace(content) == "" {
			continue
		}
		content = strings.TrimRight(content, "\n") + "\n"
		path := filepath.ToSlash(filepath.Join("logs", source.Name+".log"))
		if err := addText(path, content, "log"); err != nil {
			return debugBundleResult{}, err
		}
	}

	if err := addJSON("manifest.json", manifest, "manifest"); err != nil {
		return debugBundleResult{}, err
	}
	if err := writer.Close(); err != nil {
		return debugBundleResult{}, fmt.Errorf("close archive %s: %w", archivePath, err)
	}
	result := debugBundleResult{
		SchemaVersion: diagnosticBundleSchemaVersion,
		Archive:       archivePath,
		Summary:       evidence.Summary,
		Redacted:      evidence.Redacted,
		Manifest:      manifest,
	}
	return result, nil
}

func renderDebugBundleResult(w io.Writer, result debugBundleResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "schema_version", Value: result.SchemaVersion},
		kvPair{Key: "archive", Value: result.Archive},
		kvPair{Key: "summary", Value: result.Summary},
		kvPair{Key: "redacted", Value: fmt.Sprintf("%t", result.Redacted)},
		kvPair{Key: "files", Value: fmt.Sprintf("%d", len(result.Manifest.Files))},
	); err != nil {
		return err
	}
	if len(result.Manifest.Files) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\narchive_files"); err != nil {
		return err
	}
	tw := newTabWriter(w)
	if _, err := fmt.Fprintln(tw, "PATH\tKIND\tBYTES"); err != nil {
		return err
	}
	for _, file := range result.Manifest.Files {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\n", file.Path, file.Kind, file.Bytes); err != nil {
			return err
		}
	}
	return tw.Flush()
}
