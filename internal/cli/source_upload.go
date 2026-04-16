package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newSourceUploadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source-upload",
		Short: "Inspect uploaded source archives and their import references",
	}
	cmd.AddCommand(c.newSourceUploadShowCommand())
	return cmd
}

func (c *CLI) newSourceUploadShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <upload-id>",
		Aliases: []string{"get", "status"},
		Short:   "Show uploaded source metadata and referencing operations",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			inspection, err := client.GetSourceUpload(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"source_upload": inspection})
			}
			return renderSourceUploadInspection(c.stdout, inspection)
		},
	}
}

func renderSourceUploadInspection(w io.Writer, inspection model.SourceUploadInspection) error {
	if err := writeKeyValues(w,
		kvPair{Key: "upload_id", Value: strings.TrimSpace(inspection.Upload.ID)},
		kvPair{Key: "tenant_id", Value: strings.TrimSpace(inspection.Upload.TenantID)},
		kvPair{Key: "filename", Value: strings.TrimSpace(inspection.Upload.Filename)},
		kvPair{Key: "content_type", Value: strings.TrimSpace(inspection.Upload.ContentType)},
		kvPair{Key: "archive_sha256", Value: strings.TrimSpace(inspection.Upload.SHA256)},
		kvPair{Key: "archive_size_bytes", Value: fmt.Sprintf("%d", inspection.Upload.SizeBytes)},
		kvPair{Key: "created_at", Value: formatTime(inspection.Upload.CreatedAt)},
		kvPair{Key: "updated_at", Value: formatTime(inspection.Upload.UpdatedAt)},
	); err != nil {
		return err
	}
	if len(inspection.References) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeSourceUploadReferenceTable(w, inspection.References)
}

func writeSourceUploadReferenceTable(w io.Writer, references []model.SourceUploadReference) error {
	sorted := append([]model.SourceUploadReference(nil), references...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].CreatedAt.Equal(sorted[j].CreatedAt) {
			return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
		}
		return strings.TrimSpace(sorted[i].OperationID) < strings.TrimSpace(sorted[j].OperationID)
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "OPERATION\tTYPE\tSTATUS\tAPP\tBUILD\tSOURCE_DIR\tIMAGE\tUPDATED"); err != nil {
		return err
	}
	for _, reference := range sorted {
		appName := firstNonEmptyTrimmed(strings.TrimSpace(reference.AppName), strings.TrimSpace(reference.AppID))
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			strings.TrimSpace(reference.OperationID),
			strings.TrimSpace(reference.OperationType),
			strings.TrimSpace(reference.OperationStatus),
			appName,
			strings.TrimSpace(reference.BuildStrategy),
			strings.TrimSpace(reference.SourceDir),
			strings.TrimSpace(reference.ResolvedImageRef),
			formatTime(reference.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
