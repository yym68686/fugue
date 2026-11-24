package cli

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newRecoveredBackupRetirementCommand() *cobra.Command {
	var req model.RecoveredBackupRetirementRequest
	cmd := &cobra.Command{
		Use:   "retire-recovered <artifact>",
		Short: "Preview or retire one protected recovered control-plane Longhorn backup",
		Long:  "Platform administrators only. Preview is read-only. Submit the preview confirmation explicitly after retaining real isolated PostgreSQL restore evidence for the replacement. This queues logical retirement, not local snapshot deletion or proof of physical reclamation.",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if strings.TrimSpace(req.ReplacementArtifactID) == "" {
				return fmt.Errorf("--replacement is required")
			}
			if req.Confirmation != "" && (strings.TrimSpace(req.Reason) == "" || strings.TrimSpace(req.RestoreEvidenceSHA256) == "") {
				return fmt.Errorf("confirmation requires --reason and --restore-evidence-sha256")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			var result model.RecoveredBackupRetirement
			path := "/v1/backups/artifacts/" + url.PathEscape(args[0]) + "/retire-recovered"
			if err := client.doJSON(http.MethodPost, path, req, &result); err != nil {
				return err
			}
			if req.Confirmation != "" && (!result.Retired || result.AuditID == "" || result.ArtifactID != args[0] || result.ReplacementArtifactID != req.ReplacementArtifactID) {
				return fmt.Errorf("server did not confirm retirement and audit for the requested artifact; inspect its current status")
			}
			if c.wantsJSON() {
				return c.writeJSON(result)
			}
			switch {
			case result.Retired:
				fmt.Fprintf(c.stdout, "Retired artifact %s (audit %s). Remote cleanup and local snapshot reclamation are not confirmed.\n", result.ArtifactID, result.AuditID)
			case len(result.Blockers) != 0:
				fmt.Fprintf(c.stdout, "Retirement blocked for %s:\n", result.ArtifactID)
				for _, reason := range result.Blockers {
					fmt.Fprintf(c.stdout, "  - %s\n", reason)
				}
			default:
				fmt.Fprintf(c.stdout, "Preview only: retire %s, retain %s. Use --confirm %s with the same inputs to execute.\n", result.ArtifactID, result.ReplacementArtifactID, result.Confirmation)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&req.ReplacementArtifactID, "replacement", "", "Exact artifact ID of the recent retained backup")
	cmd.Flags().StringVar(&req.RestoreEvidenceSHA256, "restore-evidence-sha256", "", "SHA-256 of the operator-verified isolated restore receipt")
	cmd.Flags().StringVar(&req.Reason, "reason", "", "Retirement reason to retain in the audit log")
	cmd.Flags().StringVar(&req.Confirmation, "confirm", "", "Digest returned by a separate preview with the same arguments")
	return cmd
}
