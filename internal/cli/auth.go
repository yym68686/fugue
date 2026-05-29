package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func (c *CLI) newAuthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage local Fugue CLI authentication",
		Long: strings.TrimSpace(`
Manage the API key used by the Fugue CLI.

Use login to save a key for the current API base URL, status to inspect where the current key comes from, and logout to remove a saved key. Ordinary --token use stays one-shot unless you also pass --save-token.
`),
		Example: strings.TrimSpace(`
fugue auth login --token <copied-access-key>
fugue auth status
fugue auth logout
fugue --token <copied-access-key> --save-token data push
`),
	}
	cmd.AddCommand(c.newAuthLoginCommand())
	cmd.AddCommand(c.newAuthStatusCommand())
	cmd.AddCommand(c.newAuthLogoutCommand())
	return cmd
}

func (c *CLI) newAuthLoginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Save a Fugue API key for later CLI commands",
		Example: strings.TrimSpace(`
fugue auth login --token <copied-access-key>
fugue auth login --base-url https://api.example.com --token <copied-access-key>
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			token, _ := c.transientTokenWithSource()
			if strings.TrimSpace(token) == "" {
				return fmt.Errorf("--token is required, or set FUGUE_API_KEY/FUGUE_TOKEN/FUGUE_BOOTSTRAP_KEY")
			}
			cred, err := c.verifyAndSaveAuthToken(token, c.effectiveBaseURL())
			if err != nil {
				return err
			}
			source := authTokenSource(cred.Source)
			result := map[string]any{
				"base_url": cred.BaseURL,
				"source":   source,
				"location": authTokenLocation(source),
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			fmt.Fprintf(c.stdout, "Saved Fugue API key for %s\n", cred.BaseURL)
			fmt.Fprintf(c.stdout, "Location: %s\n", authTokenLocation(source))
			return nil
		},
	}
	return cmd
}

func (c *CLI) newAuthStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the current Fugue CLI authentication source",
		Example: strings.TrimSpace(`
fugue auth status
fugue auth status --json
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			token, source := c.effectiveTokenWithSource()
			meta, saved, err := savedAuthTokenMetadata(c.effectiveBaseURL())
			if err != nil {
				return err
			}
			status := map[string]any{
				"base_url":       c.effectiveBaseURL(),
				"configured":     strings.TrimSpace(token) != "",
				"active_source":  source,
				"saved":          saved,
				"saved_source":   meta.Source,
				"saved_location": meta.Location,
				"saved_updated":  meta.UpdatedAt,
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, status)
			}
			fmt.Fprintf(c.stdout, "Base URL: %s\n", c.effectiveBaseURL())
			if strings.TrimSpace(token) == "" {
				fmt.Fprintln(c.stdout, "Status: no API key configured")
				fmt.Fprintln(c.stdout, "Run: fugue auth login --token <copied-access-key>")
				return nil
			}
			fmt.Fprintln(c.stdout, "Status: API key configured")
			fmt.Fprintf(c.stdout, "Active source: %s\n", displayAuthTokenSource(source))
			fmt.Fprintf(c.stdout, "Token: %s\n", authTokenFingerprint(token))
			if saved {
				fmt.Fprintf(c.stdout, "Saved credential: %s\n", meta.Location)
				if meta.UpdatedAt != "" {
					fmt.Fprintf(c.stdout, "Saved at: %s\n", meta.UpdatedAt)
				}
			} else {
				fmt.Fprintln(c.stdout, "Saved credential: none")
			}
			return nil
		},
	}
	return cmd
}

func (c *CLI) newAuthLogoutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Remove the saved Fugue API key for this base URL",
		Example: strings.TrimSpace(`
fugue auth logout
fugue auth logout --base-url https://api.example.com
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			deleted, err := deleteSavedAuthCredential(c.effectiveBaseURL())
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"base_url": c.effectiveBaseURL(), "deleted": deleted})
			}
			if deleted {
				fmt.Fprintf(c.stdout, "Removed saved Fugue API key for %s\n", c.effectiveBaseURL())
			} else {
				fmt.Fprintf(c.stdout, "No saved Fugue API key for %s\n", c.effectiveBaseURL())
			}
			if envAuthConfigured() {
				fmt.Fprintln(c.stdout, "Environment credentials are still configured and may continue to authenticate commands.")
			}
			return nil
		},
	}
	return cmd
}

func envAuthConfigured() bool {
	for _, key := range []string{"FUGUE_TOKEN", "FUGUE_API_KEY", "FUGUE_BOOTSTRAP_KEY"} {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}
