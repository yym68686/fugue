package cli

import (
	"errors"
	"strings"

	"github.com/spf13/cobra"
)

// The registry is an optional Fugue control-plane view of an otherwise
// independent edge. Runtime policy, credentials, health and DNS writes stay
// with the standalone executor; these commands only manage account metadata.
func (c *CLI) newStaticEdgeRegistryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "registry",
		Short: "Manage account metadata for independent static edges through Fugue API",
		Long:  "Registering an edge here does not make Fugue the runtime or DNS authority. Direct static-edge and traffic-pool commands continue to work when the Fugue API is unavailable.",
	}
	cmd.AddCommand(c.newStaticEdgeRegistryListCommand(), c.newStaticEdgeRegistryRegisterCommand(), c.newStaticEdgeRegistryGetCommand(), c.newStaticEdgeRegistryProofCommand(), c.newStaticEdgeRegistryRevokeCommand())
	return cmd
}

func (c *CLI) newStaticEdgeRegistryListCommand() *cobra.Command {
	var tenantID, projectID string
	cmd := &cobra.Command{Use: "ls", Aliases: []string{"list"}, Short: "List visible static-edge registrations", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		response, err := client.ListStaticEdgeRegistrations(tenantID, projectID)
		if err != nil {
			return err
		}
		if c.wantsJSON() {
			return c.writeJSON(response)
		}
		return c.renderResourceResult(response)
	}}
	cmd.Flags().StringVar(&tenantID, "tenant-id", "", "Platform-admin tenant filter")
	cmd.Flags().StringVar(&projectID, "project-id", "", "Project filter")
	return cmd
}

func (c *CLI) newStaticEdgeRegistryRegisterCommand() *cobra.Command {
	var request createStaticEdgeRegistrationRequest
	cmd := &cobra.Command{Use: "register", Short: "Register independent edge metadata in a project", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if strings.TrimSpace(request.ProjectID) == "" || strings.TrimSpace(request.Name) == "" || strings.TrimSpace(request.EdgeID) == "" {
			return errors.New("--project-id, --name, and --edge-id are required")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		registration, err := client.CreateStaticEdgeRegistration(request)
		if err != nil {
			return err
		}
		return c.renderResourceResult(registration)
	}}
	flags := cmd.Flags()
	flags.StringVar(&request.TenantID, "tenant-id", "", "Tenant ID (platform-admin only)")
	flags.StringVar(&request.ProjectID, "project-id", "", "Owning project ID")
	flags.StringVar(&request.Name, "name", "", "Unique registration name")
	flags.StringVar(&request.EdgeID, "edge-id", "", "Exact standalone edge identity")
	flags.StringVar(&request.Transport, "transport", "mtls", "Management transport: mtls or ssh")
	flags.StringVar(&request.ManagerURL, "manager-url", "", "Standalone manager HTTPS authority")
	flags.StringVar(&request.CertificateFingerprint, "certificate-fingerprint", "", "SHA-256 certificate fingerprint")
	flags.StringVar(&request.SigningKeyID, "signing-key-id", "", "Signing key identifier used by the possession observation")
	flags.StringVar(&request.PossessionProofDigest, "possession-proof-digest", "", "sha256: digest of the direct possession observation")
	return cmd
}

func (c *CLI) newStaticEdgeRegistryGetCommand() *cobra.Command {
	return &cobra.Command{Use: "show <registration-id>", Aliases: []string{"get"}, Short: "Show one static-edge registration", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		registration, err := client.GetStaticEdgeRegistration(args[0])
		if err != nil {
			return err
		}
		return c.renderResourceResult(registration)
	}}
}

func (c *CLI) newStaticEdgeRegistryProofCommand() *cobra.Command {
	var request updateStaticEdgePossessionProofRequest
	cmd := &cobra.Command{Use: "proof <registration-id>", Short: "Record a new direct possession observation", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(request.PossessionProofDigest) == "" || strings.TrimSpace(request.SigningKeyID) == "" {
			return errors.New("--possession-proof-digest and --signing-key-id are required")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		registration, err := client.UpdateStaticEdgePossessionProof(args[0], request)
		if err != nil {
			return err
		}
		return c.renderResourceResult(registration)
	}}
	cmd.Flags().StringVar(&request.PossessionProofDigest, "possession-proof-digest", "", "sha256: digest of the direct possession observation")
	cmd.Flags().StringVar(&request.SigningKeyID, "signing-key-id", "", "Signing key identifier used by the possession observation")
	cmd.Flags().BoolVar(&request.Ready, "ready", true, "Mark the registration ready after this observation")
	return cmd
}

func (c *CLI) newStaticEdgeRegistryRevokeCommand() *cobra.Command {
	return &cobra.Command{Use: "revoke <registration-id>", Short: "Revoke a static-edge registration", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		registration, err := client.RevokeStaticEdgeRegistration(args[0])
		if err != nil {
			return err
		}
		return c.renderResourceResult(registration)
	}}
}
