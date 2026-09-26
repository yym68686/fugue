package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/model"
	sc "fugue/internal/staticedgecontract"
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
	var contextName string
	cmd := &cobra.Command{Use: "register", Short: "Register independent edge metadata in a project", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if strings.TrimSpace(request.ProjectID) == "" || strings.TrimSpace(contextName) == "" {
			return errors.New("--project-id and --edge-context are required")
		}
		if strings.TrimSpace(request.SigningKeyID) == "" {
			return errors.New("--signing-key-id is required")
		}
		observed, digest, err := observeStaticEdgeForRegistry(cmd.Context(), contextName)
		if err != nil {
			return err
		}
		if request.EdgeID != "" && request.EdgeID != observed.EdgeID {
			return errors.New("--edge-id does not match independent manager identity")
		}
		request.EdgeID, request.Transport, request.PossessionProofDigest = observed.EdgeID, observed.Transport, digest
		if request.Name == "" {
			request.Name = observed.Name
		}
		if request.ManagerURL == "" {
			request.ManagerURL = observed.ManagerURL
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
	flags.StringVar(&contextName, "edge-context", "", "Existing independent manager context for direct possession observation")
	flags.StringVar(&request.Name, "name", "", "Unique registration name (defaults to context name)")
	flags.StringVar(&request.EdgeID, "edge-id", "", "Exact standalone edge identity to verify")
	flags.StringVar(&request.ManagerURL, "manager-url", "", "Standalone manager HTTPS authority")
	flags.StringVar(&request.CertificateFingerprint, "certificate-fingerprint", "", "SHA-256 certificate fingerprint")
	flags.StringVar(&request.SigningKeyID, "signing-key-id", "", "Signing key identifier used by the possession observation")
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
	var contextName string
	cmd := &cobra.Command{Use: "proof <registration-id>", Short: "Record a new direct possession observation", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(contextName) == "" || strings.TrimSpace(request.SigningKeyID) == "" {
			return errors.New("--edge-context and --signing-key-id are required")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		current, err := client.GetStaticEdgeRegistration(args[0])
		if err != nil {
			return err
		}
		observed, digest, err := observeStaticEdgeForRegistry(cmd.Context(), contextName)
		if err != nil {
			return err
		}
		if current.EdgeID != observed.EdgeID || current.Transport != observed.Transport {
			return errors.New("registration does not match independent manager identity and transport")
		}
		request.PossessionProofDigest = digest
		registration, err := client.UpdateStaticEdgePossessionProof(args[0], request)
		if err != nil {
			return err
		}
		return c.renderResourceResult(registration)
	}}
	cmd.Flags().StringVar(&contextName, "edge-context", "", "Existing independent manager context for direct possession observation")
	cmd.Flags().StringVar(&request.SigningKeyID, "signing-key-id", "", "Signing key identifier used by the possession observation")
	cmd.Flags().BoolVar(&request.Ready, "ready", true, "Mark the registration ready after this observation")
	return cmd
}

type staticEdgeRegistryObservation struct {
	Name       string
	EdgeID     string
	Transport  string
	ManagerURL string
}

func observeStaticEdgeForRegistry(ctx context.Context, contextName string) (staticEdgeRegistryObservation, string, error) {
	cfg, err := loadStaticEdgeContext(contextName)
	if err != nil {
		return staticEdgeRegistryObservation{}, "", err
	}
	request := sc.Request{Schema: sc.RPCSchema, EdgeID: cfg.EdgeID, RequestID: newStaticRequestID(), Operation: "status"}
	response, err := staticEdgeCall(ctx, cfg, request)
	if err != nil {
		return staticEdgeRegistryObservation{}, "", err
	}
	if response.Result == nil || !response.Result.Ready || !response.Result.RuntimeMatches || response.Result.EdgeID != cfg.EdgeID || response.Result.RuntimeDigest == "" {
		return staticEdgeRegistryObservation{}, "", fmt.Errorf("independent manager %q is not ready or its runtime identity does not match", cfg.EdgeID)
	}
	raw, err := json.Marshal(response)
	if err != nil {
		return staticEdgeRegistryObservation{}, "", err
	}
	sum := sha256.Sum256(raw)
	managerURL := ""
	if cfg.Transport == model.StaticEdgeTransportMTLS {
		managerURL = cfg.ManagerURL
	}
	return staticEdgeRegistryObservation{Name: cfg.Name, EdgeID: cfg.EdgeID, Transport: cfg.Transport, ManagerURL: managerURL}, "sha256:" + hex.EncodeToString(sum[:]), nil
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
