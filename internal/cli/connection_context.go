package cli

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type connectionContext struct {
	Name       string `json:"name"`
	BaseURL    string `json:"base_url"`
	WebBaseURL string `json:"web_base_url,omitempty"`
	Tenant     string `json:"tenant,omitempty"`
	Project    string `json:"project,omitempty"`
}
type connectionContextFile struct {
	SchemaVersion int                 `json:"schema_version"`
	Active        string              `json:"active,omitempty"`
	Contexts      []connectionContext `json:"contexts"`
}

func connectionContextPath() string {
	if p := strings.TrimSpace(os.Getenv("FUGUE_CONTEXT_FILE")); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(authConfigPath()), "contexts.json")
}
func readConnectionContexts() (connectionContextFile, error) {
	cfg := connectionContextFile{SchemaVersion: 1, Contexts: []connectionContext{}}
	raw, err := os.ReadFile(connectionContextPath())
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("read contexts: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return cfg, fmt.Errorf("context file must contain exactly one JSON value")
	}
	if cfg.SchemaVersion != 1 {
		return cfg, fmt.Errorf("unsupported context schema %d", cfg.SchemaVersion)
	}
	names := map[string]bool{}
	for _, entry := range cfg.Contexts {
		if err := validateConnectionContext(entry); err != nil {
			return cfg, err
		}
		if names[entry.Name] {
			return cfg, fmt.Errorf("duplicate context %q", entry.Name)
		}
		names[entry.Name] = true
	}
	if cfg.Active != "" && !names[cfg.Active] {
		return cfg, fmt.Errorf("active context %q does not exist", cfg.Active)
	}
	return cfg, nil
}
func validateConnectionContext(entry connectionContext) error {
	if strings.TrimSpace(entry.Name) == "" || entry.Name != strings.TrimSpace(entry.Name) || entry.Name == "none" {
		return fmt.Errorf("context name is required and cannot be none")
	}
	for _, raw := range []string{entry.BaseURL, entry.WebBaseURL} {
		if raw == "" && raw == entry.WebBaseURL {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("context URL must be an HTTP(S) URL without credentials, query or fragment")
		}
	}
	if entry.BaseURL == "" {
		return fmt.Errorf("context base URL is required")
	}
	return nil
}
func saveConnectionContexts(cfg connectionContextFile) error {
	path := connectionContextPath()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".contexts-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	defer tmp.Close()
	if err := tmp.Chmod(0600); err != nil {
		return err
	}
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
func loadActiveConnectionContext() (*connectionContext, error) {
	selected := strings.TrimSpace(os.Getenv("FUGUE_CONTEXT"))
	if selected == "none" {
		return nil, nil
	}
	cfg, err := readConnectionContexts()
	if err != nil {
		return nil, err
	}
	if selected == "" {
		selected = cfg.Active
	}
	if selected == "" {
		return nil, nil
	}
	for _, entry := range cfg.Contexts {
		if entry.Name == selected {
			value := entry
			return &value, nil
		}
	}
	return nil, fmt.Errorf("context %q not found", selected)
}
func (c *CLI) contextValue(field string) string {
	if c.connection == nil {
		return ""
	}
	if field == "base_url" {
		return c.connection.BaseURL
	}
	// Explicitly targeting another API must not inherit a tenant/project from
	// an unrelated saved connection.
	target := firstNonEmpty(c.root.BaseURL, os.Getenv("FUGUE_BASE_URL"), os.Getenv("FUGUE_API_URL"), c.connection.BaseURL)
	if canonicalAuthBaseURL(target) != canonicalAuthBaseURL(c.connection.BaseURL) {
		return ""
	}
	switch field {
	case "web_base_url":
		return c.connection.WebBaseURL
	case "tenant":
		return c.connection.Tenant
	case "project":
		return c.connection.Project
	}
	return ""
}
func (c *CLI) newContextCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "context", Short: "Manage client connections and scope defaults; never stores serving configuration or credentials"}
	ls := &cobra.Command{Use: "ls", Aliases: []string{"list"}, Short: "List saved connection contexts", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := readConnectionContexts()
		if err != nil {
			return err
		}
		sort.Slice(cfg.Contexts, func(i, j int) bool { return cfg.Contexts[i].Name < cfg.Contexts[j].Name })
		return c.renderResourceResult(cfg)
	}}
	show := &cobra.Command{Use: "show", Short: "Explain the effective API target, scope and credential source", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if c.connectionErr != nil {
			return c.connectionErr
		}
		_, tokenSource := c.effectiveTokenWithSource()
		source := func(flag, env, context string) string {
			if flag != "" {
				return "flag"
			}
			if env != "" {
				return "environment"
			}
			if context != "" {
				return "context"
			}
			return "default"
		}
		return c.renderResourceResult(map[string]any{"base_url": c.effectiveBaseURL(), "tenant": c.effectiveTenantName(), "project": c.effectiveProjectName(), "credential_source": tokenSource, "credential_reference": canonicalAuthBaseURL(c.effectiveBaseURL()), "sources": map[string]string{"base_url": source(c.root.BaseURL, firstNonEmpty(os.Getenv("FUGUE_BASE_URL"), os.Getenv("FUGUE_API_URL")), c.contextValue("base_url")), "tenant": source(c.root.TenantName, firstNonEmpty(os.Getenv("FUGUE_TENANT"), os.Getenv("FUGUE_TENANT_NAME")), c.contextValue("tenant")), "project": source(c.root.ProjectName, firstNonEmpty(os.Getenv("FUGUE_PROJECT"), os.Getenv("FUGUE_PROJECT_NAME")), c.contextValue("project"))}})
	}}
	create := &cobra.Command{Use: "create <name>", Short: "Save a connection using --base-url, --web-base-url, --tenant and --project", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := readConnectionContexts()
		if err != nil {
			return err
		}
		for _, existing := range cfg.Contexts {
			if existing.Name == args[0] {
				return fmt.Errorf("context %q already exists; delete it before replacing it", args[0])
			}
		}
		entry := connectionContext{Name: args[0], BaseURL: c.effectiveBaseURL(), WebBaseURL: c.root.WebBaseURL, Tenant: c.root.TenantName, Project: c.root.ProjectName}
		if err := validateConnectionContext(entry); err != nil {
			return err
		}
		cfg.Contexts = append(cfg.Contexts, entry)
		if err := saveConnectionContexts(cfg); err != nil {
			return err
		}
		return c.renderResourceResult(entry)
	}}
	use := &cobra.Command{Use: "use <name>", Short: "Select a saved connection context", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := readConnectionContexts()
		if err != nil {
			return err
		}
		found := false
		for _, entry := range cfg.Contexts {
			if entry.Name == args[0] {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("context %q not found", args[0])
		}
		cfg.Active = args[0]
		if err := saveConnectionContexts(cfg); err != nil {
			return err
		}
		return c.renderResourceResult(map[string]string{"active": cfg.Active})
	}}
	clear := &cobra.Command{Use: "clear", Short: "Clear the active context without deleting saved connections", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := readConnectionContexts()
		if err != nil {
			return err
		}
		cfg.Active = ""
		if err := saveConnectionContexts(cfg); err != nil {
			return err
		}
		return c.renderResourceResult(map[string]string{"active": ""})
	}}
	remove := &cobra.Command{Use: "delete <name>", Short: "Remove a saved connection; stored authentication is unchanged", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := readConnectionContexts()
		if err != nil {
			return err
		}
		entries := []connectionContext{}
		for _, entry := range cfg.Contexts {
			if entry.Name != args[0] {
				entries = append(entries, entry)
			}
		}
		if len(entries) == len(cfg.Contexts) {
			return fmt.Errorf("context %q not found", args[0])
		}
		cfg.Contexts = entries
		if cfg.Active == args[0] {
			cfg.Active = ""
		}
		if err := saveConnectionContexts(cfg); err != nil {
			return err
		}
		return c.renderResourceResult(map[string]any{"name": args[0], "deleted": true})
	}}
	cmd.AddCommand(ls, show, create, use, clear, remove)
	return cmd
}
