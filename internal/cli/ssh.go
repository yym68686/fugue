package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appSSHEnableOptions struct {
	TargetPort         int
	User               string
	AuthorizedKeyIDs   []string
	AuthorizedKeys     []string
	AllowTCPForwarding bool
}

type appSSHConnectOptions struct {
	IdentityFile string
	PrintOnly    bool
}

func (c *CLI) newSSHKeyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ssh-key",
		Aliases: []string{"ssh-keys"},
		Short:   "Manage native SSH public keys",
	}
	cmd.AddCommand(
		c.newSSHKeyListCommand(),
		c.newSSHKeyAddCommand(),
		c.newSSHKeyRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newSSHKeyListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List SSH public keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			keys, err := client.ListSSHKeys()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"ssh_keys": keys})
			}
			return writeSSHKeyTable(c.stdout, keys, c.showIDs())
		},
	}
}

func (c *CLI) newSSHKeyAddCommand() *cobra.Command {
	var label string
	cmd := &cobra.Command{
		Use:   "add <public-key-file-or-key>",
		Short: "Add an SSH public key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			publicKey, inferredLabel, err := readPublicKeyArgument(args[0])
			if err != nil {
				return err
			}
			if strings.TrimSpace(label) == "" {
				label = inferredLabel
			}
			key, err := client.CreateSSHKey(createSSHKeyRequest{
				Label:     label,
				PublicKey: publicKey,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"ssh_key": key})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "ssh_key", Value: formatDisplayName(key.Label, key.ID, c.showIDs())},
				kvPair{Key: "fingerprint", Value: key.Fingerprint},
			)
		},
	}
	cmd.Flags().StringVar(&label, "label", "", "Human-readable label for this SSH public key")
	return cmd
}

func (c *CLI) newSSHKeyRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "rm <key>",
		Aliases: []string{"remove", "delete"},
		Short:   "Remove an SSH public key",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			key, err := c.resolveNamedSSHKey(client, args[0])
			if err != nil {
				return err
			}
			deleted, err := client.DeleteSSHKey(key.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"deleted": true, "ssh_key": deleted})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "deleted", Value: "true"},
				kvPair{Key: "ssh_key", Value: formatDisplayName(deleted.Label, deleted.ID, c.showIDs())},
				kvPair{Key: "fingerprint", Value: deleted.Fingerprint},
			)
		},
	}
}

func (c *CLI) newAppSSHCommand() *cobra.Command {
	var opts appSSHConnectOptions
	cmd := &cobra.Command{
		Use:   "ssh <app>",
		Short: "Connect to an app with native SSH",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runAppSSHConnect(args[0], opts)
		},
	}
	cmd.Flags().StringVarP(&opts.IdentityFile, "identity", "i", "", "Identity file to pass to ssh")
	cmd.Flags().BoolVar(&opts.PrintOnly, "print", false, "Print the ssh command instead of executing it")
	cmd.AddCommand(
		c.newAppSSHEnableCommand(),
		c.newAppSSHDisableCommand(),
		c.newAppSSHShowCommand(),
		c.newAppSSHDiagnoseCommand(),
		c.newAppSSHRotatePortCommand(),
		c.newAppSSHConfigCommand(),
	)
	return cmd
}

func (c *CLI) newAppSSHEnableCommand() *cobra.Command {
	opts := appSSHEnableOptions{
		TargetPort: model.DefaultAppSSHPort,
		User:       model.DefaultAppSSHUser,
	}
	cmd := &cobra.Command{
		Use:   "enable <app>",
		Short: "Enable native SSH for an app",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.resolveAppSSHClientAndApp(args[0])
			if err != nil {
				return err
			}
			enabled := true
			resp, err := client.PatchAppSSH(app.ID, patchAppSSHRequest{
				Enabled:            &enabled,
				TargetPort:         opts.TargetPort,
				User:               opts.User,
				AuthorizedKeyIDs:   opts.AuthorizedKeyIDs,
				AuthorizedKeys:     opts.AuthorizedKeys,
				AllowTCPForwarding: opts.AllowTCPForwarding,
			})
			if err != nil {
				return err
			}
			return c.writeAppSSHResponse(resp)
		},
	}
	cmd.Flags().IntVar(&opts.TargetPort, "target-port", opts.TargetPort, "Container SSH target port")
	cmd.Flags().StringVar(&opts.User, "user", opts.User, "SSH username")
	cmd.Flags().StringArrayVar(&opts.AuthorizedKeyIDs, "key-id", nil, "Existing Fugue SSH key ID to authorize; repeatable")
	cmd.Flags().StringArrayVar(&opts.AuthorizedKeys, "key", nil, "Raw SSH public key to authorize; repeatable")
	cmd.Flags().BoolVar(&opts.AllowTCPForwarding, "allow-tcp-forwarding", false, "Allow TCP forwarding in supported images")
	return cmd
}

func (c *CLI) newAppSSHDisableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "disable <app>",
		Short: "Disable native SSH for an app",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.resolveAppSSHClientAndApp(args[0])
			if err != nil {
				return err
			}
			enabled := false
			resp, err := client.PatchAppSSH(app.ID, patchAppSSHRequest{Enabled: &enabled})
			if err != nil {
				return err
			}
			return c.writeAppSSHResponse(resp)
		},
	}
}

func (c *CLI) newAppSSHShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"status", "get"},
		Short:   "Show native SSH status for an app",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.resolveAppSSHClientAndApp(args[0])
			if err != nil {
				return err
			}
			resp, err := client.GetAppSSH(app.ID)
			if err != nil {
				return err
			}
			return c.writeAppSSHResponse(resp)
		},
	}
}

func (c *CLI) newAppSSHDiagnoseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "diagnose <app>",
		Short: "Diagnose native SSH routing for an app",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.resolveAppSSHClientAndApp(args[0])
			if err != nil {
				return err
			}
			resp, err := client.DiagnoseAppSSH(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			if err := writeAppSSHStatus(c.stdout, resp.App, resp.SSH, c.showIDs()); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeAppSSHDiagnosisChecks(c.stdout, resp.Checks)
		},
	}
}

func (c *CLI) newAppSSHRotatePortCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "rotate-port <app>",
		Short: "Rotate the app's public SSH port",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.resolveAppSSHClientAndApp(args[0])
			if err != nil {
				return err
			}
			resp, err := client.RotateAppSSHPort(app.ID)
			if err != nil {
				return err
			}
			return c.writeAppSSHResponse(resp)
		},
	}
}

func (c *CLI) newAppSSHConfigCommand() *cobra.Command {
	var identityFile string
	cmd := &cobra.Command{
		Use:   "config <app>",
		Short: "Print an OpenSSH config block for an app",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.resolveAppSSHClientAndApp(args[0])
			if err != nil {
				return err
			}
			resp, err := client.GetAppSSH(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":         resp.App,
					"ssh":         resp.SSH,
					"config":      appSSHOpenSSHConfig(resp.App, resp.SSH, identityFile),
					"ssh_command": appSSHCommand(resp.SSH, identityFile),
				})
			}
			if err := ensureAppSSHConnectable(resp.SSH); err != nil {
				return err
			}
			_, err = fmt.Fprint(c.stdout, appSSHOpenSSHConfig(resp.App, resp.SSH, identityFile))
			return err
		},
	}
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "IdentityFile value to include")
	return cmd
}

func (c *CLI) runAppSSHConnect(appRef string, opts appSSHConnectOptions) error {
	client, app, err := c.resolveAppSSHClientAndApp(appRef)
	if err != nil {
		return err
	}
	resp, err := client.GetAppSSH(app.ID)
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app":         resp.App,
			"ssh":         resp.SSH,
			"ssh_command": appSSHCommand(resp.SSH, opts.IdentityFile),
		})
	}
	if err := ensureAppSSHConnectable(resp.SSH); err != nil {
		return err
	}
	command := appSSHCommandArgs(resp.SSH, opts.IdentityFile)
	if opts.PrintOnly {
		_, err := fmt.Fprintln(c.stdout, strings.Join(command, " "))
		return err
	}
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		return fmt.Errorf("local ssh binary not found; install OpenSSH client or run `fugue app ssh config %s`", appRef)
	}
	cmd := exec.Command(sshPath, command[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = c.stdout
	cmd.Stderr = c.stderr
	return cmd.Run()
}

func (c *CLI) resolveAppSSHClientAndApp(appRef string) (*Client, model.App, error) {
	client, err := c.newClient()
	if err != nil {
		return nil, model.App{}, err
	}
	app, err := c.resolveNamedApp(client, appRef)
	if err != nil {
		return nil, model.App{}, err
	}
	return client, app, nil
}

func (c *CLI) writeAppSSHResponse(resp appSSHResponse) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, resp)
	}
	return writeAppSSHStatus(c.stdout, resp.App, resp.SSH, c.showIDs())
}

func ensureAppSSHConnectable(status model.AppSSHStatus) error {
	if !status.Supported {
		return fmt.Errorf("app SSH is unsupported: %s", strings.TrimSpace(status.Message))
	}
	if !status.Ready || strings.TrimSpace(status.Hostname) == "" || status.PublicPort <= 0 {
		message := strings.TrimSpace(status.Message)
		if message == "" {
			message = "endpoint is not ready"
		}
		return fmt.Errorf("app SSH is not ready: %s", message)
	}
	return nil
}

func appSSHCommand(status model.AppSSHStatus, identityFile string) string {
	return strings.Join(appSSHCommandArgs(status, identityFile), " ")
}

func appSSHCommandArgs(status model.AppSSHStatus, identityFile string) []string {
	user := firstNonEmptyTrimmed(status.User, model.DefaultAppSSHUser)
	args := []string{"ssh", "-p", fmt.Sprintf("%d", status.PublicPort)}
	if identityFile = strings.TrimSpace(identityFile); identityFile != "" {
		args = append(args, "-i", identityFile)
	}
	args = append(args, fmt.Sprintf("%s@%s", user, strings.TrimSpace(status.Hostname)))
	return args
}

func appSSHOpenSSHConfig(app model.App, status model.AppSSHStatus, identityFile string) string {
	alias := "fugue-" + model.Slugify(firstNonEmptyTrimmed(app.Name, app.ID))
	if alias == "fugue-" {
		alias = "fugue-app"
	}
	lines := []string{
		"Host " + alias,
		"  HostName " + strings.TrimSpace(status.Hostname),
		"  Port " + fmt.Sprintf("%d", status.PublicPort),
		"  User " + firstNonEmptyTrimmed(status.User, model.DefaultAppSSHUser),
		"  IdentitiesOnly yes",
	}
	if identityFile = strings.TrimSpace(identityFile); identityFile != "" {
		lines = append(lines, "  IdentityFile "+identityFile)
	}
	return strings.Join(lines, "\n") + "\n"
}

func readPublicKeyArgument(value string) (string, string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", fmt.Errorf("public key is required")
	}
	if stat, err := os.Stat(value); err == nil && !stat.IsDir() {
		payload, err := os.ReadFile(value)
		if err != nil {
			return "", "", err
		}
		return strings.TrimSpace(string(payload)), strings.TrimSuffix(filepath.Base(value), filepath.Ext(value)), nil
	}
	fields := strings.Fields(value)
	label := ""
	if len(fields) > 2 {
		label = strings.Join(fields[2:], " ")
	}
	return value, label, nil
}

func writeSSHKeyTable(w interface{ Write([]byte) (int, error) }, keys []model.SSHKey, showIDs bool) error {
	sorted := append([]model.SSHKey(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].CreatedAt.Equal(sorted[j].CreatedAt) {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KEY\tFINGERPRINT\tSTATUS\tCREATED"); err != nil {
		return err
	}
	for _, key := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			formatDisplayName(key.Label, key.ID, showIDs),
			key.Fingerprint,
			key.Status,
			formatTime(key.CreatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAppSSHStatus(w interface{ Write([]byte) (int, error) }, app model.App, status model.AppSSHStatus, showIDs bool) error {
	pairs := []kvPair{
		{Key: "app", Value: formatDisplayName(app.Name, app.ID, showIDs)},
		{Key: "supported", Value: fmt.Sprintf("%t", status.Supported)},
		{Key: "ready", Value: fmt.Sprintf("%t", status.Ready)},
	}
	if strings.TrimSpace(status.Hostname) != "" {
		pairs = append(pairs, kvPair{Key: "host", Value: status.Hostname})
	}
	if status.PublicPort > 0 {
		pairs = append(pairs, kvPair{Key: "port", Value: fmt.Sprintf("%d", status.PublicPort)})
	}
	if strings.TrimSpace(status.User) != "" {
		pairs = append(pairs, kvPair{Key: "user", Value: status.User})
	}
	if status.TargetPort > 0 {
		pairs = append(pairs, kvPair{Key: "target_port", Value: fmt.Sprintf("%d", status.TargetPort)})
	}
	if strings.TrimSpace(status.HostKeyFingerprint) != "" {
		pairs = append(pairs, kvPair{Key: "host_key", Value: status.HostKeyFingerprint})
	}
	if strings.TrimSpace(status.Message) != "" {
		pairs = append(pairs, kvPair{Key: "message", Value: status.Message})
	}
	if status.Ready && strings.TrimSpace(status.Hostname) != "" && status.PublicPort > 0 {
		pairs = append(pairs, kvPair{Key: "ssh", Value: appSSHCommand(status, "")})
	}
	return writeKeyValues(w, pairs...)
}

func writeAppSSHDiagnosisChecks(w interface{ Write([]byte) (int, error) }, checks []appSSHDiagnosisCheck) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CHECK\tPASS\tMESSAGE"); err != nil {
		return err
	}
	for _, check := range checks {
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%s\n", check.Name, check.Pass, check.Message); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func (c *CLI) resolveNamedSSHKey(client *Client, ref string) (model.SSHKey, error) {
	keys, err := client.ListSSHKeys()
	if err != nil {
		return model.SSHKey{}, err
	}
	return resolveSingleMatch(ref, matchVisibleSSHKeys(keys, ref), "ssh key", describeSSHKeyMatch)
}

func matchVisibleSSHKeys(keys []model.SSHKey, ref string) []model.SSHKey {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	matches := make([]model.SSHKey, 0, 1)
	for _, key := range keys {
		switch {
		case strings.EqualFold(key.ID, ref):
			matches = append(matches, key)
		case strings.EqualFold(key.Label, ref):
			matches = append(matches, key)
		case strings.EqualFold(key.Fingerprint, ref):
			matches = append(matches, key)
		}
	}
	return matches
}

func describeSSHKeyMatch(key model.SSHKey) string {
	return fmt.Sprintf("%s (%s, %s)", firstNonEmptyTrimmed(key.Label, key.ID), key.ID, key.Fingerprint)
}
