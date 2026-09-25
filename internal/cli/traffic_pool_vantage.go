package cli

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"fugue/internal/entryfailover"
	c "fugue/internal/staticedgecontract"
	"github.com/spf13/cobra"
)

type trustedVantageHost struct {
	Hostname, Port, HostKeyAlias, KnownHosts string
}

func trustedVantageSSH(alias string) (trustedVantageHost, error) {
	var host trustedVantageHost
	if !c.ValidID(alias) || strings.Contains(alias, "..") {
		return host, errors.New("safe OpenSSH alias required")
	}
	out, err := exec.Command("ssh", "-G", alias).Output()
	if err != nil {
		return host, err
	}
	var keyFiles []string
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "hostname":
			host.Hostname = fields[1]
		case "port":
			host.Port = fields[1]
		case "userknownhostsfile":
			keyFiles = append(keyFiles, fields[1:]...)
		case "proxycommand", "proxyjump":
			if fields[1] != "none" {
				return host, errors.New("probe enrollment requires direct pinned SSH transport")
			}
		}
	}
	if net.ParseIP(host.Hostname) == nil && !staticEdgeDNSName.MatchString(host.Hostname) {
		return host, errors.New("invalid vantage SSH hostname")
	}
	port, err := strconv.Atoi(host.Port)
	if err != nil || port < 1 || port > 65535 {
		return host, errors.New("invalid vantage SSH port")
	}
	host.HostKeyAlias = host.Hostname
	if port != 22 {
		host.HostKeyAlias = net.JoinHostPort(host.Hostname, host.Port)
	}
	var pinned []string
	for _, file := range keyFiles {
		if strings.HasPrefix(file, "~/") {
			home, e := os.UserHomeDir()
			if e != nil {
				return host, e
			}
			file = filepath.Join(home, file[2:])
		}
		found, e := exec.Command("ssh-keygen", "-F", host.HostKeyAlias, "-f", file).Output()
		if e != nil {
			continue
		}
		for _, line := range strings.Split(string(found), "\n") {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 3 || !strings.HasPrefix(fields[1], "ssh-") && !strings.HasPrefix(fields[1], "ecdsa-") {
				return host, errors.New("unrecognized pinned host key")
			}
			pinned = append(pinned, strings.Join(fields[:3], " "))
		}
	}
	if len(pinned) == 0 {
		return host, errors.New("vantage host has no pinned known_hosts key")
	}
	host.KnownHosts = strings.Join(pinned, "\n") + "\n"
	return host, nil
}

func (cli *CLI) newTrafficPoolVantageCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "vantage", Short: "Manage independent read-only probe vantages"}
	var contextName, executorSSH, vantageID, vantageSSH string
	enroll := &cobra.Command{Use: "enroll", Short: "Enroll a pinned, forced-command SSH probe vantage", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cli.enrollTrafficPoolVantage(cmd.Context(), contextName, executorSSH, vantageID, vantageSSH)
		}}
	enroll.Flags().StringVar(&contextName, "executor-context", "", "Local independent executor mTLS context")
	enroll.Flags().StringVar(&executorSSH, "executor-ssh", "", "OpenSSH alias of independent executor")
	enroll.Flags().StringVar(&vantageID, "vantage-id", "", "Signed SSH vantage ID")
	enroll.Flags().StringVar(&vantageSSH, "vantage-ssh", "", "Locally trusted OpenSSH alias of probe node")
	for _, name := range []string{"executor-ssh", "vantage-id", "vantage-ssh"} {
		_ = enroll.MarkFlagRequired(name)
	}
	cmd.AddCommand(enroll)
	return cmd
}

func (cli *CLI) enrollTrafficPoolVantage(ctx context.Context, contextName, executorSSH, vantageID, vantageSSH string) error {
	if !c.ValidID(executorSSH) || strings.Contains(executorSSH, "..") || !entryInstanceName.MatchString(vantageID) {
		return errors.New("safe executor alias and vantage ID required")
	}
	cfg, err := trafficPoolContextNamed(contextName)
	if err != nil {
		return err
	}
	signedPath := filepath.Join(filepath.Dir(trafficPoolContextPath()), "entry-failover-identities", cfg.Name, "signed-policy.json")
	raw, err := os.ReadFile(signedPath)
	if err != nil {
		return err
	}
	publicKey, err := os.ReadFile(filepath.Join(filepath.Dir(signedPath), "signing.pub"))
	if err != nil {
		return err
	}
	key, err := c.ParsePublicKeyPEM(publicKey)
	if err != nil {
		return err
	}
	signed, err := entryfailover.ParseSignedPolicy(raw, map[string]ed25519.PublicKey{"bootstrap": key})
	if err != nil {
		return err
	}
	if signed.Policy.PoolID != cfg.Name {
		return errors.New("context and signed pool identity differ")
	}
	var vantage entryfailover.Vantage
	found := false
	for _, item := range signed.Policy.Vantages {
		if item.ID == vantageID && item.Transport == "ssh" {
			vantage, found = item, true
			break
		}
	}
	if !found {
		return errors.New("SSH vantage is outside signed policy")
	}
	if vantage.SSHHost != vantageSSH {
		return errors.New("signed SSH alias and local vantage alias differ")
	}
	trusted, err := trustedVantageSSH(vantageSSH)
	if err != nil {
		return err
	}
	identifier := strings.TrimPrefix(c.Hash([]byte(signed.Policy.PoolID+"/"+vantage.ID)), "sha256:")[:12]
	user := "fep" + identifier
	home := "/var/lib/fugue-probe-" + identifier
	executorHome := "/var/lib/fugue-entry-failover"
	keyPath := executorHome + "/.ssh/" + user
	initKey := "set -eu; install -d -m 0700 -o fuguefailover -g fuguefailover " + executorHome + "/.ssh; if ! test -e " + keyPath + "; then runuser -u fuguefailover -- ssh-keygen -q -t ed25519 -N '' -f " + keyPath + "; fi; cat " + keyPath + ".pub"
	generated, err := staticEdgeSSHOutput(ctx, executorSSH, nil, initKey)
	if err != nil {
		return err
	}
	fields := strings.Fields(string(generated))
	if len(fields) < 2 || fields[0] != "ssh-ed25519" {
		return errors.New("executor produced an invalid probe public key")
	}
	if _, err = base64.StdEncoding.DecodeString(fields[1]); err != nil {
		return err
	}
	archRaw, err := staticEdgeSSHOutput(ctx, vantageSSH, nil, "uname -m")
	if err != nil {
		return err
	}
	arch := ""
	switch strings.TrimSpace(string(archRaw)) {
	case "x86_64":
		arch = "amd64"
	case "aarch64":
		arch = "arm64"
	}
	if arch == "" {
		return errors.New("probe node requires a supported Linux architecture")
	}
	binary, err := loadEntryFailoverReleaseBinary(ctx, arch)
	if err != nil {
		return err
	}
	prepare := "set -eu; if ! getent passwd " + user + " >/dev/null; then useradd --system --home-dir " + home + " --shell /bin/sh " + user + "; fi; install -d -m 0700 -o " + user + " -g " + user + " " + home + "/.ssh"
	if _, err = staticEdgeSSHOutput(ctx, vantageSSH, nil, prepare); err != nil {
		return err
	}
	for _, item := range []struct {
		path        string
		data        []byte
		mode, owner string
	}{
		{vantage.BinaryPath, binary, "0755", "root:root"},
		{vantage.PublicKeyPath, publicKey, "0644", "root:root"},
	} {
		check := "if test -e " + item.path + "; then test \"$(sha256sum " + item.path + " | cut -d ' ' -f 1)\" = " + strings.TrimPrefix(c.Hash(item.data), "sha256:") + "; fi"
		if _, err = staticEdgeSSHOutput(ctx, vantageSSH, nil, check); err != nil {
			return fmt.Errorf("probe artifact drift at %s", item.path)
		}
		if err = staticEdgeSSHWrite(ctx, vantageSSH, item.path, item.data, item.mode, item.owner); err != nil {
			return err
		}
	}
	forced := "restrict,command=\"" + vantage.BinaryPath + " probe-rpc --key " + vantage.PublicKeyPath + "\" " + fields[0] + " " + fields[1] + "\n"
	authPath := home + "/.ssh/authorized_keys"
	if existing, e := staticEdgeSSHOutput(ctx, vantageSSH, nil, "if test -e "+authPath+"; then cat "+authPath+"; fi"); e != nil {
		return e
	} else if len(existing) > 0 && string(existing) != forced {
		return errors.New("probe authorized_keys differs; refusing overwrite")
	}
	if err = staticEdgeSSHWrite(ctx, vantageSSH, authPath, []byte(forced), "0600", user+":"+user); err != nil {
		return err
	}
	if _, err = staticEdgeSSHOutput(ctx, vantageSSH, nil, "chmod 0700 "+home+"/.ssh; chown "+user+":"+user+" "+home+"/.ssh"); err != nil {
		return err
	}
	sshConfig := "Host " + vantage.SSHHost + "\n  HostName " + trusted.Hostname + "\n  Port " + trusted.Port + "\n  User " + user + "\n  IdentityFile " + keyPath + "\n  UserKnownHostsFile " + executorHome + "/.ssh/known_hosts_" + vantage.ID + "\n  HostKeyAlias " + trusted.HostKeyAlias + "\n  StrictHostKeyChecking yes\n  IdentitiesOnly yes\n  BatchMode yes\n"
	if _, err = staticEdgeSSHOutput(ctx, executorSSH, nil, "install -d -m 0700 -o fuguefailover -g fuguefailover "+executorHome+"/.ssh/conf.d"); err != nil {
		return err
	}
	common := []byte("Include " + executorHome + "/.ssh/conf.d/*\n")
	for _, item := range []struct {
		path string
		data []byte
		mode string
	}{
		{executorHome + "/.ssh/config", common, "0600"},
		{executorHome + "/.ssh/conf.d/" + vantage.ID + ".conf", []byte(sshConfig), "0600"},
		{executorHome + "/.ssh/known_hosts_" + vantage.ID, []byte(trusted.KnownHosts), "0644"},
	} {
		check := "if test -e " + item.path + "; then test \"$(sha256sum " + item.path + " | cut -d ' ' -f 1)\" = " + strings.TrimPrefix(c.Hash(item.data), "sha256:") + "; fi"
		if _, err = staticEdgeSSHOutput(ctx, executorSSH, nil, check); err != nil {
			return fmt.Errorf("executor SSH trust configuration differs at %s", item.path)
		}
		if err = staticEdgeSSHWrite(ctx, executorSSH, item.path, item.data, item.mode, "fuguefailover:fuguefailover"); err != nil {
			return err
		}
	}
	if _, err = staticEdgeSSHOutput(ctx, executorSSH, nil, "chmod 0700 "+executorHome+"/.ssh "+executorHome+"/.ssh/conf.d; chown fuguefailover:fuguefailover "+executorHome+"/.ssh "+executorHome+"/.ssh/conf.d"); err != nil {
		return err
	}
	return cli.writeJSON(map[string]string{"executor_context": cfg.Name, "vantage_id": vantageID, "probe_user": user, "policy_digest": signed.Digest})
}
