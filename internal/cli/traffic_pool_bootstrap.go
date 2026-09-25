package cli

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"fugue/internal/entryfailover"
	c "fugue/internal/staticedgecontract"
	"github.com/spf13/cobra"
)

type trafficPoolBootstrapOptions struct {
	Name, SSHHost, PublicIP, PolicyFile string
	Port                                int
}

var entryInstanceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)

func (cli *CLI) newTrafficPoolBootstrapCommand() *cobra.Command {
	var o trafficPoolBootstrapOptions
	cmd := &cobra.Command{Use: "bootstrap", Short: "Initialize a dedicated independent executor in shadow mode", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return cli.bootstrapTrafficPool(cmd.Context(), o) }}
	cmd.Flags().StringVar(&o.Name, "name", "", "Unique executor context and systemd instance name")
	cmd.Flags().StringVar(&o.SSHHost, "ssh", "", "Local OpenSSH alias for the independent executor host")
	cmd.Flags().StringVar(&o.PublicIP, "public-ip", "", "Management IPv4 or IPv6 address")
	cmd.Flags().IntVar(&o.Port, "port", 9444, "Dedicated mTLS management port")
	cmd.Flags().StringVar(&o.PolicyFile, "file", "", "Unsigned shadow policy JSON, including exact DNS baseline")
	for _, name := range []string{"name", "ssh", "public-ip", "file"} {
		_ = cmd.MarkFlagRequired(name)
	}
	return cmd
}

func trafficPoolBootstrapPolicy(o trafficPoolBootstrapOptions) (entryfailover.Policy, error) {
	var p entryfailover.Policy
	if !entryInstanceName.MatchString(o.Name) || !c.ValidID(o.SSHHost) || strings.Contains(o.SSHHost, "..") || net.ParseIP(o.PublicIP) == nil || o.Port < 1024 || o.Port > 65535 {
		return p, errors.New("safe name, SSH alias, management IP and unprivileged port required")
	}
	raw, err := os.ReadFile(o.PolicyFile)
	if err != nil {
		return p, err
	}
	if len(raw) > 60<<10 {
		return p, errors.New("policy exceeds size bound")
	}
	if err = c.StrictJSON(raw, &p); err != nil {
		return p, err
	}
	if err = p.Validate(); err != nil {
		return p, err
	}
	if p.PoolID != o.Name {
		return p, errors.New("bootstrap context name must equal signed pool ID")
	}
	if p.Mode != "shadow" || !p.ExpiresAt.After(time.Now()) {
		return p, errors.New("bootstrap requires a currently valid shadow policy")
	}
	for _, target := range p.Targets {
		if target.Kind == "static-ip" && target.Address == o.PublicIP {
			return p, errors.New("executor cannot share the candidate static entry address")
		}
	}
	return p, nil
}

func (cli *CLI) bootstrapTrafficPool(ctx context.Context, o trafficPoolBootstrapOptions) error {
	p, err := trafficPoolBootstrapPolicy(o)
	if err != nil {
		return err
	}
	unlock, err := acquireStaticEdgeCutoverLock("entry-bootstrap-" + o.Name)
	if err != nil {
		return err
	}
	defer unlock()
	base := "/etc/fugue-entry-failover/" + o.Name
	state := "/var/lib/fugue-entry-failover/" + o.Name
	unit := "fugue-entry-failover-" + o.Name + ".service"
	identityDir := filepath.Join(filepath.Dir(trafficPoolContextPath()), "entry-failover-identities", o.Name)
	identityPath := filepath.Join(identityDir, "identity.json")
	var identity staticEdgeBootstrapIdentity
	if saved, readErr := os.ReadFile(identityPath); readErr == nil {
		if err = c.StrictJSON(saved, &identity); err != nil {
			return err
		}
		if identity.ServerName != o.PublicIP {
			return errors.New("bootstrap management address differs from saved identity")
		}
	} else if !os.IsNotExist(readErr) {
		return readErr
	} else {
		identity, err = createStaticEdgeManagementIdentity(o.Name, o.PublicIP)
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(identity)
		if err = staticEdgeWriteFile(identityPath, raw); err != nil {
			return err
		}
	}
	if len(identity.SigningPrivateKey) != ed25519.PrivateKeySize {
		return errors.New("invalid bootstrap signing key")
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(identity.SigningPrivateKey)
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{"ca.pem": identity.CA, "ca.key": identity.CAKey, "client.pem": identity.ClientCert, "client.key": identity.ClientKey,
		"signing.pub": identity.SigningPublic, "signing.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})} {
		path := filepath.Join(identityDir, name)
		if old, e := os.ReadFile(path); e == nil {
			if c.Hash(old) != c.Hash(data) {
				return errors.New("local executor identity drift")
			}
		} else if !os.IsNotExist(e) {
			return e
		} else if err = staticEdgeWriteFile(path, data); err != nil {
			return err
		}
	}
	signed, err := entryfailover.Sign(p, "bootstrap", identity.SigningPrivateKey)
	if err != nil {
		return err
	}
	signedRaw, _ := json.Marshal(signed)
	signedPath := filepath.Join(identityDir, "signed-policy.json")
	if prior, readErr := os.ReadFile(signedPath); readErr == nil {
		if c.Hash(prior) != c.Hash(signedRaw) {
			return errors.New("bootstrap policy differs from the saved signed policy; use policy apply")
		}
	} else if !os.IsNotExist(readErr) {
		return readErr
	} else if err = staticEdgeWriteFile(signedPath, signedRaw); err != nil {
		return err
	}
	cfg := trafficPoolContext{Name: o.Name, Endpoint: "https://" + net.JoinHostPort(o.PublicIP, fmt.Sprint(o.Port)), ServerName: o.PublicIP,
		CA: filepath.Join(identityDir, "ca.pem"), ClientCert: filepath.Join(identityDir, "client.pem"), ClientKey: filepath.Join(identityDir, "client.key")}
	contexts, err := readTrafficPoolContexts()
	if err != nil {
		return err
	}
	found := false
	for _, existing := range contexts.Contexts {
		if existing.Name == o.Name {
			if existing != cfg {
				return errors.New("executor context already exists with different identity")
			}
			found = true
		}
	}
	if !found {
		contexts.Contexts = append(contexts.Contexts, cfg)
		if contexts.Active == "" {
			contexts.Active = o.Name
		}
		if err = contexts.save(); err != nil {
			return err
		}
	}
	// Check for an existing process before touching its immutable configuration.
	remoteState, err := staticEdgeSSHOutput(ctx, o.SSHHost, nil, "if systemctl is-active --quiet "+unit+"; then echo active; else echo inactive; fi; uname -m")
	if err != nil {
		return err
	}
	if strings.HasPrefix(string(remoteState), "active\n") {
		var status entryfailover.Status
		if err = trafficPoolCall(ctx, cfg, "GET", "/v1/entry-failover/status", nil, &status); err != nil {
			return err
		}
		if status.PolicyDigest != signed.Digest {
			return errors.New("existing executor has another signed policy; use policy apply")
		}
		return cli.writeJSON(status)
	}
	arch := ""
	if strings.HasSuffix(strings.TrimSpace(string(remoteState)), "x86_64") {
		arch = "amd64"
	}
	if strings.HasSuffix(strings.TrimSpace(string(remoteState)), "aarch64") {
		arch = "arm64"
	}
	if arch == "" {
		return errors.New("executor requires a supported Linux architecture")
	}
	prepare := "set -eu; if ! getent passwd fuguefailover >/dev/null; then useradd --system --home-dir /var/lib/fugue-entry-failover --shell /usr/sbin/nologin fuguefailover; fi; install -d -m 0700 -o fuguefailover -g fuguefailover " + state + "; install -d -m 0750 -o root -g fuguefailover " + base
	if _, err = staticEdgeSSHOutput(ctx, o.SSHHost, nil, prepare); err != nil {
		return err
	}
	binary, err := loadEntryFailoverReleaseBinary(ctx, arch)
	if err != nil {
		return err
	}
	installDir := "/opt/fugue-entry-failover/" + strings.TrimPrefix(c.Hash(binary), "sha256:")
	if err = staticEdgeSSHWrite(ctx, o.SSHHost, installDir+"/fugue-entry-failover", binary, "0755", "root:root"); err != nil {
		return err
	}
	// Bootstrap is additive. Existing policy or identity files must match exactly.
	files := map[string][]byte{"policy.json": signedRaw, "signing.pub": identity.SigningPublic, "server.pem": identity.ServerCert, "server.key": identity.ServerKey, "ca.pem": identity.CA}
	settings := map[string]any{"state_dir": state, "policy_path": base + "/policy.json", "vault_key_path": base + "/vault.key", "listen": net.JoinHostPort(o.PublicIP, fmt.Sprint(o.Port)),
		"verification_keys": map[string]string{"bootstrap": base + "/signing.pub"}, "initial_credential_slot": "current",
		"credential_slots": map[string]any{"current": map[string]any{"server_cert": base + "/server.pem", "server_key": base + "/server.key", "client_ca": base + "/ca.pem", "grants": map[string]string{identity.ClientFingerprint: "admin"}}}}
	files["config.json"], err = json.Marshal(settings)
	if err != nil {
		return err
	}
	for name, data := range files {
		path := base + "/" + name
		check := "if test -e " + path + "; then test \"$(sha256sum " + path + " | cut -d ' ' -f 1)\" = " + strings.TrimPrefix(c.Hash(data), "sha256:") + "; fi"
		if _, err = staticEdgeSSHOutput(ctx, o.SSHHost, nil, check); err != nil {
			return fmt.Errorf("existing executor configuration differs at %s", path)
		}
		if err = staticEdgeSSHWrite(ctx, o.SSHHost, path, data, "0640", "root:fuguefailover"); err != nil {
			return err
		}
	}
	unitBytes := []byte(fmt.Sprintf(trafficPoolSystemdUnit, installDir, base, state, state))
	if err = staticEdgeSSHWrite(ctx, o.SSHHost, "/etc/systemd/system/"+unit, unitBytes, "0644", "root:root"); err != nil {
		return err
	}
	start := "set -eu; chown root:fuguefailover " + base + "; chmod 0750 " + base + "; install -d -m 0700 -o fuguefailover -g fuguefailover " + state + "; if ! test -e " + base + "/vault.key; then umask 077; openssl rand -hex 32 > " + base + "/vault.key; chown fuguefailover:fuguefailover " + base + "/vault.key; chmod 0600 " + base + "/vault.key; fi; systemctl daemon-reload; systemctl enable --now " + unit + "; systemctl is-active --quiet " + unit
	if _, err = staticEdgeSSHOutput(ctx, o.SSHHost, nil, start); err != nil {
		return err
	}
	var status entryfailover.Status
	deadline := time.Now().Add(20 * time.Second)
	for {
		err = trafficPoolCall(ctx, cfg, "GET", "/v1/entry-failover/status", nil, &status)
		if err == nil {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if status.PolicyDigest != signed.Digest || status.Mode != "shadow" || status.CredentialPresent {
		return errors.New("executor bootstrap state differs; inspect status before proceeding")
	}
	return cli.writeJSON(status)
}

const trafficPoolSystemdUnit = `[Unit]
Description=Independent Fugue entry failover executor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=fuguefailover
Group=fuguefailover
ExecStart=%s/fugue-entry-failover run --config %s/config.json
Restart=on-failure
RestartSec=5s
TimeoutStopSec=120s
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=%s
WorkingDirectory=%s
MemoryMax=192M
CPUQuota=25%%
LimitNOFILE=1024

[Install]
WantedBy=multi-user.target
`

func loadEntryFailoverReleaseBinary(ctx context.Context, arch string) ([]byte, error) {
	version := currentCLIBuildInfo().Version
	if !strings.HasPrefix(version, "v") {
		return nil, errors.New("executor bootstrap requires a tagged CLI release")
	}
	release, err := fetchCLIRelease(ctx, version)
	if err != nil {
		return nil, err
	}
	asset := "fugue_entry_failover_linux_" + arch + ".tar.gz"
	archiveURL, ok := release.Assets[asset]
	if !ok {
		return nil, fmt.Errorf("release %s lacks %s", version, asset)
	}
	checksumURL, ok := release.Assets["fugue_entry_failover_checksums.txt"]
	if !ok {
		return nil, errors.New("executor checksum asset missing")
	}
	dir, err := os.MkdirTemp("", "fugue-entry-failover-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	archivePath, checksumPath := filepath.Join(dir, asset), filepath.Join(dir, "checksums.txt")
	if err = downloadCLIReleaseAsset(ctx, archiveURL, archivePath); err != nil {
		return nil, err
	}
	if err = downloadCLIReleaseAsset(ctx, checksumURL, checksumPath); err != nil {
		return nil, err
	}
	expected, err := readCLIAssetChecksum(checksumPath, asset)
	if err != nil {
		return nil, err
	}
	actual, err := sha256File(archivePath)
	if err != nil {
		return nil, err
	}
	if expected != actual {
		return nil, errors.New("executor release checksum mismatch")
	}
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	for {
		h, e := reader.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if h.Name == "fugue-entry-failover" && h.Typeflag == tar.TypeReg && h.Size > 0 && h.Size <= 100<<20 {
			return io.ReadAll(io.LimitReader(reader, 100<<20))
		}
	}
	return nil, errors.New("executor archive lacks bounded executable")
}
