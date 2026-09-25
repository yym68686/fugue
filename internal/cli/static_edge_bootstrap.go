package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	c "fugue/internal/staticedgecontract"

	"github.com/spf13/cobra"
)

type staticEdgeBootstrapOptions struct {
	SSHHost          string
	SourceSSH        string
	EdgeID           string
	PublicIP         string
	Hostnames        []string
	OriginIP         string
	OriginPort       int
	OriginServerName string
	SourceCaddy      string
	SourceCertRoot   string
	SourceClientDir  string
	CaddySHA256      string
	BusinessCA       string
	BusinessCAKey    string
	Execute          bool
}

var staticEdgeSafeRemotePath = regexp.MustCompile(`^/[a-zA-Z0-9_./-]+$`)

func (cli *CLI) newStaticEdgeBootstrapCommand() *cobra.Command {
	var o staticEdgeBootstrapOptions
	cmd := &cobra.Command{Use: "bootstrap", Short: "Initialize a dedicated autonomous static edge over SSH", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return cli.bootstrapStaticEdge(cmd.Context(), o)
	}}
	f := cmd.Flags()
	f.StringVar(&o.SSHHost, "ssh-host", "", "New VPS OpenSSH alias, with root privileges")
	f.StringVar(&o.SourceSSH, "source-ssh", "", "Existing static edge OpenSSH alias used only to copy pinned data-plane assets")
	f.StringVar(&o.EdgeID, "edge-id", "", "New independent manager identity")
	f.StringVar(&o.PublicIP, "public-ip", "", "New VPS public IPv4 address")
	f.StringSliceVar(&o.Hostnames, "hostname", nil, "Exact public hostname to serve; repeat as needed")
	f.StringVar(&o.OriginIP, "origin-ip", "", "Existing static origin IPv4 address")
	f.IntVar(&o.OriginPort, "origin-port", 19443, "Static origin TLS port")
	f.StringVar(&o.OriginServerName, "origin-server-name", "", "Static origin TLS and HTTP host name")
	f.StringVar(&o.SourceCaddy, "source-caddy", "/usr/bin/caddy", "Exact Caddy executable on source edge")
	f.StringVar(&o.SourceCertRoot, "source-cert-root", "/var/lib/caddy/.local/share/caddy/certificates/acme-v02.api.letsencrypt.org-directory", "Existing Caddy certificate storage root")
	f.StringVar(&o.SourceClientDir, "source-client-dir", "/etc/fugue-static-edge", "Source edge business mTLS client material directory")
	f.StringVar(&o.CaddySHA256, "caddy-sha256", "", "Reviewed SHA256 of the source Caddy executable")
	f.StringVar(&o.BusinessCA, "business-ca", "", "Local business CA certificate, already trusted by the origin")
	f.StringVar(&o.BusinessCAKey, "business-ca-key", "", "Local issuer private key; never sent to either server")
	f.BoolVar(&o.Execute, "execute", false, "Install and start isolated services on the new VPS")
	for _, n := range []string{"ssh-host", "source-ssh", "edge-id", "public-ip", "hostname", "origin-ip", "origin-server-name", "caddy-sha256", "business-ca", "business-ca-key"} {
		_ = cmd.MarkFlagRequired(n)
	}
	return cmd
}

func validateStaticEdgeBootstrap(o staticEdgeBootstrapOptions) error {
	for _, h := range []string{o.SSHHost, o.SourceSSH} {
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`).MatchString(h) {
			return errors.New("SSH hosts must be local aliases without shell syntax")
		}
	}
	if o.SSHHost == o.SourceSSH {
		return errors.New("source and target SSH aliases must differ")
	}
	if !c.ValidID(o.EdgeID) {
		return errors.New("valid --edge-id required")
	}
	if ip := net.ParseIP(o.PublicIP); ip == nil || ip.To4() == nil || strings.Contains(o.PublicIP, ":") {
		return errors.New("--public-ip must be IPv4")
	}
	if ip := net.ParseIP(o.OriginIP); ip == nil || ip.To4() == nil || strings.Contains(o.OriginIP, ":") {
		return errors.New("--origin-ip must be IPv4")
	}
	if o.OriginPort < 1 || o.OriginPort > 65535 {
		return errors.New("invalid origin port")
	}
	if len(o.Hostnames) == 0 || len(o.Hostnames) > 16 {
		return errors.New("1-16 public hostnames required")
	}
	seen := map[string]bool{}
	for _, h := range o.Hostnames {
		if seen[h] {
			return errors.New("duplicate hostname")
		}
		seen[h] = true
	}
	for _, h := range append(append([]string{}, o.Hostnames...), o.OriginServerName) {
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9.-]*[a-zA-Z0-9]$`).MatchString(h) || !strings.Contains(h, ".") {
			return fmt.Errorf("invalid hostname %q", h)
		}
	}
	for _, p := range []string{o.SourceCaddy, o.SourceCertRoot, o.SourceClientDir} {
		if !staticEdgeSafeRemotePath.MatchString(p) || strings.Contains(p, "..") {
			return errors.New("source paths must be absolute safe paths")
		}
	}
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(o.CaddySHA256) {
		return errors.New("--caddy-sha256 must be the reviewed 64-character binary digest")
	}
	if !filepath.IsAbs(o.BusinessCA) || !filepath.IsAbs(o.BusinessCAKey) {
		return errors.New("absolute business CA and issuer key paths required")
	}
	return nil
}

type staticEdgeBootstrapAsset struct {
	Path   string
	Local  string
	SHA256 string
	Mode   string
	Owner  string
}
type staticEdgeBootstrapReceipt struct {
	Schema    int
	Intent    string
	EdgeID    string
	Phase     string
	Assets    []staticEdgeBootstrapAsset
	CaddyJSON json.RawMessage
	Bundle    c.Bundle
	Context   staticEdgeContext
}

func (cli *CLI) bootstrapStaticEdge(ctx context.Context, o staticEdgeBootstrapOptions) error {
	if e := validateStaticEdgeBootstrap(o); e != nil {
		return e
	}
	intentOpts := o
	intentOpts.Execute = false
	raw, _ := json.Marshal(intentOpts)
	intent := c.Hash(raw)
	dir := filepath.Join(staticEdgeConfigDir(), "bootstrap", o.EdgeID)
	receiptPath := filepath.Join(dir, "receipt.json")
	unlock, e := acquireStaticEdgeCutoverLock("bootstrap-" + o.EdgeID)
	if e != nil {
		return e
	}
	defer unlock()
	var receipt staticEdgeBootstrapReceipt
	stored, e := os.ReadFile(receiptPath)
	if e == nil {
		if e = c.StrictJSON(stored, &receipt); e != nil {
			return e
		}
		if receipt.Schema != 1 || receipt.Intent != intent || receipt.EdgeID != o.EdgeID {
			return errors.New("bootstrap intent differs from the durable receipt; refusing overwrite")
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	if !o.Execute {
		return cli.writeJSON(map[string]any{"dry_run": true, "intent": intent, "target": o.SSHHost, "public_ip": o.PublicIP, "hostnames": o.Hostnames, "resume_phase": receipt.Phase})
	}
	contexts, e := readStaticEdgeContexts()
	if e != nil {
		return e
	}
	for _, x := range contexts.Contexts {
		if x.Name == o.EdgeID && receipt.Schema == 0 {
			return errors.New("context already exists without this bootstrap receipt")
		}
	}
	if receipt.Schema == 0 {
		preflight := "set -eu; test \"$(id -u)\" = 0; test \"$(uname -s)\" = Linux; test \"$(uname -m)\" = x86_64; command -v openssl >/dev/null; command -v systemctl >/dev/null; test ! -e /etc/fugue-static-edge; test ! -e /etc/systemd/system/static-caddy.service; test ! -e /opt/static-caddy; if ss -lntu | grep -Eq '[:](80|443|9443|18480)[[:space:]]'; then exit 31; fi; ip -o -4 addr show | grep -F ' " + o.PublicIP + "/' >/dev/null; date -u +%s"
		now, e := staticEdgeSSHOutput(ctx, o.SSHHost, nil, preflight)
		if e != nil {
			return fmt.Errorf("target is not a clean Linux amd64 VPS with the claimed IP and free ports: %w", e)
		}
		var epoch int64
		if _, e = fmt.Sscanf(strings.TrimSpace(string(now)), "%d", &epoch); e != nil || time.Since(time.Unix(epoch, 0)).Abs() > 2*time.Minute {
			return errors.New("target clock differs by more than two minutes")
		}
		if _, e = staticEdgeSSHOutput(ctx, o.SourceSSH, nil, "test \"$(uname -s)/$(uname -m)\" = Linux/x86_64"); e != nil {
			return errors.New("source binary must be Linux amd64")
		}
		binary, e := staticEdgeSSHOutput(ctx, o.SourceSSH, nil, "cat "+o.SourceCaddy)
		if e != nil {
			return e
		}
		if c.Hash(binary) != "sha256:"+o.CaddySHA256 {
			return errors.New("source Caddy does not match reviewed digest")
		}
		manager, e := loadStaticEdgeManagerBinary(ctx, "")
		if e != nil {
			return e
		}
		ca, e := os.ReadFile(o.BusinessCA)
		if e != nil {
			return e
		}
		sourceCA, e := staticEdgeSSHOutput(ctx, o.SourceSSH, nil, "cat "+path.Join(o.SourceClientDir, "ca.crt"))
		if e != nil {
			return e
		}
		localCert, e := staticEdgeParseCertificate(ca)
		if e != nil {
			return e
		}
		sourceCert, e := staticEdgeParseCertificate(sourceCA)
		if e != nil {
			return e
		}
		if !bytes.Equal(localCert.Raw, sourceCert.Raw) {
			return errors.New("business CA differs from the source trust")
		}
		if _, _, e = staticEdgeIssuer(o.BusinessCA, o.BusinessCAKey); e != nil {
			return e
		}
		// Every public asset is collected and validated before touching the target.
		type assetData struct {
			path, mode, owner string
			data              []byte
		}
		assets := []assetData{{"/opt/static-caddy/current/caddy", "0755", "root:root", binary}, {"/usr/local/bin/fugue-static-edge-manager", "0755", "root:root", manager}, {"/etc/fugue-static-edge/ca.crt", "0644", "caddy:caddy", ca}}
		for _, host := range o.Hostnames {
			data := map[string][]byte{}
			for _, ext := range []string{"crt", "key", "json"} {
				name := path.Join(o.SourceCertRoot, host, host+"."+ext)
				data[ext], e = staticEdgeSSHOutput(ctx, o.SourceSSH, nil, "cat "+name)
				if e != nil {
					return e
				}
				target := path.Join("/var/lib/caddy/.local/share/caddy/certificates/acme-v02.api.letsencrypt.org-directory", host, host+"."+ext)
				assets = append(assets, assetData{target, "0600", "caddy:caddy", data[ext]})
			}
			pair, e := tls.X509KeyPair(data["crt"], data["key"])
			if e != nil {
				return fmt.Errorf("public TLS key pair for %s: %w", host, e)
			}
			leaf, e := x509.ParseCertificate(pair.Certificate[0])
			if e != nil {
				return e
			}
			if e = leaf.VerifyHostname(host); e != nil {
				return e
			}
			if time.Until(leaf.NotAfter) < 7*24*time.Hour {
				return fmt.Errorf("%s certificate expires in less than seven days", host)
			}
		}
		caddyfile := staticEdgeBootstrapCaddyfile(o)
		config, e := staticEdgeSSHOutput(ctx, o.SourceSSH, []byte(caddyfile), o.SourceCaddy+" adapt --config /dev/stdin --adapter caddyfile")
		if e != nil {
			return e
		}
		if !json.Valid(config) {
			return errors.New("Caddy adaptation returned invalid JSON")
		}
		identity, e := createStaticEdgeManagementIdentity(o.EdgeID, o.PublicIP)
		if e != nil {
			return e
		}
		// Persist an offline management CA and client/signing keys, never on a server.
		if e = cli.saveStaticEdgeBootstrapIdentity(o.EdgeID, &identity); e != nil {
			return e
		}
		policy, e := json.Marshal(staticEdgeBootstrapManagerPolicy(o, c.Hash(binary), identity.ClientFingerprint))
		if e != nil {
			return e
		}
		assets = append(assets, []assetData{
			{"/etc/fugue-static-edge/management/ca.pem", "0644", "root:root", identity.CA},
			{"/etc/fugue-static-edge/management/server.pem", "0644", "root:root", identity.ServerCert},
			{"/etc/fugue-static-edge/management/server.key", "0600", "root:root", identity.ServerKey},
			{"/etc/fugue-static-edge/signing.pub", "0644", "root:root", identity.SigningPublic},
			{"/etc/fugue-static-edge/Caddyfile", "0644", "root:root", []byte(caddyfile)},
			{"/etc/fugue-static-edge/caddy.json", "0644", "root:root", config},
			{"/etc/fugue-static-edge/manager.json", "0600", "root:root", policy},
			{"/etc/systemd/system/static-caddy.service", "0644", "root:root", []byte(staticEdgeCaddySystemdUnit)},
			{"/etc/systemd/system/fugue-static-edge-manager.service", "0644", "root:root", []byte(staticEdgeManagerSystemdUnit)},
		}...)
		cfg := staticEdgeContext{Name: o.EdgeID, EdgeID: o.EdgeID, Transport: "mtls", ManagerURL: "https://" + net.JoinHostPort(o.PublicIP, "9443"), ServerName: identity.ServerName, CAFile: identity.CAPath, ClientCert: identity.ClientCertPath, ClientKey: identity.ClientKeyPath, SSHHost: o.SSHHost, ManagerCommand: "/usr/local/bin/fugue-static-edge-manager"}
		if e = validateStaticEdgeContext(cfg); e != nil {
			return e
		}
		bundle := c.Bundle{Schema: c.SchemaV1, EdgeID: o.EdgeID, Role: "edge", Generation: 1, Mode: "serving", CaddyConfig: config, HealthChecks: []string{"origin-health"}}
		if e = c.SignBundle(&bundle, identity.SigningPrivateKey, "bootstrap"); e != nil {
			return e
		}
		receipt = staticEdgeBootstrapReceipt{Schema: 1, Intent: intent, EdgeID: o.EdgeID, Phase: "prepared", CaddyJSON: config, Bundle: bundle, Context: cfg}
		for i, a := range assets {
			local := filepath.Join(dir, fmt.Sprintf("asset-%02d", i))
			if e = staticEdgeWriteFile(local, a.data); e != nil {
				return e
			}
			receipt.Assets = append(receipt.Assets, staticEdgeBootstrapAsset{a.path, local, c.Hash(a.data), a.mode, a.owner})
		}
		if e = saveStaticEdgeBootstrapReceipt(receiptPath, receipt); e != nil {
			return e
		}
	}
	// The ownership marker is created atomically only on an empty target. A rerun
	// may resume this exact intent, but cannot adopt an unrelated live service.
	marker := strings.TrimPrefix(intent, "sha256:")
	claim := "set -eu; if test -d /etc/fugue-static-edge; then test \"$(cat /etc/fugue-static-edge/.bootstrap-owner)\" = " + marker + "; else mkdir /etc/fugue-static-edge; printf '%s\\n' " + marker + " > /etc/fugue-static-edge/.bootstrap-owner; chmod 0600 /etc/fugue-static-edge/.bootstrap-owner; fi; id -u caddy >/dev/null 2>&1 || useradd --system --home /var/lib/caddy --create-home caddy"
	if _, e = staticEdgeSSHOutput(ctx, o.SSHHost, nil, claim); e != nil {
		return fmt.Errorf("target ownership verification failed: %w", e)
	}
	active, e := staticEdgeSSHOutput(ctx, o.SSHHost, nil, "if systemctl is-active --quiet static-caddy.service; then echo active; else echo inactive; fi")
	if e != nil {
		return e
	}
	if strings.TrimSpace(string(active)) == "inactive" {
		if receipt.Phase == "ready" || receipt.Phase == "candidate_running" {
			return errors.New("previously started candidate is now inactive; refusing bootstrap overwrite; inspect its service and LKG")
		}
		for _, a := range receipt.Assets {
			data, e := os.ReadFile(a.Local)
			if e != nil {
				return e
			}
			if c.Hash(data) != a.SHA256 {
				return errors.New("prepared bootstrap asset digest mismatch")
			}
			if e = staticEdgeSSHWrite(ctx, o.SSHHost, a.Path, data, a.Mode, a.Owner); e != nil {
				return e
			}
		}
		// Only the candidate generates its business private key. The CSR is signed
		// locally by the existing CA; no source private key is copied.
		csr, e := staticEdgeSSHOutput(ctx, o.SSHHost, nil, "set -eu; umask 077; test -s /etc/fugue-static-edge/client.key || openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out /etc/fugue-static-edge/client.key; openssl req -new -key /etc/fugue-static-edge/client.key -subj /CN="+o.EdgeID+" -out /etc/fugue-static-edge/client.csr; chown caddy:caddy /etc/fugue-static-edge/client.key; cat /etc/fugue-static-edge/client.csr")
		if e != nil {
			return e
		}
		cert, e := issueStaticEdgeClientCSR(csr, o.BusinessCA, o.BusinessCAKey)
		if e != nil {
			return e
		}
		if e = staticEdgeSSHWrite(ctx, o.SSHHost, "/etc/fugue-static-edge/client.crt", cert, "0644", "caddy:caddy"); e != nil {
			return e
		}
		prepare := "set -eu; chown -R caddy:caddy /var/lib/caddy; chown root:caddy /etc/fugue-static-edge; chmod 2750 /etc/fugue-static-edge; chown root:caddy /etc/fugue-static-edge/caddy.json; chmod 0640 /etc/fugue-static-edge/caddy.json; /opt/static-caddy/current/caddy validate --config /etc/fugue-static-edge/caddy.json >/dev/null; systemctl daemon-reload; systemctl enable --now static-caddy.service; systemctl enable --now fugue-static-edge-manager.service"
		if _, e = staticEdgeSSHOutput(ctx, o.SSHHost, nil, prepare); e != nil {
			return fmt.Errorf("candidate service preparation: %w", e)
		}
	} else {
		// Never overwrite/restart a live candidate, even if bootstrap previously lost
		// its response. Verify immutable files and resume only management adoption.
		for _, a := range receipt.Assets {
			if strings.HasPrefix(a.Path, "/var/lib/caddy/") {
				continue
			}
			got, e := staticEdgeSSHOutput(ctx, o.SSHHost, nil, "sha256sum "+a.Path)
			if e != nil {
				return e
			}
			if !strings.HasPrefix(string(got), strings.TrimPrefix(a.SHA256, "sha256:")+" ") {
				return fmt.Errorf("live candidate file drift: %s", a.Path)
			}
		}
		if _, e = staticEdgeSSHOutput(ctx, o.SSHHost, nil, "systemctl enable --now fugue-static-edge-manager.service"); e != nil {
			return e
		}
	}
	receipt.Phase = "candidate_running"
	if e = saveStaticEdgeBootstrapReceipt(receiptPath, receipt); e != nil {
		return e
	}
	// Bounded startup readiness retry is read-only; it never replays customer work.
	deadline := time.Now().Add(45 * time.Second)
	var status c.Response
	for {
		status, e = staticEdgeCall(ctx, receipt.Context, c.Request{Schema: c.RPCSchema, EdgeID: o.EdgeID, RequestID: newStaticRequestID(), Operation: "status"})
		if e == nil && status.Result != nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("candidate manager readiness: %w", e)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	for _, host := range o.Hostnames {
		if e = probeStaticEdgeEndpoint(ctx, o.PublicIP, host, 443, "/_static-edge/health", 20*time.Second); e != nil {
			return e
		}
	}
	if status.Result.ActiveDigest == "" {
		rev := status.Result.Revision
		_, e = staticEdgeCall(ctx, receipt.Context, c.Request{Schema: c.RPCSchema, EdgeID: o.EdgeID, RequestID: "bootstrap-adopt-" + o.EdgeID, Operation: "adopt", ExpectedRevision: &rev, Bundle: &receipt.Bundle})
		if e != nil {
			return fmt.Errorf("candidate adoption: %w", e)
		}
	} else if status.Result.ActiveDigest != receipt.Bundle.BundleDigest {
		return errors.New("candidate already has a different active bundle")
	}
	found := false
	for _, x := range contexts.Contexts {
		if x.Name == o.EdgeID {
			a, _ := json.Marshal(x)
			b, _ := json.Marshal(receipt.Context)
			if !bytes.Equal(a, b) {
				return errors.New("local candidate context drift")
			}
			found = true
		}
	}
	if !found {
		contexts.Contexts = append(contexts.Contexts, receipt.Context)
		if e = saveStaticEdgeContexts(contexts); e != nil {
			return e
		}
	}
	receipt.Phase = "ready"
	if e = saveStaticEdgeBootstrapReceipt(receiptPath, receipt); e != nil {
		return e
	}
	return cli.writeJSON(map[string]any{"ready": true, "edge_id": o.EdgeID, "public_ip": o.PublicIP, "hostnames": o.Hostnames, "manager_transport": "mtls", "business_identity": "unique CSR signed by existing CA", "old_edge_untouched": true, "receipt": receiptPath})
}
func saveStaticEdgeBootstrapReceipt(p string, r staticEdgeBootstrapReceipt) error {
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	return staticEdgeWriteFile(p, append(b, '\n'))
}
func staticEdgeParseCertificate(raw []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("invalid certificate PEM")
	}
	return x509.ParseCertificate(block.Bytes)
}
func staticEdgeIssuer(certPath, keyPath string) (*x509.Certificate, crypto.Signer, error) {
	certRaw, e := os.ReadFile(certPath)
	if e != nil {
		return nil, nil, e
	}
	cert, e := staticEdgeParseCertificate(certRaw)
	if e != nil {
		return nil, nil, e
	}
	if !cert.IsCA || time.Now().Before(cert.NotBefore) || time.Until(cert.NotAfter) < 30*24*time.Hour {
		return nil, nil, errors.New("business issuer must be a valid CA with 30 days remaining")
	}
	raw, e := os.ReadFile(keyPath)
	if e != nil {
		return nil, nil, e
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, nil, errors.New("invalid issuer key")
	}
	var key any
	switch block.Type {
	case "PRIVATE KEY":
		key, e = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		key, e = x509.ParseECPrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		key, e = x509.ParsePKCS1PrivateKey(block.Bytes)
	default:
		e = errors.New("unsupported issuer key")
	}
	if e != nil {
		return nil, nil, e
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, nil, errors.New("issuer is not a signing key")
	}
	pub, _ := x509.MarshalPKIXPublicKey(signer.Public())
	if !bytes.Equal(pub, cert.RawSubjectPublicKeyInfo) {
		return nil, nil, errors.New("issuer key does not match CA")
	}
	return cert, signer, nil
}
func issueStaticEdgeClientCSR(raw []byte, caPath, keyPath string) ([]byte, error) {
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, errors.New("invalid business CSR")
	}
	csr, e := x509.ParseCertificateRequest(block.Bytes)
	if e != nil {
		return nil, e
	}
	if e = csr.CheckSignature(); e != nil {
		return nil, e
	}
	ca, key, e := staticEdgeIssuer(caPath, keyPath)
	if e != nil {
		return nil, e
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if e != nil {
		return nil, e
	}
	now := time.Now()
	expires := now.AddDate(1, 0, 0)
	if expires.After(ca.NotAfter) {
		expires = ca.NotAfter
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: csr.Subject, NotBefore: now.Add(-time.Hour), NotAfter: expires, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, e := x509.CreateCertificate(rand.Reader, template, ca, csr.PublicKey, key)
	if e != nil {
		return nil, e
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

type staticEdgeBootstrapIdentity struct {
	CA, CAKey, ServerCert, ServerKey, ClientCert, ClientKey, SigningPublic []byte
	SigningPrivateKey                                                      ed25519.PrivateKey
	ClientFingerprint, ServerName, CAPath, ClientCertPath, ClientKeyPath   string
}

func createStaticEdgeManagementIdentity(edgeID, ip string) (staticEdgeBootstrapIdentity, error) {
	var out staticEdgeBootstrapIdentity
	out.ServerName = edgeID + ".static-edge.invalid"
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return out, err
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: edgeID + " management CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(3, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return out, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return out, err
	}
	out.CA = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caPKCS8, err := x509.MarshalPKCS8PrivateKey(caKey)
	if err != nil {
		return out, err
	}
	out.CAKey = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: caPKCS8})
	issue := func(name string, usage x509.ExtKeyUsage, dns []string) ([]byte, []byte, []byte, error) {
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			return nil, nil, nil, e
		}
		serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		if e != nil {
			return nil, nil, nil, e
		}
		tpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, DNSNames: dns, NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(1, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, e := x509.CreateCertificate(rand.Reader, tpl, caCert, &key.PublicKey, caKey)
		if e != nil {
			return nil, nil, nil, e
		}
		pkcs, e := x509.MarshalPKCS8PrivateKey(key)
		if e != nil {
			return nil, nil, nil, e
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs}), der, nil
	}
	out.ServerCert, out.ServerKey, _, err = issue(out.ServerName, x509.ExtKeyUsageServerAuth, []string{out.ServerName})
	if err != nil {
		return out, err
	}
	var clientDER []byte
	out.ClientCert, out.ClientKey, clientDER, err = issue(edgeID+" operator", x509.ExtKeyUsageClientAuth, nil)
	if err != nil {
		return out, err
	}
	sum := sha256.Sum256(clientDER)
	out.ClientFingerprint = hex.EncodeToString(sum[:])
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return out, err
	}
	out.SigningPrivateKey = priv
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return out, err
	}
	out.SigningPublic = pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	_ = ip
	return out, nil
}

func (cli *CLI) saveStaticEdgeBootstrapIdentity(edgeID string, x *staticEdgeBootstrapIdentity) error {
	dir := filepath.Join(staticEdgeConfigDir(), "identities", edgeID)
	identityPath := filepath.Join(dir, "identity.json")
	if saved, e := os.ReadFile(identityPath); e == nil {
		if e = c.StrictJSON(saved, x); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	} else {
		b, e := json.Marshal(x)
		if e != nil {
			return e
		}
		if e = staticEdgeWriteFile(identityPath, b); e != nil {
			return e
		}
	}
	if len(x.SigningPrivateKey) != ed25519.PrivateKeySize {
		return errors.New("invalid stored signing key")
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(x.SigningPrivateKey)
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{"ca.pem": x.CA, "ca.key": x.CAKey, "client.pem": x.ClientCert, "client.key": x.ClientKey, "signing.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), "signing.pub": x.SigningPublic} {
		p := filepath.Join(dir, name)
		if b, e := os.ReadFile(p); e == nil {
			if !bytes.Equal(b, data) {
				return fmt.Errorf("identity file drift: %s", p)
			}
			continue
		} else if !os.IsNotExist(e) {
			return e
		}
		if err := staticEdgeWriteFile(p, data); err != nil {
			return err
		}
	}
	x.CAPath = filepath.Join(dir, "ca.pem")
	x.ClientCertPath = filepath.Join(dir, "client.pem")
	x.ClientKeyPath = filepath.Join(dir, "client.key")
	return nil
}

func staticEdgeBootstrapManagerPolicy(o staticEdgeBootstrapOptions, binaryHash, fingerprint string) map[string]any {
	return map[string]any{"edge_id": o.EdgeID, "role": "edge", "state_dir": "/var/lib/fugue-static-edge-manager", "listen": "0.0.0.0:9443", "socket": "/run/fugue-static-edge-manager/manager.sock", "ssh_grant": "admin", "verification_keys": map[string]string{"bootstrap": "/etc/fugue-static-edge/signing.pub"}, "credential_slots": map[string]any{"current": map[string]any{"server_cert": "/etc/fugue-static-edge/management/server.pem", "server_key": "/etc/fugue-static-edge/management/server.key", "client_ca": "/etc/fugue-static-edge/management/ca.pem", "grants": map[string]string{fingerprint: "admin"}}}, "initial_credential_slot": "current", "caddy": map[string]any{"binary": "/opt/static-caddy/current/caddy", "binary_sha256": binaryHash, "admin_socket": "/run/static-caddy/admin.sock", "config_file": "/etc/fugue-static-edge/caddy.json", "checks": map[string]any{"origin-health": map[string]any{"url": "http://127.0.0.1:18480/_static-edge/health", "status": 200}}}}
}

func staticEdgeBootstrapCaddyfile(o staticEdgeBootstrapOptions) string {
	backend := fmt.Sprintf(`reverse_proxy https://%s:%d {
  header_up Host %s
  header_up X-Forwarded-Proto https
  flush_interval -1
  transport http {
    tls_server_name %s
    tls_trust_pool file /etc/fugue-static-edge/ca.crt
    tls_client_auth /etc/fugue-static-edge/client.crt /etc/fugue-static-edge/client.key
    versions 1.1
    keepalive 30s
    read_timeout 24h
    write_timeout 24h
    response_header_timeout 0s
  }
}`, o.OriginIP, o.OriginPort, o.OriginServerName, o.OriginServerName)
	return fmt.Sprintf(`{
  admin unix//run/static-caddy/admin.sock
  persist_config off
  grace_period 120s
}
:18480 {
  bind 127.0.0.1
  handle /_static-edge/health {
    %s
  }
  handle {
    %s
  }
}
%s {
  header X-Fugue-Static-Edge %s
  log {
    output file /var/lib/caddy/static-edge-access.json {
      roll_size 10MiB
      roll_keep 3
    }
    format json
  }
  reverse_proxy 127.0.0.1:18480 {
    flush_interval -1
    transport http {
      versions 1.1
      keepalive 30s
      read_timeout 24h
      write_timeout 24h
      response_header_timeout 0s
    }
  }
}
`, backend, backend, strings.Join(o.Hostnames, ", "), o.EdgeID)
}

const staticEdgeCaddySystemdUnit = `[Unit]
Description=Dedicated autonomous static edge data plane
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=caddy
Group=caddy
ExecStart=/opt/static-caddy/current/caddy run --config /etc/fugue-static-edge/caddy.json
Restart=on-failure
RestartSec=3s
TimeoutStopSec=130s
LimitNOFILE=1048576
RuntimeDirectory=static-caddy
RuntimeDirectoryMode=0700
StateDirectory=caddy
Environment=HOME=/var/lib/caddy
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true

[Install]
WantedBy=multi-user.target
`

const staticEdgeManagerSystemdUnit = `[Unit]
Description=Independent static edge configuration manager
After=network-online.target static-caddy.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/fugue-static-edge-manager --config /etc/fugue-static-edge/manager.json
Restart=on-failure
RestartSec=5s
TimeoutStopSec=120s
UMask=0077
RuntimeDirectory=fugue-static-edge-manager
RuntimeDirectoryMode=0700
StateDirectory=fugue-static-edge-manager
StateDirectoryMode=0700
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=/etc/fugue-static-edge

[Install]
WantedBy=multi-user.target
`

func staticEdgeSSHOutput(ctx context.Context, alias string, stdin []byte, remoteCommand string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3", "-o", "StrictHostKeyChecking=yes", "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", alias, remoteCommand)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ssh %s command failed: %w: %s", alias, err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func staticEdgeSSHWrite(ctx context.Context, alias, path string, data []byte, mode, owner string) error {
	if !staticEdgeSafeRemotePath.MatchString(path) || strings.Contains(path, "..") || !regexp.MustCompile(`^0[0-7]{3}$`).MatchString(mode) || !regexp.MustCompile(`^[a-z]+:[a-z]+$`).MatchString(owner) {
		return errors.New("unsafe remote file destination")
	}
	dir := filepath.Dir(path)
	script := fmt.Sprintf("set -eu; umask 077; install -d -m 0755 %s; tmp=$(mktemp %s/.fugue-bootstrap.XXXXXX); trap 'rm -f \"$tmp\"' EXIT; cat > \"$tmp\"; test \"$(sha256sum \"$tmp\" | cut -d ' ' -f 1)\" = %s; chmod %s \"$tmp\"; chown %s \"$tmp\"; mv \"$tmp\" %s", dir, dir, strings.TrimPrefix(c.Hash(data), "sha256:"), mode, owner, path)
	_, err := staticEdgeSSHOutput(ctx, alias, data, script)
	if err != nil {
		return fmt.Errorf("upload %s: %w", path, err)
	}
	return nil
}

func loadStaticEdgeManagerBinary(ctx context.Context, localPath string) ([]byte, error) {
	if localPath != "" {
		return os.ReadFile(localPath)
	}
	version := currentCLIBuildInfo().Version
	if !strings.HasPrefix(version, "v") {
		return nil, errors.New("bootstrap requires a tagged CLI release")
	}
	release, err := fetchCLIRelease(ctx, version)
	if err != nil {
		return nil, err
	}
	asset := "fugue_static_edge_manager_linux_amd64.tar.gz"
	archiveURL, ok := release.Assets[asset]
	if !ok {
		return nil, fmt.Errorf("release %s lacks %s", version, asset)
	}
	checksumURL, ok := release.Assets["fugue_static_edge_manager_checksums.txt"]
	if !ok {
		return nil, errors.New("manager checksum asset missing")
	}
	dir, err := os.MkdirTemp("", "fugue-static-edge-manager-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	archivePath := filepath.Join(dir, asset)
	checksumPath := filepath.Join(dir, "checksums.txt")
	if err := downloadCLIReleaseAsset(ctx, archiveURL, archivePath); err != nil {
		return nil, err
	}
	if err := downloadCLIReleaseAsset(ctx, checksumURL, checksumPath); err != nil {
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
		return nil, errors.New("manager release checksum mismatch")
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
	tarReader := tar.NewReader(gz)
	for {
		header, e := tarReader.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if header != nil && path.Base(header.Name) == "fugue-static-edge-manager" {
			return io.ReadAll(io.LimitReader(tarReader, 100<<20))
		}
	}
	return nil, errors.New("manager release archive lacks executable")
}

func tlsKeyPair(cert, key []byte) (any, error) { return tls.X509KeyPair(cert, key) }
