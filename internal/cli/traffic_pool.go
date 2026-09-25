package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/entryfailover"
	sc "fugue/internal/staticedgecontract"
	"github.com/spf13/cobra"
)

type trafficPoolContext struct {
	Name       string `json:"name"`
	Endpoint   string `json:"endpoint"`
	ServerName string `json:"server_name,omitempty"`
	CA         string `json:"ca"`
	ClientCert string `json:"client_cert"`
	ClientKey  string `json:"client_key"`
}
type trafficPoolContexts struct {
	SchemaVersion int                  `json:"schema_version"`
	Active        string               `json:"active,omitempty"`
	Contexts      []trafficPoolContext `json:"contexts"`
}

func trafficPoolContextPath() string {
	if p := os.Getenv("FUGUE_TRAFFIC_POOL_CONTEXT_FILE"); p != "" {
		return p
	}
	d, err := os.UserConfigDir()
	if err != nil {
		d = ".config"
	}
	return filepath.Join(d, "fugue", "traffic-pool-contexts.json")
}

func validateTrafficPoolContext(x trafficPoolContext) error {
	if !sc.ValidID(x.Name) {
		return errors.New("valid context name required")
	}
	u, err := url.Parse(x.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("executor must be an HTTPS authority without path, query or credentials")
	}
	if x.CA == "" || x.ClientCert == "" || x.ClientKey == "" {
		return errors.New("executor mTLS credential paths required")
	}
	return nil
}
func readTrafficPoolContexts() (trafficPoolContexts, error) {
	cfg := trafficPoolContexts{SchemaVersion: 1, Contexts: []trafficPoolContext{}}
	raw, err := os.ReadFile(trafficPoolContextPath())
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if err = sc.StrictJSON(raw, &cfg); err != nil {
		return cfg, err
	}
	if cfg.SchemaVersion != 1 {
		return cfg, errors.New("unsupported traffic pool context schema")
	}
	seen := map[string]bool{}
	for _, x := range cfg.Contexts {
		if err = validateTrafficPoolContext(x); err != nil {
			return cfg, err
		}
		if seen[x.Name] {
			return cfg, errors.New("duplicate traffic pool context")
		}
		seen[x.Name] = true
	}
	if cfg.Active != "" && !seen[cfg.Active] {
		return cfg, errors.New("active traffic pool context missing")
	}
	return cfg, nil
}
func (cfg trafficPoolContexts) save() error {
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return staticEdgeWriteFile(trafficPoolContextPath(), append(raw, '\n'))
}
func trafficPoolContextNamed(name string) (trafficPoolContext, error) {
	cfg, err := readTrafficPoolContexts()
	if err != nil {
		return trafficPoolContext{}, err
	}
	if name == "" {
		name = cfg.Active
	}
	for _, x := range cfg.Contexts {
		if x.Name == name {
			return x, nil
		}
	}
	return trafficPoolContext{}, fmt.Errorf("traffic pool context %q missing", name)
}

func trafficPoolCall(ctx context.Context, cfg trafficPoolContext, method, path string, input any, output any) error {
	if err := validateTrafficPoolContext(cfg); err != nil {
		return err
	}
	ca, err := os.ReadFile(staticEdgePath(cfg.CA))
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid executor CA")
	}
	cert, err := tls.LoadX509KeyPair(staticEdgePath(cfg.ClientCert), staticEdgePath(cfg.ClientKey))
	if err != nil {
		return err
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}, ServerName: cfg.ServerName}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 120 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var body io.Reader
	if input != nil {
		switch value := input.(type) {
		case []byte:
			body = bytes.NewReader(value)
		default:
			raw, err := json.Marshal(value)
			if err != nil {
				return err
			}
			body = bytes.NewReader(raw)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(cfg.Endpoint, "/")+path, body)
	if err != nil {
		return err
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("executor response unknown; inspect status before retry: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("executor %s returned HTTP %d; inspect status/evidence before retry", path, resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return errors.New("executor response too large")
	}
	if output != nil {
		return sc.StrictJSON(raw, output)
	}
	return nil
}

func (cli *CLI) newTrafficPoolCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "traffic-pool", Short: "Manage an independent entry failover executor over mTLS"}
	cmd.AddCommand(cli.newTrafficPoolContextCommand(), cli.newTrafficPoolPolicyCommand(), cli.newTrafficPoolBootstrapCommand(), cli.newTrafficPoolVantageCommand())
	var contextName string
	for _, op := range []string{"status", "evidence", "operation", "preflight"} {
		op := op
		x := &cobra.Command{Use: op, Short: "Inspect independent traffic pool " + op, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := trafficPoolContextNamed(contextName)
			if err != nil {
				return err
			}
			if op == "preflight" {
				var observed entryfailover.Preflight
				if err := trafficPoolCall(cmd.Context(), cfg, http.MethodGet, "/v1/entry-failover/preflight", nil, &observed); err != nil {
					return err
				}
				if err := cli.writeJSON(observed); err != nil {
					return err
				}
				if !observed.PolicyValid || !observed.CredentialReady || observed.Current == "" || len(observed.Errors) != 0 || len(observed.Targets) < 2 {
					return errors.New("traffic pool preflight is not ready")
				}
				for _, vantages := range observed.Targets {
					if len(vantages) < 2 {
						return errors.New("traffic pool preflight lacks independent probe evidence")
					}
					for _, probe := range vantages {
						if !probe.Healthy {
							return errors.New("traffic pool preflight contains an unhealthy target or vantage")
						}
					}
				}
				return nil
			}
			var out json.RawMessage
			if err := trafficPoolCall(cmd.Context(), cfg, http.MethodGet, "/v1/entry-failover/"+op, nil, &out); err != nil {
				return err
			}
			return cli.writeJSON(out)
		}}
		x.Flags().StringVar(&contextName, "executor-context", "", "Independent executor mTLS context")
		cmd.AddCommand(x)
	}
	var switchContext, target string
	sw := &cobra.Command{Use: "switch", Args: cobra.NoArgs, Short: "Explicitly switch a ready traffic pool target", RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := trafficPoolContextNamed(switchContext)
		if err != nil {
			return err
		}
		var status entryfailover.Status
		if err = trafficPoolCall(cmd.Context(), cfg, http.MethodGet, "/v1/entry-failover/status", nil, &status); err != nil {
			return err
		}
		var out json.RawMessage
		err = trafficPoolCall(cmd.Context(), cfg, http.MethodPost, "/v1/entry-failover/switch",
			map[string]string{"target_id": target, "expected_policy_digest": status.PolicyDigest}, &out)
		if err != nil {
			return err
		}
		return cli.writeJSON(out)
	}}
	sw.Flags().StringVar(&switchContext, "executor-context", "", "Independent executor mTLS context")
	sw.Flags().StringVar(&target, "to", "", "Configured target ID")
	_ = sw.MarkFlagRequired("to")
	cmd.AddCommand(sw)
	var credentialContext string
	var tokenStdin bool
	credential := &cobra.Command{Use: "credential-import", Args: cobra.NoArgs, Short: "Import a private Cloudflare token to the independent executor", RunE: func(cmd *cobra.Command, _ []string) error {
		if !tokenStdin {
			return errors.New("--token-stdin required")
		}
		cfg, err := trafficPoolContextNamed(credentialContext)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(cmd.InOrStdin(), 514))
		if err != nil {
			return err
		}
		token := strings.TrimSpace(string(raw))
		if len(raw) > 513 || token == "" || len(token) > 512 || strings.ContainsAny(token, " \t\r\n") {
			return errors.New("invalid token input")
		}
		var out json.RawMessage
		if err = trafficPoolCall(cmd.Context(), cfg, http.MethodPost, "/v1/entry-failover/credential", map[string]string{"token": token}, &out); err != nil {
			return err
		}
		return cli.writeJSON(out)
	}}
	credential.Flags().StringVar(&credentialContext, "executor-context", "", "Independent executor mTLS context")
	credential.Flags().BoolVar(&tokenStdin, "token-stdin", false, "Read token from stdin")
	cmd.AddCommand(credential)
	return cmd
}

func (cli *CLI) newTrafficPoolContextCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "context", Short: "Manage local mTLS executor contexts"}
	var x trafficPoolContext
	add := &cobra.Command{Use: "add <name>", Short: "Save an independent executor mTLS context", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, args []string) error {
		cfg, err := readTrafficPoolContexts()
		if err != nil {
			return err
		}
		x.Name = args[0]
		if err = validateTrafficPoolContext(x); err != nil {
			return err
		}
		for _, old := range cfg.Contexts {
			if old.Name == x.Name {
				return errors.New("context already exists")
			}
		}
		for _, p := range []*string{&x.CA, &x.ClientCert, &x.ClientKey} {
			*p, err = filepath.Abs(staticEdgePath(*p))
			if err != nil {
				return err
			}
		}
		cfg.Contexts = append(cfg.Contexts, x)
		if cfg.Active == "" {
			cfg.Active = x.Name
		}
		if err = cfg.save(); err != nil {
			return err
		}
		return cli.renderResourceResult(x)
	}}
	add.Flags().StringVar(&x.Endpoint, "endpoint", "", "Independent HTTPS executor URL")
	add.Flags().StringVar(&x.ServerName, "server-name", "", "TLS server name or IP SAN")
	add.Flags().StringVar(&x.CA, "ca", "", "Management CA path")
	add.Flags().StringVar(&x.ClientCert, "client-cert", "", "mTLS client certificate path")
	add.Flags().StringVar(&x.ClientKey, "client-key", "", "mTLS client key path")
	cmd.AddCommand(add)
	cmd.AddCommand(&cobra.Command{Use: "ls", Short: "List local executor contexts", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error {
		cfg, err := readTrafficPoolContexts()
		if err != nil {
			return err
		}
		sort.Slice(cfg.Contexts, func(i, j int) bool { return cfg.Contexts[i].Name < cfg.Contexts[j].Name })
		return cli.renderResourceResult(cfg)
	}})
	cmd.AddCommand(&cobra.Command{Use: "use <name>", Short: "Select the active executor context", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, args []string) error {
		cfg, err := readTrafficPoolContexts()
		if err != nil {
			return err
		}
		for _, item := range cfg.Contexts {
			if item.Name == args[0] {
				cfg.Active = args[0]
				if err = cfg.save(); err != nil {
					return err
				}
				return cli.renderResourceResult(map[string]string{"active": args[0]})
			}
		}
		return errors.New("context not found")
	}})
	return cmd
}

func (cli *CLI) newTrafficPoolPolicyCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "policy", Short: "Sign and apply independent traffic policy"}
	var validateFile string
	validate := &cobra.Command{Use: "validate", Short: "Validate a local traffic pool policy without network access", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error {
		raw, err := os.ReadFile(validateFile)
		if err != nil {
			return err
		}
		var policy entryfailover.Policy
		if err = sc.StrictJSON(raw, &policy); err != nil {
			return err
		}
		if err = policy.Validate(); err != nil {
			return err
		}
		return cli.renderResourceResult(map[string]any{"valid": true, "policy_digest": policy.Digest(), "mode": policy.Mode, "hostnames": policy.Hostnames})
	}}
	validate.Flags().StringVar(&validateFile, "file", "", "Unsigned policy JSON file")
	_ = validate.MarkFlagRequired("file")
	cmd.AddCommand(validate)
	var file, privatePath, keyID, outPath string
	sign := &cobra.Command{Use: "sign", Short: "Sign a bounded traffic policy with an offline key", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error {
		raw, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		var policy entryfailover.Policy
		if err = sc.StrictJSON(raw, &policy); err != nil {
			return err
		}
		keyRaw, err := os.ReadFile(staticEdgePath(privatePath))
		if err != nil {
			return err
		}
		key, err := sc.ParsePrivateKeyPEM(keyRaw)
		if err != nil {
			return err
		}
		signed, err := entryfailover.Sign(policy, keyID, key)
		if err != nil {
			return err
		}
		output, err := json.MarshalIndent(signed, "", "  ")
		if err != nil {
			return err
		}
		f, err := os.OpenFile(outPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		if _, err = f.Write(append(output, '\n')); err != nil {
			_ = f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		return cli.renderResourceResult(map[string]string{"policy_digest": signed.Digest, "output": outPath})
	}}
	sign.Flags().StringVar(&file, "file", "", "Policy JSON file")
	sign.Flags().StringVar(&privatePath, "signing-key", "", "Offline Ed25519 private key PEM")
	sign.Flags().StringVar(&keyID, "key-id", "", "Trusted policy signer key ID")
	sign.Flags().StringVar(&outPath, "out", "", "New signed policy JSON path")
	for _, name := range []string{"file", "signing-key", "key-id", "out"} {
		_ = sign.MarkFlagRequired(name)
	}
	cmd.AddCommand(sign)
	var signedPath, contextName string
	apply := &cobra.Command{Use: "apply", Short: "Apply a signed policy to the independent executor", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := trafficPoolContextNamed(contextName)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(signedPath)
		if err != nil {
			return err
		}
		var out json.RawMessage
		if err = trafficPoolCall(cmd.Context(), cfg, http.MethodPost, "/v1/entry-failover/policy", raw, &out); err != nil {
			return err
		}
		return cli.writeJSON(out)
	}}
	apply.Flags().StringVar(&signedPath, "file", "", "Signed policy JSON file")
	apply.Flags().StringVar(&contextName, "executor-context", "", "Independent executor mTLS context")
	_ = apply.MarkFlagRequired("file")
	cmd.AddCommand(apply)
	return cmd
}
