package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	c "fugue/internal/staticedgecontract"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func loadStaticEdgeBundle(path string) (c.Bundle, error) {
	raw, e := os.ReadFile(path)
	if e != nil {
		return c.Bundle{}, e
	}
	if len(raw) > c.MaxBytes {
		return c.Bundle{}, errors.New("bundle too large")
	}
	if !json.Valid(raw) {
		// Strict YAML then JSON conversion preserves the exact declared schema.
		var doc any
		d := yaml.NewDecoder(bytes.NewReader(raw))
		if e = d.Decode(&doc); e != nil {
			return c.Bundle{}, e
		}
		var extra any
		if d.Decode(&extra) != io.EOF {
			return c.Bundle{}, errors.New("one YAML document required")
		}
		raw, e = json.Marshal(doc)
		if e != nil {
			return c.Bundle{}, e
		}
	}
	var b c.Bundle
	if e = c.StrictJSON(raw, &b); e != nil {
		return b, e
	}
	e = c.ValidateBundle(&b)
	return b, e
}
func (cli *CLI) newStaticEdgeCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "static-edge", Short: "Manage standalone edges directly using mTLS or SSH; no Fugue API", Long: "Manage independent edge/origin managers. Local contexts, signing keys and manager identities are separate from Fugue API credentials. No automatic SSH fallback. Every write requires an expected revision and returns a durable receipt."}
	cmd.AddCommand(cli.newStaticEdgeContextCommand(), cli.newStaticEdgeCloudflareCommand(), cli.newStaticEdgeCutoverCommand(), cli.newStaticEdgeBootstrapCommand(), cli.staticEdgeLocalBundle("validate"), cli.staticEdgeLocalBundle("plan"), cli.staticEdgeKeys(), cli.staticEdgeSign())
	bundle := &cobra.Command{Use: "bundle", Short: "Validate and sign configuration bundles"}
	bundle.AddCommand(cli.staticEdgeLocalBundle("validate"), cli.staticEdgeSign())
	cmd.AddCommand(bundle)
	for _, op := range []string{"status", "health", "evidence", "operation", "stage", "adopt", "activate", "drain", "undrain", "rollback", "recover", "apply"} {
		cmd.AddCommand(cli.staticEdgeOperation(op))
	}
	cert := &cobra.Command{Use: "cert", Short: "Inspect or rotate pre-provisioned management credential slots"}
	s := cli.staticEdgeOperation("cert-status")
	s.Use = "status [context]"
	r := cli.staticEdgeOperation("cert-rotate")
	r.Use = "rotate [context]"
	cert.AddCommand(s, r)
	cmd.AddCommand(cert)
	ls := &cobra.Command{Use: "ls", Args: cobra.NoArgs, Short: "List local targets; does not contact the Fugue API", RunE: func(*cobra.Command, []string) error {
		cfg, e := readStaticEdgeContexts()
		if e != nil {
			return e
		}
		return cli.renderResourceResult(cfg)
	}}
	cmd.AddCommand(ls)
	return cmd
}
func (cli *CLI) newStaticEdgeContextCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "context", Short: "Manage independent local manager contexts"}
	opts := staticEdgeContext{Transport: "mtls", ManagerCommand: "/usr/local/bin/fugue-static-edge-manager"}
	add := &cobra.Command{Use: "add <name>", Args: cobra.ExactArgs(1), Short: "Save manager connection and credential file references", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, e := readStaticEdgeContexts()
		if e != nil {
			return e
		}
		for _, x := range cfg.Contexts {
			if x.Name == args[0] {
				return errors.New("context exists; delete before replacing")
			}
		}
		opts.Name = args[0]
		if e = validateStaticEdgeContext(opts); e != nil {
			return e
		}
		for _, p := range []*string{&opts.CAFile, &opts.ClientCert, &opts.ClientKey} {
			if *p != "" {
				v, e := filepath.Abs(staticEdgePath(*p))
				if e != nil {
					return e
				}
				*p = v
			}
		}
		cfg.Contexts = append(cfg.Contexts, opts)
		if e = saveStaticEdgeContexts(cfg); e != nil {
			return e
		}
		return cli.renderResourceResult(opts)
	}}
	f := add.Flags()
	f.StringVar(&opts.EdgeID, "edge-id", "", "Exact remote manager identity")
	f.StringVar(&opts.Transport, "transport", "mtls", "Default transport: mtls or ssh")
	f.StringVar(&opts.ManagerURL, "manager", "", "Independent HTTPS manager URL")
	f.StringVar(&opts.ServerName, "server-name", "", "TLS verification server name")
	f.StringVar(&opts.CAFile, "ca", "", "Management CA file")
	f.StringVar(&opts.ClientCert, "client-cert", "", "Management client certificate file")
	f.StringVar(&opts.ClientKey, "client-key", "", "Management private key file reference")
	f.StringVar(&opts.SSHHost, "ssh-host", "", "Local OpenSSH alias, also usable as explicit recovery transport")
	f.StringVar(&opts.ManagerCommand, "manager-command", opts.ManagerCommand, "Absolute remote manager executable; shell syntax forbidden")
	_ = add.MarkFlagRequired("edge-id")
	cmd.AddCommand(add)
	for _, name := range []string{"ls", "show", "use", "delete"} {
		op := name
		x := &cobra.Command{Use: op + " <name>", Short: op + " local static edge context", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			cfg, e := readStaticEdgeContexts()
			if e != nil {
				return e
			}
			if op == "ls" {
				sort.Slice(cfg.Contexts, func(i, j int) bool { return cfg.Contexts[i].Name < cfg.Contexts[j].Name })
				return cli.renderResourceResult(cfg)
			}
			name := cfg.Active
			if len(args) > 0 {
				name = args[0]
			}
			for i, x := range cfg.Contexts {
				if x.Name != name {
					continue
				}
				switch op {
				case "show":
					return cli.renderResourceResult(x)
				case "use":
					cfg.Active = name
				case "delete":
					cfg.Contexts = append(cfg.Contexts[:i], cfg.Contexts[i+1:]...)
					if cfg.Active == name {
						cfg.Active = ""
					}
				}
				if e = saveStaticEdgeContexts(cfg); e != nil {
					return e
				}
				return cli.renderResourceResult(map[string]string{"context": name, "operation": op})
			}
			return fmt.Errorf("context %q not found", name)
		}}
		if op == "ls" {
			x.Use = "ls"
			x.Aliases = []string{"list"}
			x.Args = cobra.NoArgs
		}
		if op == "show" {
			x.Use = "show [name]"
			x.Args = cobra.MaximumNArgs(1)
		}
		cmd.AddCommand(x)
	}
	return cmd
}
func (cli *CLI) staticEdgeLocalBundle(op string) *cobra.Command {
	var file string
	cmd := &cobra.Command{Use: op, Args: cobra.NoArgs, Short: op + " a bundle locally; no serving changes", Example: "fugue static-edge " + op + " --file ./edge-bundle.json", RunE: func(*cobra.Command, []string) error {
		b, e := loadStaticEdgeBundle(file)
		if e != nil {
			return e
		}
		return cli.renderResourceResult(map[string]any{"valid": true, "edge_id": b.EdgeID, "generation": b.Generation, "bundle_digest": b.BundleDigest, "signature_present": b.Signature != "", "runtime_verified": false})
	}}
	cmd.Flags().StringVar(&file, "file", "", "Bundle JSON or YAML")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}
func (cli *CLI) staticEdgeKeys() *cobra.Command {
	var private, public string
	cmd := &cobra.Command{Use: "keygen", Args: cobra.NoArgs, Short: "Create Ed25519 bundle signing files without overwriting existing keys", RunE: func(*cobra.Command, []string) error {
		privatePath, e := filepath.Abs(private)
		if e != nil {
			return e
		}
		publicPath, e := filepath.Abs(public)
		if e != nil {
			return e
		}
		if privatePath == publicPath {
			return errors.New("private and public key paths must differ")
		}
		for _, path := range []string{privatePath, publicPath} {
			if _, e := os.Lstat(path); !os.IsNotExist(e) {
				return fmt.Errorf("refusing existing or inaccessible key path %s", path)
			}
		}
		pub, key, e := ed25519.GenerateKey(nil)
		if e != nil {
			return e
		}
		k, e := x509.MarshalPKCS8PrivateKey(key)
		if e != nil {
			return e
		}
		p, e := x509.MarshalPKIXPublicKey(pub)
		if e != nil {
			return e
		}
		for path, block := range map[string]*pem.Block{private: {Type: "PRIVATE KEY", Bytes: k}, public: {Type: "PUBLIC KEY", Bytes: p}} {
			f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				return e
			}
			_, e = f.Write(pem.EncodeToMemory(block))
			ce := f.Close()
			if e != nil {
				return e
			}
			if ce != nil {
				return ce
			}
		}
		return cli.renderResourceResult(map[string]string{"private_key_file": private, "public_key_file": public})
	}}
	cmd.Flags().StringVar(&private, "private-key", "", "New private key file")
	cmd.Flags().StringVar(&public, "public-key", "", "New public key file")
	_ = cmd.MarkFlagRequired("private-key")
	_ = cmd.MarkFlagRequired("public-key")
	return cmd
}
func (cli *CLI) staticEdgeSign() *cobra.Command {
	var file, key, id, out string
	cmd := &cobra.Command{Use: "sign", Args: cobra.NoArgs, Short: "Sign a bundle with an Ed25519 PKCS8 key", RunE: func(*cobra.Command, []string) error {
		b, e := loadStaticEdgeBundle(file)
		if e != nil {
			return e
		}
		raw, e := os.ReadFile(staticEdgePath(key))
		if e != nil {
			return e
		}
		k, e := c.ParsePrivateKeyPEM(raw)
		if e != nil {
			return e
		}
		if e = c.SignBundle(&b, k, id); e != nil {
			return e
		}
		raw, e = json.MarshalIndent(b, "", "  ")
		if e != nil {
			return e
		}
		if e = staticEdgeWriteFile(out, append(raw, '\n')); e != nil {
			return e
		}
		return cli.renderResourceResult(map[string]string{"signed_file": out, "bundle_digest": b.BundleDigest})
	}}
	f := cmd.Flags()
	f.StringVar(&file, "file", "", "Unsigned bundle file")
	f.StringVar(&key, "signing-key", "", "Ed25519 private key file")
	f.StringVar(&id, "key-id", "", "Trusted signer identity")
	f.StringVar(&out, "out", "", "Signed bundle output file")
	for _, n := range []string{"file", "signing-key", "key-id", "out"} {
		_ = cmd.MarkFlagRequired(n)
	}
	return cmd
}
func (cli *CLI) staticEdgeOperation(op string) *cobra.Command {
	var file, transport, id, target, lookup, slot string
	var revision uint64
	var deep bool
	cmd := &cobra.Command{Use: op + " [context]", Args: cobra.MaximumNArgs(1), Short: op + " via the independent manager", RunE: func(cmd *cobra.Command, args []string) error {
		name := ""
		if len(args) > 0 {
			name = args[0]
		}
		cfg, e := loadStaticEdgeContext(name)
		if e != nil {
			return e
		}
		if transport != "" {
			cfg.Transport = transport
		}
		if e = validateStaticEdgeContext(cfg); e != nil {
			return e
		}
		if id == "" {
			id = newStaticRequestID()
		}
		if !c.ValidID(id) || (op == "apply" && len(id) > 80) {
			return errors.New("invalid request id (apply max 80 characters)")
		}
		if !c.ReadOnly(op) {
			fmt.Fprintf(cli.stderr, "static-edge request_id=%s target=%s transport=%s\n", id, cfg.EdgeID, cfg.Transport)
		}
		req := c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: id, Operation: op, TargetDigest: target, LookupRequestID: lookup, CredentialSlot: slot}
		if !c.ReadOnly(op) {
			req.ExpectedRevision = &revision
		}
		if file != "" {
			b, e := loadStaticEdgeBundle(file)
			if e != nil {
				return e
			}
			if b.EdgeID != cfg.EdgeID {
				return errors.New("bundle and context identities differ")
			}
			if b.Signature == "" {
				return errors.New("sign the bundle before contacting manager")
			}
			req.Bundle = &b
		}
		if op == "recover" && cfg.Transport != "ssh" {
			return errors.New("recover requires explicit --transport ssh")
		}
		if op == "apply" {
			req.Operation = "stage"
			req.RequestID = id + "_stage"
			staged, e := staticEdgeCall(cmd.Context(), cfg, req)
			if e != nil {
				if staged.Receipt != nil {
					_ = cli.renderResourceResult(staged)
				}
				return e
			}
			if staged.Receipt == nil {
				return errors.New("stage returned no receipt")
			}
			r := staged.Receipt.Revision
			req = c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: id + "_activate", Operation: "activate", ExpectedRevision: &r, TargetDigest: req.Bundle.BundleDigest}
		}
		response, e := staticEdgeCall(cmd.Context(), cfg, req)
		if response.Schema != "" {
			if re := cli.renderResourceResult(response); re != nil {
				return re
			}
		}
		return e
	}}
	f := cmd.Flags()
	f.StringVar(&transport, "transport", "", "Explicit mtls or ssh override; never automatically falls back")
	f.StringVar(&id, "request-id", "", "Stable id for retry/query after an uncertain response")
	if !c.ReadOnly(op) {
		f.Uint64Var(&revision, "expected-revision", 0, "Exact observed state revision")
		_ = cmd.MarkFlagRequired("expected-revision")
	}
	switch op {
	case "stage", "adopt", "apply":
		f.StringVar(&file, "file", "", "Signed bundle file")
		_ = cmd.MarkFlagRequired("file")
	case "activate", "drain", "undrain", "rollback":
		f.StringVar(&target, "digest", "", "Exact staged or LKG bundle digest")
		_ = cmd.MarkFlagRequired("digest")
	case "operation":
		f.StringVar(&lookup, "id", "", "Request id to inspect")
		_ = cmd.MarkFlagRequired("id")
	case "cert-rotate":
		f.StringVar(&slot, "slot", "", "Pre-provisioned management certificate/trust/grant slot")
		_ = cmd.MarkFlagRequired("slot")
	case "health":
		f.BoolVar(&deep, "deep", true, "Verify exact runtime config and local health checks")
	case "status":
		cmd.Aliases = []string{"show"}
	}
	return cmd
}
