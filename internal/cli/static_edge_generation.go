package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	c "fugue/internal/staticedgecontract"
	"fugue/internal/staticedgemanager"
	"github.com/spf13/cobra"
)

//go:embed static_edge_generation.py
var staticGenerationScript string

type staticGenerationFence struct {
	Unit   string `json:"unit"`
	PID    int    `json:"pid"`
	Config string `json:"config"`
	SHA256 string `json:"sha256"`
}
type staticGenerationPlan struct {
	Schema           string                             `json:"schema"`
	ID               string                             `json:"id"`
	EdgeID           string                             `json:"edge_id"`
	Role             string                             `json:"role"`
	SSHHost          string                             `json:"ssh_host"`
	ManagementIP     string                             `json:"management_ip"`
	ManagementListen string                             `json:"management_listen"`
	SourceCommit     string                             `json:"source_commit"`
	PackageSHA256    string                             `json:"package_sha256"`
	CaddyConfig      json.RawMessage                    `json:"caddy_config"`
	Checks           map[string]staticedgemanager.Probe `json:"checks"`
	Assets           map[string]string                  `json:"assets"` // generation-relative destination -> local source
	Listeners        []string                           `json:"listeners"`
	Preserve         []staticGenerationFence            `json:"preserve"`
}

const staticGenerationSchema = "fugue.static-edge.generation/v1"

var generationID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
var generationSHA = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (p staticGenerationPlan) validate() error {
	if p.Schema != staticGenerationSchema || !generationID.MatchString(p.ID) || !c.ValidID(p.EdgeID) || (p.Role != "edge" && p.Role != "origin") {
		return errors.New("invalid generation identity")
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`).MatchString(p.SSHHost) || net.ParseIP(p.ManagementIP) == nil || !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(p.SourceCommit) || !generationSHA.MatchString(p.PackageSHA256) {
		return errors.New("invalid generation host or pinned artifact")
	}
	if len(p.Listeners) == 0 || len(p.Listeners) > 8 || len(p.Checks) == 0 || len(p.Checks) > 16 || len(p.Assets) > 24 || len(p.Preserve) > 8 {
		return errors.New("generation exceeds bounds")
	}
	seen := map[string]bool{}
	for _, address := range append(append([]string{}, p.Listeners...), p.ManagementListen) {
		host, port, e := net.SplitHostPort(address)
		if e != nil || net.ParseIP(host) == nil || port == "0" || seen[address] {
			return errors.New("explicit unique IP listeners required")
		}
		seen[address] = true
	}
	for name, src := range p.Assets {
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_./-]{0,100}$`).MatchString(name) || strings.Contains(name, "..") || strings.HasPrefix(name, "/") || !filepath.IsAbs(src) {
			return errors.New("invalid generation asset path")
		}
	}
	for _, f := range p.Preserve {
		if !regexp.MustCompile(`^[a-zA-Z0-9_.@-]+\.service$`).MatchString(f.Unit) || f.PID < 1 || !staticEdgeSafeRemotePath.MatchString(f.Config) || !generationSHA.MatchString(f.SHA256) {
			return errors.New("invalid predecessor fence")
		}
	}
	if !json.Valid(p.CaddyConfig) {
		return errors.New("valid Caddy JSON required")
	}
	return nil
}
func generationPackage(raw []byte, plan staticGenerationPlan) (map[string][]byte, error) {
	if len(raw) > 64<<20 || c.Hash(raw) != "sha256:"+plan.PackageSHA256 {
		return nil, errors.New("generation archive hash mismatch")
	}
	gz, e := gzip.NewReader(bytes.NewReader(raw))
	if e != nil {
		return nil, e
	}
	defer gz.Close()
	t := tar.NewReader(gz)
	out := map[string][]byte{}
	allowed := map[string]bool{"caddy": true, "fugue-static-edge-manager": true, "fugue-static-edge-observer": true, "provenance.json": true}
	for {
		h, e := t.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if !allowed[h.Name] || out[h.Name] != nil || h.Typeflag != tar.TypeReg || h.Size < 1 || h.Size > 100<<20 {
			return nil, errors.New("invalid generation archive member")
		}
		out[h.Name], e = io.ReadAll(io.LimitReader(t, 100<<20+1))
		if e != nil {
			return nil, e
		}
	}
	var proof struct {
		Source string            `json:"source_commit"`
		Files  map[string]string `json:"files"`
	}
	if len(out) != 4 || json.Unmarshal(out["provenance.json"], &proof) != nil || proof.Source != plan.SourceCommit {
		return nil, errors.New("generation source provenance mismatch")
	}
	for name, raw := range out {
		if name != "provenance.json" && c.Hash(raw) != "sha256:"+proof.Files[name] {
			return nil, errors.New("generation member digest mismatch")
		}
	}
	return out, nil
}
func (cli *CLI) newStaticGenerationCommand() *cobra.Command {
	var file, archive string
	var execute bool
	cmd := &cobra.Command{Use: "generation", Short: "Stage immutable serving generations alongside predecessors"}
	stage := &cobra.Command{Use: "stage", Args: cobra.NoArgs, Short: "Start a verified independent generation; never stop or reload a predecessor", RunE: func(cmd *cobra.Command, _ []string) error {
		raw, e := os.ReadFile(file)
		if e != nil {
			return e
		}
		var p staticGenerationPlan
		if len(raw) > 4<<20 {
			return errors.New("generation plan too large")
		}
		if e = c.StrictJSON(raw, &p); e != nil {
			return e
		}
		if e = p.validate(); e != nil {
			return e
		}
		pack, e := os.ReadFile(archive)
		if e != nil {
			return e
		}
		members, e := generationPackage(pack, p)
		if e != nil {
			return e
		}
		if !execute {
			return cli.renderResourceResult(map[string]any{"validated": true, "generation": p.ID, "source_commit": p.SourceCommit, "old_processes_retained": true, "dns_changed": false, "execute": false})
		}
		return cli.stageStaticGeneration(cmd.Context(), p, members)
	}}
	stage.Flags().StringVar(&file, "file", "", "Explicit local generation plan")
	stage.Flags().StringVar(&archive, "package", "", "Official CI archive with source provenance and reviewed SHA256")
	stage.Flags().BoolVar(&execute, "execute", false, "Start this generation without changing predecessor listeners or DNS")
	_ = stage.MarkFlagRequired("file")
	_ = stage.MarkFlagRequired("package")
	cmd.AddCommand(stage)
	return cmd
}
func (cli *CLI) stageStaticGeneration(ctx context.Context, p staticGenerationPlan, members map[string][]byte) error {
	unlock, e := acquireStaticEdgeCutoverLock("generation-" + p.ID)
	if e != nil {
		return e
	}
	defer unlock()
	root := "/opt/fugue-static-generations/" + p.ID
	config := bytes.ReplaceAll(p.CaddyConfig, []byte("${GENERATION_ROOT}"), []byte(root))
	var doc map[string]any
	if e = json.Unmarshal(config, &doc); e != nil {
		return e
	}
	// Startup and local management have one declared owner; code never guesses
	// a production address or imports a different serving generation's state.
	doc["admin"] = map[string]any{"listen": "unix/" + root + "/run/caddy.sock", "config": map[string]bool{"persist": false}}
	config, e = json.Marshal(doc)
	if e != nil {
		return e
	}
	identity, e := createStaticEdgeManagementIdentity(p.ID, p.ManagementIP)
	if e != nil {
		return e
	}
	if e = cli.saveStaticEdgeBootstrapIdentity(p.ID, &identity); e != nil {
		return e
	}
	assets := map[string][]byte{}
	for name, source := range p.Assets {
		info, e := os.Lstat(source)
		if e != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
			return errors.New("generation asset must be a bounded regular file")
		}
		assets[name], e = os.ReadFile(source)
		if e != nil {
			return e
		}
	}
	for name, raw := range map[string][]byte{"management/ca.pem": identity.CA, "management/server.pem": identity.ServerCert, "management/server.key": identity.ServerKey, "management/signing.pub": identity.SigningPublic} {
		if _, ok := assets[name]; ok {
			return errors.New("asset conflicts with generated identity")
		}
		assets[name] = raw
	}
	checks := map[string]staticedgemanager.Probe{}
	names := []string{}
	for name, probe := range p.Checks {
		checks[name] = probe
		names = append(names, name)
	}
	sort.Strings(names)
	manager := map[string]any{"edge_id": p.EdgeID, "role": p.Role, "state_dir": root + "/manager-state", "listen": p.ManagementListen, "socket": root + "/run/manager.sock", "ssh_grant": "admin", "verification_keys": map[string]string{"generation": root + "/management/signing.pub"}, "credential_slots": map[string]any{"current": map[string]any{"server_cert": root + "/management/server.pem", "server_key": root + "/management/server.key", "client_ca": root + "/management/ca.pem", "grants": map[string]string{identity.ClientFingerprint: "admin"}}}, "initial_credential_slot": "current", "observation_socket": root + "/run/observer.sock", "caddy": map[string]any{"binary": root + "/caddy", "binary_sha256": c.Hash(members["caddy"]), "admin_socket": root + "/run/caddy.sock", "config_file": root + "/caddy.json", "checks": checks}}
	observer := map[string]any{"socket": root + "/run/observer.sock", "store": map[string]any{"node_id": p.EdgeID, "directory": root + "/evidence"}}
	managerRaw, _ := json.Marshal(manager)
	input := map[string]any{"plan": p, "root": root, "files": members, "assets": assets, "caddy": json.RawMessage(config), "manager": json.RawMessage(managerRaw), "observer": observer}
	wire, e := json.Marshal(input)
	if e != nil {
		return e
	}
	if len(wire) > 128<<20 {
		return errors.New("generation package envelope too large")
	}
	remote := "python3 -c '" + strings.ReplaceAll(staticGenerationScript, "'", "'\\''") + "'"
	out, e := staticEdgeSSHOutput(ctx, p.SSHHost, wire, remote)
	if e != nil {
		return fmt.Errorf("generation staging failed; predecessor retained: %w", e)
	}
	var receipt map[string]any
	if e = json.Unmarshal(out, &receipt); e != nil {
		return errors.New("invalid generation receipt")
	}
	local := filepath.Join(staticEdgeConfigDir(), "generations", p.ID)
	if e = staticEdgeWriteFile(filepath.Join(local, "stage.json"), out); e != nil {
		return e
	}
	b := c.Bundle{Schema: c.SchemaV1, EdgeID: p.EdgeID, Role: p.Role, Generation: 1, Mode: "serving", CaddyConfig: config, HealthChecks: names}
	if e = c.SignBundle(&b, identity.SigningPrivateKey, "generation"); e != nil {
		return e
	}
	braw, _ := json.MarshalIndent(b, "", "  ")
	if e = staticEdgeWriteFile(filepath.Join(local, "bundle.json"), braw); e != nil {
		return e
	}
	context := staticEdgeContext{Name: p.ID, EdgeID: p.EdgeID, Transport: "ssh", SSHHost: p.SSHHost, ManagerCommand: root + "/manager-rpc", ManagerURL: "https://" + p.ManagementListen, ServerName: p.ManagementIP, CAFile: identity.CAPath, ClientCert: identity.ClientCertPath, ClientKey: identity.ClientKeyPath}
	contexts, e := readStaticEdgeContexts()
	if e != nil {
		return e
	}
	found := false
	for _, v := range contexts.Contexts {
		if v.Name == p.ID {
			old, _ := json.Marshal(v)
			next, _ := json.Marshal(context)
			if !bytes.Equal(old, next) {
				return errors.New("generation context drift")
			}
			found = true
		}
	}
	if !found {
		contexts.Contexts = append(contexts.Contexts, context)
		if e = saveStaticEdgeContexts(contexts); e != nil {
			return e
		}
	}
	receipt["context"] = p.ID
	receipt["signed_bundle_file"] = filepath.Join(local, "bundle.json")
	receipt["serving_adopted"] = false
	return cli.renderResourceResult(receipt)
}
