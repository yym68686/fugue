package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"fugue/internal/model"
	c "fugue/internal/staticedgecontract"
	"github.com/spf13/cobra"
)

type staticEdgeAppAddOptions struct {
	HTTP3Port        int
	EdgeContext      string
	OriginContext    string
	EdgeBundle       string
	OriginBundle     string
	EdgeServer       string
	OriginServer     string
	EdgeTemplateHost string
	Hostname         string
	PathPrefix       string
	Upstream         string
	SigningKey       string
	EdgeSigningKey   string
	OriginSigningKey string
	EdgeKeyID        string
	OriginKeyID      string
	OutputDir        string
	Execute          bool
}

type staticEdgeAppAddResult struct {
	AppID            string                   `json:"app_id"`
	AppName          string                   `json:"app_name"`
	Hostname         string                   `json:"hostname"`
	PathPrefix       string                   `json:"path_prefix"`
	Upstream         string                   `json:"upstream"`
	EdgeBundle       string                   `json:"edge_bundle"`
	OriginBundle     string                   `json:"origin_bundle"`
	EdgeGeneration   uint64                   `json:"edge_generation"`
	OriginGeneration uint64                   `json:"origin_generation"`
	Signed           bool                     `json:"signed"`
	Executed         bool                     `json:"executed"`
	DNSChanged       bool                     `json:"dns_changed"`
	OperationFile    string                   `json:"operation_file"`
	Phase            string                   `json:"phase"`
	Operations       []staticEdgeAppOperation `json:"operations,omitempty"`
	EdgeRevision     uint64                   `json:"edge_revision"`
	OriginRevision   uint64                   `json:"origin_revision"`
}

type staticEdgeAppOperation struct {
	ManagerID string     `json:"manager_id"`
	Operation string     `json:"operation"`
	RequestID string     `json:"request_id"`
	State     string     `json:"state"`
	Revision  uint64     `json:"revision,omitempty"`
	Receipt   *c.Receipt `json:"receipt,omitempty"`
}

var staticEdgeAppHostnamePattern = regexp.MustCompile(`^(?i:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)(?:\.(?i:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?))*$`)

func (cli *CLI) newStaticEdgeAppCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "app",
		Short: "Build and optionally apply independent static-edge routes for a Fugue app",
		Long:  strings.TrimSpace(`Resolve a Fugue app's stable internal Service, add its hostname route to full edge/origin bundle baselines, sign the candidates, and optionally apply them directly to the independent managers. This command does not change Fugue app configuration, release state, or DNS.`),
	}
	var opts staticEdgeAppAddOptions
	add := &cobra.Command{
		Use:   "add <app>",
		Short: "Add a Fugue app to independent static edge/origin bundles",
		Args:  cobra.ExactArgs(1),
		Example: strings.TrimSpace(`fugue static-edge app add my-app \
		  --edge-context static-entry --origin-context static-origin \
		  --edge-bundle ./current-edge.json --origin-bundle ./current-origin.json \
		  --edge-template-host existing.example.com --signing-key ./static-edge.key \
		  --out-dir ./static-edge-routes/my-app --execute`),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := cli.newClient()
			if err != nil {
				return err
			}
			app, err := cli.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			return cli.addStaticEdgeApp(cmd.Context(), app, opts)
		},
	}
	flags := add.Flags()
	flags.IntVar(&opts.HTTP3Port, "http3-port", 0, "Explicit public HTTP/3 port when it differs from the template's advertised port (1-65535)")
	flags.StringVar(&opts.EdgeContext, "edge-context", "", "Existing independent edge manager context")
	flags.StringVar(&opts.OriginContext, "origin-context", "", "Existing independent origin manager context")
	flags.StringVar(&opts.EdgeBundle, "edge-bundle", "", "Complete current edge bundle JSON/YAML baseline matching the manager's active digest")
	flags.StringVar(&opts.OriginBundle, "origin-bundle", "", "Complete current origin bundle JSON/YAML baseline matching the manager's active digest")
	flags.StringVar(&opts.EdgeServer, "edge-server", "", "HTTP server key in the edge Caddy JSON (auto-selected only when exactly one exists)")
	flags.StringVar(&opts.OriginServer, "origin-server", "", "HTTP server key in the origin Caddy JSON (auto-selected only when exactly one exists)")
	flags.StringVar(&opts.EdgeTemplateHost, "edge-template-host", "", "Existing public hostname whose edge route will be cloned to preserve the origin hop")
	flags.StringVar(&opts.Hostname, "hostname", "", "Hostname to serve (defaults to the app's primary Fugue hostname)")
	flags.StringVar(&opts.PathPrefix, "path-prefix", "", "Path prefix to route (defaults to the app route prefix, or / for an explicit hostname)")
	flags.StringVar(&opts.Upstream, "upstream", "", "Override origin-reachable app Service as host:port; defaults to Fugue internal_service")
	flags.StringVar(&opts.SigningKey, "signing-key", "", "Local Ed25519 PKCS8 bundle signer shared by both managers")
	flags.StringVar(&opts.EdgeSigningKey, "edge-signing-key", "", "Edge signer file (overrides --signing-key for the edge)")
	flags.StringVar(&opts.OriginSigningKey, "origin-signing-key", "", "Origin signer file (overrides --signing-key for the origin)")
	flags.StringVar(&opts.EdgeKeyID, "edge-key-id", "", "Trusted signer key ID for edge manager (defaults to the source edge bundle key ID)")
	flags.StringVar(&opts.OriginKeyID, "origin-key-id", "", "Trusted signer key ID for origin manager (defaults to the source origin bundle key ID)")
	flags.StringVar(&opts.OutputDir, "out-dir", "", "New output directory for candidate bundle files; existing files are never overwritten")
	flags.BoolVar(&opts.Execute, "execute", false, "After status, generation, and source-digest checks, apply origin first and edge second")
	for _, name := range []string{"edge-context", "origin-context", "edge-bundle", "origin-bundle", "edge-template-host", "out-dir"} {
		_ = add.MarkFlagRequired(name)
	}
	cmd.AddCommand(add, cli.newStaticEdgeAppDNSCommand())
	return cmd
}

func (cli *CLI) addStaticEdgeApp(ctx context.Context, app model.App, opts staticEdgeAppAddOptions) error {
	if opts.HTTP3Port < 0 || opts.HTTP3Port > 65535 {
		return errors.New("--http3-port must be 1-65535 when supplied")
	}
	if app.InternalService == nil || strings.TrimSpace(app.InternalService.Host) == "" || app.InternalService.Port < 1 || app.InternalService.Port > 65535 {
		return errors.New("app has no usable Fugue internal Service; only HTTP apps with a stable internal Service can be routed")
	}
	if opts.EdgeContext == opts.OriginContext {
		return errors.New("edge and origin contexts must identify different managers")
	}
	edgeContext, err := loadStaticEdgeContext(opts.EdgeContext)
	if err != nil {
		return err
	}
	originContext, err := loadStaticEdgeContext(opts.OriginContext)
	if err != nil {
		return err
	}
	if edgeContext.EdgeID == originContext.EdgeID {
		return errors.New("edge and origin manager identities must differ")
	}
	edgeBase, err := loadStaticEdgeBundle(opts.EdgeBundle)
	if err != nil {
		return fmt.Errorf("load edge baseline: %w", err)
	}
	originBase, err := loadStaticEdgeBundle(opts.OriginBundle)
	if err != nil {
		return fmt.Errorf("load origin baseline: %w", err)
	}
	if edgeBase.EdgeID != edgeContext.EdgeID || edgeBase.Role != "edge" {
		return errors.New("edge bundle identity/role does not match edge context")
	}
	if originBase.EdgeID != originContext.EdgeID || originBase.Role != "origin" {
		return errors.New("origin bundle identity/role does not match origin context")
	}
	edgeObserved, err := observeStaticEdgeAppManager(ctx, edgeContext, "edge")
	if err != nil {
		return fmt.Errorf("observe edge manager: %w", err)
	}
	originObserved, err := observeStaticEdgeAppManager(ctx, originContext, "origin")
	if err != nil {
		return fmt.Errorf("observe origin manager: %w", err)
	}
	if err = matchStaticEdgeAppBaseline(edgeBase, edgeObserved); err != nil {
		return fmt.Errorf("edge baseline: %w", err)
	}
	if err = matchStaticEdgeAppBaseline(originBase, originObserved); err != nil {
		return fmt.Errorf("origin baseline: %w", err)
	}
	hostname, pathPrefix, err := staticEdgeAppRouteTarget(app, opts)
	if err != nil {
		return err
	}
	upstream := strings.TrimSpace(opts.Upstream)
	if upstream == "" {
		upstream = net.JoinHostPort(app.InternalService.Host, strconv.Itoa(app.InternalService.Port))
	}
	if err = validateStaticEdgeAppUpstream(upstream); err != nil {
		return err
	}
	edgeConfig, err := decodeStaticEdgeAppCaddyConfig(edgeBase.CaddyConfig)
	if err != nil {
		return fmt.Errorf("decode edge Caddy config: %w", err)
	}
	originConfig, err := decodeStaticEdgeAppCaddyConfig(originBase.CaddyConfig)
	if err != nil {
		return fmt.Errorf("decode origin Caddy config: %w", err)
	}
	edgeServers, err := staticEdgeAppHTTPServers(edgeConfig, opts.EdgeServer, opts.EdgeTemplateHost)
	if err != nil {
		return fmt.Errorf("edge server: %w", err)
	}
	originServer, err := staticEdgeAppHTTPServer(originConfig, opts.OriginServer)
	if err != nil {
		return fmt.Errorf("origin server: %w", err)
	}
	for _, edgeServer := range edgeServers {
		if staticEdgeAppServerHasHostname(edgeServer, hostname) {
			matches, matchErr := staticEdgeAppExistingEdgeRoutesMatch(edgeServer, opts.EdgeTemplateHost, hostname, pathPrefix)
			if matchErr != nil {
				return fmt.Errorf("existing edge route: %w", matchErr)
			}
			if !matches {
				return fmt.Errorf("edge Caddy config already contains hostname %q with a different route; refusing to treat it as attached", hostname)
			}
			continue
		}
		if err = staticEdgeAppAddEdgeRoute(edgeConfig, edgeServer, opts.EdgeTemplateHost, hostname, pathPrefix); err != nil {
			return fmt.Errorf("edge route: %w", err)
		}
	}
	staticEdgeAppAddTLSSubject(edgeConfig, opts.EdgeTemplateHost, hostname)
	if opts.HTTP3Port > 0 {
		for _, server := range edgeServers {
			staticEdgeAppSetHTTP3Port(server, hostname, opts.HTTP3Port)
		}
	}
	if staticEdgeAppServerHasForwardedHost(originServer, hostname) {
		if !staticEdgeAppExistingOriginRouteMatches(originServer, hostname, pathPrefix, upstream) {
			return fmt.Errorf("origin Caddy config already contains forwarded hostname %q with a different route; refusing to treat it as attached", hostname)
		}
	} else {
		if err = staticEdgeAppAddOriginRoute(originServer, hostname, pathPrefix, upstream); err != nil {
			return fmt.Errorf("origin route: %w", err)
		}
	}
	edgeBase.Generation = edgeObserved.ActiveGeneration + 1
	originBase.Generation = originObserved.ActiveGeneration + 1
	edgeBase.Signature, edgeBase.BundleDigest = "", ""
	originBase.Signature, originBase.BundleDigest = "", ""
	edgeBase.CaddyConfig, err = json.Marshal(edgeConfig)
	if err != nil {
		return err
	}
	originBase.CaddyConfig, err = json.Marshal(originConfig)
	if err != nil {
		return err
	}
	edgeKeyID := strings.TrimSpace(opts.EdgeKeyID)
	if edgeKeyID == "" {
		edgeKeyID = edgeBase.SigningKeyID
	}
	originKeyID := strings.TrimSpace(opts.OriginKeyID)
	if originKeyID == "" {
		originKeyID = originBase.SigningKeyID
	}
	edgeSigner := firstNonEmpty(opts.EdgeSigningKey, opts.SigningKey)
	originSigner := firstNonEmpty(opts.OriginSigningKey, opts.SigningKey)
	if opts.Execute && (edgeSigner == "" || originSigner == "") {
		return errors.New("--execute requires signing keys for both managers; use --signing-key or --edge-signing-key and --origin-signing-key")
	}
	if err = signStaticEdgeAppBundle(&edgeBase, edgeSigner, edgeKeyID); err != nil {
		return fmt.Errorf("edge candidate: %w", err)
	}
	if err = signStaticEdgeAppBundle(&originBase, originSigner, originKeyID); err != nil {
		return fmt.Errorf("origin candidate: %w", err)
	}
	if err = validateStaticEdgeAppOutputPaths(opts.OutputDir, opts.EdgeBundle, opts.OriginBundle); err != nil {
		return err
	}
	outDir, err := filepath.Abs(staticEdgePath(opts.OutputDir))
	if err != nil {
		return err
	}
	if err = os.MkdirAll(outDir, 0700); err != nil {
		return err
	}
	edgePath := filepath.Join(outDir, "edge.bundle.json")
	originPath := filepath.Join(outDir, "origin.bundle.json")
	for _, path := range []string{edgePath, originPath} {
		if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
			if statErr == nil {
				return fmt.Errorf("refusing to overwrite existing output %s", path)
			}
			return statErr
		}
	}
	if err = writeStaticEdgeAppBundle(edgePath, edgeBase); err != nil {
		return err
	}
	if err = writeStaticEdgeAppBundle(originPath, originBase); err != nil {
		return err
	}
	result := staticEdgeAppAddResult{
		AppID: app.ID, AppName: app.Name, Hostname: hostname, PathPrefix: pathPrefix,
		Upstream: upstream, EdgeBundle: edgePath, OriginBundle: originPath,
		EdgeGeneration: edgeBase.Generation, OriginGeneration: originBase.Generation,
		Signed: edgeBase.Signature != "" && originBase.Signature != "", Executed: false, DNSChanged: false,
		OperationFile: filepath.Join(outDir, "operation.json"), Phase: "prepared",
		EdgeRevision: edgeObserved.Revision, OriginRevision: originObserved.Revision,
	}
	result.Operations = []staticEdgeAppOperation{
		{ManagerID: originContext.EdgeID, Operation: "stage", RequestID: newStaticRequestID(), State: "planned"},
		{ManagerID: edgeContext.EdgeID, Operation: "stage", RequestID: newStaticRequestID(), State: "planned"},
		{ManagerID: originContext.EdgeID, Operation: "activate", RequestID: newStaticRequestID(), State: "planned"},
		{ManagerID: edgeContext.EdgeID, Operation: "activate", RequestID: newStaticRequestID(), State: "planned"},
	}
	if err = writeStaticEdgeAppResult(result, true); err != nil {
		return err
	}
	if !opts.Execute {
		return cli.renderResourceResult(result)
	}
	return cli.executeStaticEdgeApp(ctx, &result, edgeContext, originContext, edgeBase, originBase, edgeObserved.Revision, originObserved.Revision)
}

func signStaticEdgeAppBundle(bundle *c.Bundle, file, keyID string) error {
	bundle.SigningKeyID = keyID
	if strings.TrimSpace(file) == "" {
		return c.ValidateBundle(bundle)
	}
	raw, err := os.ReadFile(staticEdgePath(file))
	if err != nil {
		return err
	}
	key, err := c.ParsePrivateKeyPEM(raw)
	if err != nil {
		return err
	}
	return c.SignBundle(bundle, key, keyID)
}

func observeStaticEdgeAppManager(ctx context.Context, cfg staticEdgeContext, role string) (c.Observed, error) {
	request := c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: newStaticRequestID(), Operation: "status"}
	response, err := staticEdgeCall(ctx, cfg, request)
	if err != nil {
		return c.Observed{}, err
	}
	if response.Result == nil {
		return c.Observed{}, errors.New("manager status omitted observed state")
	}
	observed := *response.Result
	if observed.EdgeID != cfg.EdgeID || observed.Role != role {
		return c.Observed{}, fmt.Errorf("manager identity/role mismatch: want %s/%s", cfg.EdgeID, role)
	}
	if !observed.Ready || !observed.RuntimeMatches || observed.Draining || observed.ActiveGeneration == 0 || observed.ActiveDigest == "" {
		return c.Observed{}, errors.New("manager must be ready, runtime-matched, serving, and have an active bundle")
	}
	return observed, nil
}

func matchStaticEdgeAppBaseline(bundle c.Bundle, observed c.Observed) error {
	if bundle.Generation != observed.ActiveGeneration || bundle.BundleDigest != observed.ActiveDigest {
		return fmt.Errorf("local bundle is not the exact active generation (local=%d/%s remote=%d/%s)", bundle.Generation, bundle.BundleDigest, observed.ActiveGeneration, observed.ActiveDigest)
	}
	return nil
}

func staticEdgeAppRouteTarget(app model.App, opts staticEdgeAppAddOptions) (string, string, error) {
	hostname := strings.ToLower(strings.TrimSpace(opts.Hostname))
	pathPrefix := strings.TrimSpace(opts.PathPrefix)
	if hostname == "" {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			return "", "", errors.New("app has no primary hostname; provide --hostname")
		}
		hostname = strings.ToLower(strings.TrimSpace(app.Route.Hostname))
	}
	if !staticEdgeAppHostnamePattern.MatchString(hostname) || !strings.Contains(hostname, ".") {
		return "", "", errors.New("--hostname must be a valid ASCII DNS hostname without scheme, path, port, or wildcard")
	}
	if pathPrefix == "" {
		if opts.Hostname == "" && app.Route != nil {
			pathPrefix = app.Route.PathPrefix
		} else {
			pathPrefix = "/"
		}
	}
	if strings.ContainsAny(pathPrefix, "?# \t\r\n") || strings.Contains(pathPrefix, "\\") {
		return "", "", errors.New("--path-prefix must be a URL path prefix without query, fragment, whitespace, or backslash")
	}
	if strings.Contains(pathPrefix, "*") && !strings.HasSuffix(pathPrefix, "/*") {
		return "", "", errors.New("--path-prefix may only use * as the final /* prefix marker")
	}
	for _, segment := range strings.Split(pathPrefix, "/") {
		if segment == "." || segment == ".." {
			return "", "", errors.New("--path-prefix cannot contain dot path segments")
		}
	}
	pathPrefix = model.NormalizeAppRoutePathPrefix(pathPrefix)
	return hostname, pathPrefix, nil
}

func validateStaticEdgeAppUpstream(upstream string) error {
	host, portText, err := net.SplitHostPort(upstream)
	if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " \t\r\n/\\") {
		return errors.New("--upstream must be an origin-reachable host:port without scheme or path")
	}
	if net.ParseIP(host) == nil && !staticEdgeAppHostnamePattern.MatchString(host) {
		return errors.New("--upstream host must be a DNS name or IP address")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("--upstream has an invalid port")
	}
	return nil
}

func decodeStaticEdgeAppCaddyConfig(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var config map[string]any
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	if config == nil {
		return nil, errors.New("Caddy config must be a JSON object")
	}
	return config, nil
}

func staticEdgeAppHTTPServer(config map[string]any, selector string) (map[string]any, error) {
	apps, ok := config["apps"].(map[string]any)
	if !ok {
		return nil, errors.New("Caddy config has no apps object")
	}
	httpApp, ok := apps["http"].(map[string]any)
	if !ok {
		return nil, errors.New("Caddy config has no HTTP app")
	}
	servers, ok := httpApp["servers"].(map[string]any)
	if !ok || len(servers) == 0 {
		return nil, errors.New("Caddy HTTP app has no servers")
	}
	selector = strings.TrimSpace(selector)
	if selector != "" {
		server, ok := servers[selector].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Caddy HTTP server %q not found", selector)
		}
		return server, nil
	}
	if len(servers) != 1 {
		return nil, errors.New("Caddy config has multiple HTTP servers; specify --edge-server/--origin-server")
	}
	for _, value := range servers {
		server, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("Caddy HTTP server must be an object")
		}
		return server, nil
	}
	return nil, errors.New("Caddy HTTP server not found")
}

// staticEdgeAppHTTPServers selects every public HTTP server containing the
// template host when no explicit server was supplied. An edge bundle normally
// has separate TLS, HTTP redirect and HTTP/3 servers; attaching an app to only
// one of them would make the hostname protocol-dependent.
func staticEdgeAppHTTPServers(config map[string]any, selector, templateHost string) ([]map[string]any, error) {
	if strings.TrimSpace(selector) != "" {
		server, err := staticEdgeAppHTTPServer(config, selector)
		if err != nil {
			return nil, err
		}
		return []map[string]any{server}, nil
	}
	apps, ok := config["apps"].(map[string]any)
	if !ok {
		return nil, errors.New("Caddy config has no apps object")
	}
	httpApp, ok := apps["http"].(map[string]any)
	if !ok {
		return nil, errors.New("Caddy config has no HTTP app")
	}
	servers, ok := httpApp["servers"].(map[string]any)
	if !ok || len(servers) == 0 {
		return nil, errors.New("Caddy HTTP app has no servers")
	}
	var selected []map[string]any
	for _, value := range servers {
		server, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("Caddy HTTP server must be an object")
		}
		if staticEdgeAppServerHasHostname(server, templateHost) {
			selected = append(selected, server)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no edge HTTP server matched --edge-template-host %q", templateHost)
	}
	return selected, nil
}

func staticEdgeAppServerHasHostname(server map[string]any, hostname string) bool {
	routes, ok := server["routes"].([]any)
	if !ok {
		return false
	}
	for _, item := range routes {
		if routeHasStaticEdgeAppHostname(item, hostname) {
			return true
		}
	}
	return false
}

func staticEdgeAppServerHasForwardedHost(server map[string]any, hostname string) bool {
	routes, ok := server["routes"].([]any)
	if !ok {
		return false
	}
	for _, item := range routes {
		if routeHasStaticEdgeAppForwardedHost(item, hostname) {
			return true
		}
	}
	return false
}

func staticEdgeAppExistingEdgeRoutesMatch(server map[string]any, templateHost, hostname, pathPrefix string) (bool, error) {
	routes, ok := server["routes"].([]any)
	if !ok {
		return false, errors.New("edge HTTP server has no routes array")
	}
	expected := make([]string, 0)
	actual := make([]string, 0)
	for _, item := range routes {
		if routeHasStaticEdgeAppHostname(item, templateHost) {
			route, ok := item.(map[string]any)
			if !ok {
				return false, errors.New("edge template route is malformed")
			}
			matchers, ok := route["match"].([]any)
			if !ok || len(matchers) != 1 {
				return false, errors.New("edge template route has ambiguous matcher groups")
			}
			matcher, ok := matchers[0].(map[string]any)
			if !ok {
				return false, errors.New("edge template route matcher is malformed")
			}
			if _, hasPath := matcher["path"]; hasPath {
				return false, fmt.Errorf("edge template hostname %q appears in a path-specific route", templateHost)
			}
			clone, err := cloneStaticEdgeAppJSONMap(route)
			if err != nil {
				return false, err
			}
			cloneMatcher := clone["match"].([]any)[0].(map[string]any)
			cloneMatcher["host"] = []any{hostname}
			if pathPrefix == "/" {
				delete(cloneMatcher, "path")
			} else {
				cloneMatcher["path"] = staticEdgeAppPathPatterns(pathPrefix)
			}
			encoded, err := json.Marshal(clone)
			if err != nil {
				return false, err
			}
			expected = append(expected, string(encoded))
		}
		if routeHasStaticEdgeAppHostname(item, hostname) {
			encoded, err := json.Marshal(item)
			if err != nil {
				return false, err
			}
			actual = append(actual, string(encoded))
		}
	}
	if len(expected) == 0 || len(expected) != len(actual) {
		return false, nil
	}
	for index := range expected {
		if expected[index] != actual[index] {
			return false, nil
		}
	}
	return true, nil
}

func staticEdgeAppExistingOriginRouteMatches(server map[string]any, hostname, pathPrefix, upstream string) bool {
	routes, ok := server["routes"].([]any)
	if !ok {
		return false
	}
	expected := staticEdgeAppOriginRoute(hostname, pathPrefix, upstream)
	expectedRaw, err := json.Marshal(expected)
	if err != nil {
		return false
	}
	for _, item := range routes {
		if !routeHasStaticEdgeAppForwardedHost(item, hostname) {
			continue
		}
		actualRaw, marshalErr := json.Marshal(item)
		if marshalErr == nil && string(actualRaw) == string(expectedRaw) {
			return true
		}
	}
	return false
}

func staticEdgeAppAddEdgeRoute(config map[string]any, server map[string]any, templateHost, hostname, pathPrefix string) error {
	templateHost = strings.ToLower(strings.TrimSpace(templateHost))
	if !staticEdgeAppHostnamePattern.MatchString(templateHost) {
		return errors.New("--edge-template-host must be a valid ASCII DNS hostname")
	}
	if templateHost == hostname {
		return errors.New("target hostname already equals the edge route template")
	}
	routes, ok := server["routes"].([]any)
	if !ok {
		return errors.New("edge HTTP server has no routes array")
	}
	for _, item := range routes {
		if routeHasStaticEdgeAppHostname(item, hostname) {
			return fmt.Errorf("edge Caddy config already contains hostname %q; refusing to replace an existing route", hostname)
		}
	}
	templateIndex := -1
	var templateRoutes []any
	for index, item := range routes {
		if !routeHasStaticEdgeAppHostname(item, templateHost) {
			continue
		}
		route, ok := item.(map[string]any)
		if !ok {
			return errors.New("edge template route is malformed")
		}
		matchers, ok := route["match"].([]any)
		if !ok || len(matchers) != 1 {
			return errors.New("edge template host appears in a route with multiple matcher groups; choose a less ambiguous template")
		}
		matcher, ok := matchers[0].(map[string]any)
		if !ok {
			return errors.New("edge template route matcher is malformed")
		}
		for key := range matcher {
			if key != "host" && key != "path" {
				return errors.New("edge template route includes non-host/path matchers; refusing to clone an ambiguous route")
			}
		}
		if _, hasPath := matcher["path"]; hasPath {
			return fmt.Errorf("edge template hostname %q appears in a path-specific route; choose a host-only template", templateHost)
		}
		clone, err := cloneStaticEdgeAppJSONMap(route)
		if err != nil {
			return err
		}
		cloneMatcher := clone["match"].([]any)[0].(map[string]any)
		cloneMatcher["host"] = []any{hostname}
		if pathPrefix == "/" {
			delete(cloneMatcher, "path")
		} else {
			cloneMatcher["path"] = staticEdgeAppPathPatterns(pathPrefix)
		}
		if templateIndex < 0 {
			templateIndex = index
		}
		templateRoutes = append(templateRoutes, clone)
	}
	if templateIndex < 0 {
		return fmt.Errorf("no edge Caddy route matched --edge-template-host %q", templateHost)
	}
	// A public server can legitimately have several host-only routes for the
	// same hostname (for example, response headers followed by the proxy hop).
	// Clone the complete route set in order so the new host gets identical
	// behavior without weakening the ambiguity checks above.
	server["routes"] = append(routes[:templateIndex], append(templateRoutes, routes[templateIndex:]...)...)
	return nil
}

func staticEdgeAppAddTLSSubject(config map[string]any, templateHost, hostname string) {
	apps, ok := config["apps"].(map[string]any)
	if !ok {
		return
	}
	tlsApp, ok := apps["tls"].(map[string]any)
	if !ok {
		return
	}
	automation, ok := tlsApp["automation"].(map[string]any)
	if !ok {
		return
	}
	policies, ok := automation["policies"].([]any)
	if !ok {
		return
	}
	for _, policyValue := range policies {
		policy, ok := policyValue.(map[string]any)
		if !ok {
			continue
		}
		subjects, ok := policy["subjects"].([]any)
		if !ok || !staticEdgeAppStringSliceContains(subjects, templateHost) || staticEdgeAppStringSliceContains(subjects, hostname) {
			continue
		}
		policy["subjects"] = append(subjects, hostname)
	}
}

func staticEdgeAppAddOriginRoute(server map[string]any, hostname, pathPrefix, upstream string) error {
	routes, ok := server["routes"].([]any)
	if !ok && server["routes"] == nil {
		routes = []any{}
	} else if !ok {
		return errors.New("origin HTTP server has no routes array")
	}
	for _, item := range routes {
		if routeHasStaticEdgeAppForwardedHost(item, hostname) {
			return fmt.Errorf("origin Caddy config already contains forwarded hostname %q; refusing to replace an existing route", hostname)
		}
	}
	route := staticEdgeAppOriginRoute(hostname, pathPrefix, upstream)
	server["routes"] = append([]any{route}, routes...)
	return nil
}

func staticEdgeAppOriginRoute(hostname, pathPrefix, upstream string) map[string]any {
	matcher := map[string]any{"header": map[string]any{"X-Forwarded-Host": []any{hostname}}}
	if pathPrefix != "/" {
		matcher["path"] = staticEdgeAppPathPatterns(pathPrefix)
	}
	return map[string]any{
		"match": []any{matcher},
		"handle": []any{map[string]any{
			"handler":   "reverse_proxy",
			"upstreams": []any{map[string]any{"dial": upstream}},
			"headers":   map[string]any{"request": map[string]any{"set": map[string]any{"Host": []any{"{http.request.header.X-Forwarded-Host}"}}}},
		}},
		"terminal": true,
	}
}

func staticEdgeAppPathPatterns(pathPrefix string) []any {
	return []any{pathPrefix, strings.TrimSuffix(pathPrefix, "/") + "/*"}
}

func routeHasStaticEdgeAppHostname(value any, hostname string) bool {
	route, ok := value.(map[string]any)
	if !ok {
		return false
	}
	matchers, ok := route["match"].([]any)
	if !ok {
		return false
	}
	for _, matcherValue := range matchers {
		matcher, ok := matcherValue.(map[string]any)
		if !ok {
			continue
		}
		hosts, _ := matcher["host"].([]any)
		if staticEdgeAppStringSliceContains(hosts, hostname) {
			return true
		}
	}
	return false
}

func routeHasStaticEdgeAppForwardedHost(value any, hostname string) bool {
	route, ok := value.(map[string]any)
	if !ok {
		return false
	}
	matchers, ok := route["match"].([]any)
	if !ok {
		return false
	}
	for _, matcherValue := range matchers {
		matcher, ok := matcherValue.(map[string]any)
		if !ok {
			continue
		}
		headers, ok := matcher["header"].(map[string]any)
		forwardedHosts, _ := headers["X-Forwarded-Host"].([]any)
		if ok && staticEdgeAppStringSliceContains(forwardedHosts, hostname) {
			return true
		}
	}
	return false
}

func staticEdgeAppStringSliceContains(values []any, want string) bool {
	for _, value := range values {
		if text, ok := value.(string); ok && strings.EqualFold(text, want) {
			return true
		}
	}
	return false
}

func cloneStaticEdgeAppJSONMap(in map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var out map[string]any
	if err = decoder.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func validateStaticEdgeAppOutputPaths(outDir, edgeSource, originSource string) error {
	if strings.TrimSpace(outDir) == "" {
		return errors.New("--out-dir is required")
	}
	absOut, err := filepath.Abs(staticEdgePath(outDir))
	if err != nil {
		return err
	}
	for _, name := range []string{"edge.bundle.json", "origin.bundle.json", "operation.json"} {
		if _, statErr := os.Lstat(filepath.Join(absOut, name)); !os.IsNotExist(statErr) {
			if statErr == nil {
				return fmt.Errorf("refusing to overwrite existing output %s", filepath.Join(absOut, name))
			}
			return statErr
		}
	}
	for _, source := range []string{edgeSource, originSource} {
		absSource, absErr := filepath.Abs(staticEdgePath(source))
		if absErr != nil {
			return absErr
		}
		if filepath.Dir(absSource) == absOut && (filepath.Base(absSource) == "edge.bundle.json" || filepath.Base(absSource) == "origin.bundle.json") {
			return errors.New("output bundle paths must not overlap the source baseline files")
		}
	}
	return nil
}

func writeStaticEdgeAppBundle(path string, bundle c.Bundle) error {
	raw, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	return createStaticEdgeAppFile(path, append(raw, '\n'))
}

func createStaticEdgeAppFile(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = file.Write(raw); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

func writeStaticEdgeAppResult(result staticEdgeAppAddResult, create bool) error {
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if create {
		return createStaticEdgeAppFile(result.OperationFile, append(raw, '\n'))
	}
	return staticEdgeWriteFile(result.OperationFile, append(raw, '\n'))
}

// Stage both configurations before changing either serving process. Each manager
// still owns its independent revision, journal, validation and rollback; there is
// deliberately no cross-manager automatic retry or compensating mutation.
func (cli *CLI) executeStaticEdgeApp(ctx context.Context, result *staticEdgeAppAddResult, edgeContext, originContext staticEdgeContext, edgeBundle, originBundle c.Bundle, edgeRevision, originRevision uint64) error {
	steps := []struct {
		manager  staticEdgeContext
		bundle   c.Bundle
		revision *uint64
	}{
		{originContext, originBundle, &originRevision},
		{edgeContext, edgeBundle, &edgeRevision},
		{originContext, originBundle, &originRevision},
		{edgeContext, edgeBundle, &edgeRevision},
	}
	for index, step := range steps {
		operation := &result.Operations[index]
		result.Phase = step.manager.EdgeID + "_" + operation.Operation + "_pending"
		operation.State = "pending"
		if err := writeStaticEdgeAppResult(*result, false); err != nil {
			return fmt.Errorf("persist operation state before %s: %w", operation.RequestID, err)
		}
		fmt.Fprintf(cli.stderr, "static-edge request_id=%s target=%s transport=%s\n", operation.RequestID, step.manager.EdgeID, step.manager.Transport)
		receipt, err := callStaticEdgeAppOperation(ctx, step.manager, step.bundle, *step.revision, operation.RequestID, operation.Operation)
		if receipt != nil {
			operation.Receipt = receipt
			operation.Revision = receipt.Revision
			*step.revision = receipt.Revision
		}
		result.EdgeRevision, result.OriginRevision = edgeRevision, originRevision
		if err != nil {
			// Retain the request ID and receipt, if available, so operators can
			// query the manager's independent journal before retrying.
			operation.State = "error_or_unknown"
			result.Phase = step.manager.EdgeID + "_" + operation.Operation + "_error_or_unknown"
			persistErr := writeStaticEdgeAppResult(*result, false)
			renderErr := cli.renderResourceResult(*result)
			return errors.Join(fmt.Errorf("%s failed; inspect %s and manager operation %s before retrying: %w", operation.Operation, result.OperationFile, operation.RequestID, err), persistErr, renderErr)
		}
		operation.State = "succeeded"
		result.Phase = step.manager.EdgeID + "_" + operation.Operation + "_succeeded"
		if err := writeStaticEdgeAppResult(*result, false); err != nil {
			return fmt.Errorf("operation %s succeeded but its receipt could not be saved; query manager operation before retrying: %w", operation.RequestID, err)
		}
	}
	result.Phase = "applied"
	result.Executed = true
	if err := writeStaticEdgeAppResult(*result, false); err != nil {
		return fmt.Errorf("both configurations applied, but saving completion failed; inspect manager operation receipts: %w", err)
	}
	return cli.renderResourceResult(*result)
}

func callStaticEdgeAppOperation(ctx context.Context, manager staticEdgeContext, bundle c.Bundle, expectedRevision uint64, requestID, operation string) (*c.Receipt, error) {
	if bundle.EdgeID != manager.EdgeID || bundle.Signature == "" {
		return nil, errors.New("signed bundle and manager context do not match")
	}
	request := c.Request{
		Schema: c.RPCSchema, EdgeID: manager.EdgeID, RequestID: requestID, Operation: operation,
		ExpectedRevision: &expectedRevision,
	}
	if operation == "stage" {
		request.Bundle = &bundle
	} else {
		request.TargetDigest = bundle.BundleDigest
	}
	response, err := staticEdgeCall(ctx, manager, request)
	if err != nil {
		return response.Receipt, err
	}
	if response.Receipt == nil || response.Receipt.RequestID != requestID || response.Receipt.Operation != operation || response.Receipt.BundleDigest != bundle.BundleDigest {
		return response.Receipt, errors.New("manager returned no matching durable receipt")
	}
	if operation == "stage" && response.Receipt.Outcome != "staged" {
		return response.Receipt, errors.New("manager did not stage the candidate")
	}
	if operation == "activate" && (response.Receipt.Outcome != "activated" || !response.Receipt.RuntimeVerified) {
		return response.Receipt, errors.New("activation did not return a runtime-verified receipt")
	}
	return response.Receipt, nil
}
