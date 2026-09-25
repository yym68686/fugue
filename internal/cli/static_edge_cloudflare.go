package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	c "fugue/internal/staticedgecontract"
	"github.com/spf13/cobra"
)

var staticEdgeDNSName = regexp.MustCompile("^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$")
var staticEdgeCutoverOperation = regexp.MustCompile("^fse_[0-9a-f]{32}$")

type staticEdgeCutoverOptions struct {
	Zone      string        `json:"zone"`
	ZoneID    string        `json:"zone_id"`
	Hostnames []string      `json:"hostnames"`
	FromIP    string        `json:"from_ip"`
	ToIP      string        `json:"to_ip"`
	Candidate string        `json:"candidate"`
	ProbePath string        `json:"probe_path"`
	Checks    []string      `json:"checks"`
	Observe   time.Duration `json:"observe_ns"`
	Timeout   time.Duration `json:"timeout_ns"`
	Execute   bool          `json:"execute"`
	Operation string        `json:"operation,omitempty"`
}
type staticEdgeDNSRecord map[string]json.RawMessage

func (r staticEdgeDNSRecord) str(k string) string {
	var v string
	_ = json.Unmarshal(r[k], &v)
	return v
}
func (r staticEdgeDNSRecord) clone() staticEdgeDNSRecord {
	b, _ := json.Marshal(r)
	var v staticEdgeDNSRecord
	_ = json.Unmarshal(b, &v)
	return v
}
func (r staticEdgeDNSRecord) withIP(ip string) staticEdgeDNSRecord {
	v := r.clone()
	v["content"], _ = json.Marshal(ip)
	return v
}
func staticEdgeRecordEqual(a, b staticEdgeDNSRecord) bool {
	stable := func(r staticEdgeDNSRecord) []byte {
		r = r.clone()
		for _, k := range []string{"created_on", "modified_on", "comment_modified_on", "tags_modified_on", "meta", "proxiable", "zone_id", "zone_name"} {
			delete(r, k)
		}
		b, _ := json.Marshal(r)
		return b
	}
	return bytes.Equal(stable(a), stable(b))
}

type staticEdgeCutoverJournal struct {
	Schema     int
	Operation  string
	Options    staticEdgeCutoverOptions
	Records    map[string]staticEdgeDNSRecord
	Phase      string
	Error      string
	StartedAt  time.Time
	UpdatedAt  time.Time
	SwitchedAt time.Time
}

func staticEdgeConfigDir() string {
	if p := os.Getenv("FUGUE_STATIC_EDGE_STATE_DIR"); p != "" {
		return p
	}
	d, e := os.UserConfigDir()
	if e != nil {
		d = ".config"
	}
	return filepath.Join(d, "fugue", "static-edge")
}
func staticEdgeCloudflareTokenPath(zone string) string {
	if p := os.Getenv("FUGUE_STATIC_EDGE_CLOUDFLARE_TOKEN_FILE"); p != "" {
		return p
	}
	return filepath.Join(staticEdgeConfigDir(), "cloudflare", zone+".token")
}
func validateStaticEdgeZone(z string) error {
	if len(z) > 253 || !staticEdgeDNSName.MatchString(z) {
		return errors.New("valid DNS zone name required")
	}
	return nil
}
func (cli *CLI) newStaticEdgeCloudflareCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "cloudflare", Short: "Independent Cloudflare DNS credentials; no Fugue API"}
	auth := &cobra.Command{Use: "auth", Short: "Import or inspect a private zone DNS credential"}
	var zone string
	var stdin bool
	imp := &cobra.Command{Use: "import", Args: cobra.NoArgs, Short: "Store a zone DNS token from stdin without printing it", RunE: func(cmd *cobra.Command, _ []string) error {
		z := normalizeDNSName(zone)
		if e := validateStaticEdgeZone(z); e != nil {
			return e
		}
		if !stdin {
			return errors.New("--token-stdin required")
		}
		b, e := io.ReadAll(io.LimitReader(cmd.InOrStdin(), 514))
		if e != nil {
			return e
		}
		t := strings.TrimSpace(string(b))
		if len(b) > 513 || t == "" || len(t) > 512 || strings.ContainsAny(t, "\r\n \t") {
			return errors.New("invalid token input")
		}
		p := staticEdgeCloudflareTokenPath(z)
		if e = staticEdgeWriteFile(p, []byte(t+"\n")); e != nil {
			return e
		}
		return cli.writeJSON(map[string]any{"stored": true, "zone": z, "path": p, "mode": "0600"})
	}}
	imp.Flags().StringVar(&zone, "zone", "", "Exact Cloudflare zone")
	_ = imp.MarkFlagRequired("zone")
	imp.Flags().BoolVar(&stdin, "token-stdin", false, "Read token from stdin")
	var showZone string
	show := &cobra.Command{Use: "show", Args: cobra.NoArgs, Short: "Show credential metadata only", RunE: func(*cobra.Command, []string) error {
		z := normalizeDNSName(showZone)
		if e := validateStaticEdgeZone(z); e != nil {
			return e
		}
		p := staticEdgeCloudflareTokenPath(z)
		st, e := os.Stat(p)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		out := map[string]any{"zone": z, "path": p, "present": e == nil}
		if e == nil {
			out["mode"] = fmt.Sprintf("%04o", st.Mode().Perm())
		}
		return cli.writeJSON(out)
	}}
	show.Flags().StringVar(&showZone, "zone", "", "Exact Cloudflare zone")
	_ = show.MarkFlagRequired("zone")
	auth.AddCommand(imp, show)
	cmd.AddCommand(auth)
	return cmd
}
func resolveStaticEdgeCloudflareToken(zone string) (string, error) {
	if t := strings.TrimSpace(os.Getenv("FUGUE_STATIC_EDGE_CLOUDFLARE_TOKEN")); t != "" {
		return t, nil
	}
	p := staticEdgeCloudflareTokenPath(zone)
	st, e := os.Stat(p)
	if e != nil {
		return "", fmt.Errorf("import a zone credential using static-edge cloudflare auth import: %w", e)
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return "", errors.New("token must be a private regular file (0600)")
	}
	b, e := os.ReadFile(p)
	if e != nil {
		return "", e
	}
	t := strings.TrimSpace(string(b))
	if t == "" {
		return "", errors.New("empty token")
	}
	return t, nil
}
func newStaticEdgeCloudflareClient(zone string, timeout time.Duration) (*cloudflareDNSClient, error) {
	token, e := resolveStaticEdgeCloudflareToken(zone)
	if e != nil {
		return nil, e
	}
	base := strings.TrimRight(os.Getenv("FUGUE_STATIC_EDGE_CLOUDFLARE_API_URL"), "/")
	if base == "" {
		base = defaultCloudflareAPIBaseURL
	}
	u, e := url.Parse(base)
	if e != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid Cloudflare API URL")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback()) {
		return nil, errors.New("Cloudflare API requires HTTPS")
	}
	return &cloudflareDNSClient{baseURL: base, token: token, http: &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (cli *CLI) newStaticEdgeCutoverCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "cutover", Short: "Recoverable DNS switch while both autonomous edges keep serving"}
	for _, op := range []string{"plan", "run", "status", "rollback"} {
		op := op
		o := staticEdgeCutoverOptions{Timeout: 30 * time.Second, ProbePath: "/_static-edge/health", Observe: 30 * time.Second}
		x := &cobra.Command{Use: op, Short: op + " an independent, recoverable DNS cutover", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
			if op == "status" || op == "rollback" {
				return cli.staticEdgeExistingCutover(cmd.Context(), o, op)
			}
			if op == "plan" {
				o.Execute = false
			}
			return cli.runStaticEdgeCutover(cmd.Context(), o)
		}}
		f := x.Flags()
		f.StringVar(&o.Zone, "zone", "", "Exact DNS zone")
		f.StringVar(&o.ZoneID, "zone-id", "", "Zone ID; avoids Zone Read permission")
		f.StringSliceVar(&o.Hostnames, "hostname", nil, "Exact managed hostname; repeat as needed")
		f.StringVar(&o.FromIP, "from-ip", "", "Expected old IPv4")
		f.StringVar(&o.ToIP, "to-ip", "", "Candidate IPv4")
		f.StringVar(&o.Candidate, "candidate", "", "Independent candidate manager context")
		f.StringVar(&o.ProbePath, "probe-path", o.ProbePath, "Non-billable GET path expected to return HTTP 200")
		f.StringSliceVar(&o.Checks, "check", nil, "Additional non-billable check hostname/path=status; repeat as needed")
		f.DurationVar(&o.Observe, "observe", o.Observe, "Overlap observation duration (5s-10m)")
		f.DurationVar(&o.Timeout, "timeout", o.Timeout, "Per-request timeout")
		f.StringVar(&o.Operation, "operation", "", "Operation ID for status/rollback")
		f.BoolVar(&o.Execute, "execute", false, "Apply the validated intent")
		cmd.AddCommand(x)
	}
	return cmd
}
func normalizeStaticEdgeCutover(o staticEdgeCutoverOptions) (staticEdgeCutoverOptions, error) {
	o.Zone = normalizeDNSName(o.Zone)
	if e := validateStaticEdgeZone(o.Zone); e != nil {
		return o, e
	}
	if len(o.Hostnames) == 0 || len(o.Hostnames) > 16 {
		return o, errors.New("1-16 exact hostnames required")
	}
	o.Hostnames = append([]string(nil), o.Hostnames...)
	seen := map[string]bool{}
	for i, h := range o.Hostnames {
		h = normalizeDNSName(h)
		if e := validateStaticEdgeZone(h); e != nil {
			return o, e
		}
		if seen[h] || (h != o.Zone && !strings.HasSuffix(h, "."+o.Zone)) {
			return o, errors.New("unique hostnames must belong to the exact zone")
		}
		seen[h] = true
		o.Hostnames[i] = h
	}
	sort.Strings(o.Hostnames)
	if len(o.Checks) > 32 {
		return o, errors.New("at most 32 extra checks allowed")
	}
	for _, spec := range o.Checks {
		host, _, _, e := parseStaticEdgeCheck(spec)
		if e != nil {
			return o, e
		}
		if !seen[host] {
			return o, errors.New("extra check hostname must be in the exact cutover allowlist")
		}
	}
	for _, s := range []string{o.FromIP, o.ToIP} {
		ip := net.ParseIP(s)
		if ip == nil || ip.To4() == nil || strings.Contains(s, ":") {
			return o, errors.New("old and new targets must be IPv4")
		}
	}
	if o.FromIP == o.ToIP {
		return o, errors.New("old and new targets must differ")
	}
	if !c.ValidID(o.Candidate) {
		return o, errors.New("candidate manager context required")
	}
	if !strings.HasPrefix(o.ProbePath, "/") || strings.ContainsAny(o.ProbePath, "?#\r\n") {
		return o, errors.New("probe path must have no query or fragment")
	}
	if o.Timeout <= 0 || o.Timeout > 5*time.Minute {
		return o, errors.New("timeout must be positive and at most 5m")
	}
	if o.Observe < 5*time.Second || o.Observe > 10*time.Minute {
		return o, errors.New("observe must be 5s-10m")
	}
	if o.Operation != "" {
		return o, errors.New("run derives its operation ID; rerun the same command to resume")
	}
	return o, nil
}
func staticEdgeCutoverID(o staticEdgeCutoverOptions) string {
	o.Execute = false
	o.Operation = ""
	o.Timeout = 0
	raw, _ := json.Marshal(o)
	h := sha256.Sum256(raw)
	return "fse_" + hex.EncodeToString(h[:16])
}
func staticEdgeCutoverJournalPath(op string) string {
	return filepath.Join(staticEdgeConfigDir(), "cutovers", op+".json")
}
func saveStaticEdgeCutoverJournal(j *staticEdgeCutoverJournal) error {
	j.UpdatedAt = time.Now().UTC()
	b, e := json.MarshalIndent(j, "", "  ")
	if e != nil {
		return e
	}
	return staticEdgeWriteFile(staticEdgeCutoverJournalPath(j.Operation), append(b, '\n'))
}
func readStaticEdgeCutoverJournal(op string) (staticEdgeCutoverJournal, error) {
	var j staticEdgeCutoverJournal
	if !staticEdgeCutoverOperation.MatchString(op) {
		return j, errors.New("valid --operation required")
	}
	b, e := os.ReadFile(staticEdgeCutoverJournalPath(op))
	if e != nil {
		return j, e
	}
	if e = c.StrictJSON(b, &j); e != nil {
		return j, e
	}
	if _, e = normalizeStaticEdgeCutover(j.Options); e != nil {
		return j, e
	}
	if j.Schema != 1 || j.Operation != op || staticEdgeCutoverID(j.Options) != op {
		return j, errors.New("invalid journal identity")
	}
	for _, h := range j.Options.Hostnames {
		r := j.Records[h]
		if r == nil || r.str("id") == "" || r.str("name") != h || r.str("type") != "A" || r.str("content") != j.Options.FromIP {
			return j, errors.New("incomplete DNS snapshot")
		}
	}
	return j, nil
}
func (x *cloudflareDNSClient) staticEdgeA(ctx context.Context, zid, host string) (staticEdgeDNSRecord, error) {
	q := url.Values{"name.exact": []string{host}, "per_page": []string{"100"}}
	var rs []staticEdgeDNSRecord
	if e := x.do(ctx, http.MethodGet, "/zones/"+url.PathEscape(zid)+"/dns_records?"+q.Encode(), nil, &rs); e != nil {
		return nil, e
	}
	if len(rs) >= 100 {
		return nil, errors.New("record count exceeds bounded inspection")
	}
	var found staticEdgeDNSRecord
	for _, r := range rs {
		if r.str("name") != host {
			return nil, errors.New("non-exact DNS result")
		}
		switch r.str("type") {
		case "AAAA", "CNAME", "HTTPS", "SVCB":
			return nil, fmt.Errorf("%s has %s; refusing partial routing cutover", host, r.str("type"))
		case "A":
			if found != nil {
				return nil, fmt.Errorf("%s has multiple A records", host)
			}
			found = r
		}
	}
	if found == nil || found.str("id") == "" {
		return nil, fmt.Errorf("%s must have exactly one A record", host)
	}
	var proxied bool
	if e := json.Unmarshal(found["proxied"], &proxied); e != nil || proxied {
		return nil, errors.New("explicit DNS-only record required")
	}
	return found, nil
}
func (x *cloudflareDNSClient) staticEdgePatch(ctx context.Context, zid string, old staticEdgeDNSRecord, ip string) error {
	var got staticEdgeDNSRecord
	e := x.do(ctx, http.MethodPatch, "/zones/"+url.PathEscape(zid)+"/dns_records/"+url.PathEscape(old.str("id")), map[string]string{"content": ip}, &got)
	if e == nil && !staticEdgeRecordEqual(got, old.withIP(ip)) {
		return errors.New("PATCH response differs from expected record")
	}
	return e
}
func (cli *CLI) runStaticEdgeCutover(ctx context.Context, o staticEdgeCutoverOptions) error {
	return cli.runStaticEdgeCutoverWithChecks(ctx, o, verifyStaticEdgeCandidate, probeStaticEdgeBoth, observeStaticEdgeOverlap)
}
func (cli *CLI) runStaticEdgeCutoverWithChecks(ctx context.Context, o staticEdgeCutoverOptions, verify func(context.Context, staticEdgeCutoverOptions) error, probe func(context.Context, staticEdgeCutoverOptions) error, observe func(context.Context, staticEdgeCutoverOptions) error) error {
	o, e := normalizeStaticEdgeCutover(o)
	if e != nil {
		return e
	}
	unlock, e := acquireStaticEdgeCutoverLock(o.Zone)
	if e != nil {
		return e
	}
	defer unlock()
	client, e := newStaticEdgeCloudflareClient(o.Zone, o.Timeout)
	if e != nil {
		return e
	}
	client.zoneID = o.ZoneID
	zid, e := client.ensureZoneID(ctx, o.Zone)
	if e != nil {
		return e
	}
	id := staticEdgeCutoverID(o)
	j, e := readStaticEdgeCutoverJournal(id)
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if os.IsNotExist(e) {
		j = staticEdgeCutoverJournal{Schema: 1, Operation: id, Options: o, Records: map[string]staticEdgeDNSRecord{}, Phase: "planned", StartedAt: time.Now().UTC()}
		for _, h := range o.Hostnames {
			r, e := client.staticEdgeA(ctx, zid, h)
			if e != nil {
				return e
			}
			if r.str("content") != o.FromIP {
				return fmt.Errorf("%s differs from expected old IP; no owned journal permits adoption", h)
			}
			j.Records[h] = r
		}
	}
	if j.Phase == "rolled_back" || j.Phase == "rollback_pending" {
		return errors.New("intent was rolled back or rollback pending; inspect it before a new cutover")
	}
	if e = verify(ctx, o); e != nil {
		return e
	}
	if e = probe(ctx, o); e != nil {
		return e
	}
	if !o.Execute {
		return cli.writeJSON(map[string]any{"dry_run": true, "operation": id, "options": o, "records": j.Records, "candidate_ready": true})
	}
	if e = saveStaticEdgeCutoverJournal(&j); e != nil {
		return e
	}
	// Validate the entire record set before the first write, then recheck each
	// individual record immediately before its PATCH. This is not server CAS.
	for _, h := range o.Hostnames {
		r, e := client.staticEdgeA(ctx, zid, h)
		if e != nil {
			return e
		}
		if !staticEdgeRecordEqual(r, j.Records[h]) && !staticEdgeRecordEqual(r, j.Records[h].withIP(o.ToIP)) {
			return fmt.Errorf("record drift for %s before any DNS write", h)
		}
	}
	fail := func(cause error) error {
		j.Error = cause.Error()
		if e := saveStaticEdgeCutoverJournal(&j); e != nil {
			return fmt.Errorf("%v; journal error: %w", cause, e)
		}
		return fmt.Errorf("cutover %s phase=%s; both servers retained; rerun this command after resolving: %w", id, j.Phase, cause)
	}
	for _, h := range o.Hostnames {
		original := j.Records[h]
		current, e := client.staticEdgeA(ctx, zid, h)
		if e != nil {
			return fail(e)
		}
		if staticEdgeRecordEqual(current, original.withIP(o.ToIP)) {
			continue
		}
		if !staticEdgeRecordEqual(current, original) {
			return fail(fmt.Errorf("record drift for %s; refusing overwrite", h))
		}
		j.Phase = "dns_write_pending"
		j.Error = ""
		if e = saveStaticEdgeCutoverJournal(&j); e != nil {
			return e
		}
		writeErr := client.staticEdgePatch(ctx, zid, current, o.ToIP)
		actual, readErr := client.staticEdgeA(ctx, zid, h)
		if readErr != nil {
			j.Phase = "dns_outcome_unknown"
			return fail(fmt.Errorf("write=%v; readback=%w", writeErr, readErr))
		}
		if !staticEdgeRecordEqual(actual, original.withIP(o.ToIP)) {
			return fail(fmt.Errorf("record %s did not reach target: write=%v", h, writeErr))
		}
	}
	if j.SwitchedAt.IsZero() {
		j.SwitchedAt = time.Now().UTC()
	}
	j.Phase = "overlap_observing"
	j.Error = ""
	if e = saveStaticEdgeCutoverJournal(&j); e != nil {
		return e
	}
	if e = observe(ctx, o); e != nil {
		return fail(e)
	}
	if e = verify(ctx, o); e != nil {
		return fail(e)
	}
	for _, h := range o.Hostnames {
		r, e := client.staticEdgeA(ctx, zid, h)
		if e != nil {
			return fail(e)
		}
		if !staticEdgeRecordEqual(r, j.Records[h].withIP(o.ToIP)) {
			return fail(fmt.Errorf("final DNS drift for %s", h))
		}
	}
	j.Phase = "completed_old_retained"
	j.Error = ""
	if e = saveStaticEdgeCutoverJournal(&j); e != nil {
		return e
	}
	return cli.writeJSON(map[string]any{"operation": id, "phase": j.Phase, "hostnames": o.Hostnames, "authoritative_target": o.ToIP, "dns_api_verified": true, "both_endpoints_healthy": true, "old_server_must_remain_serving": true, "unmanaged_records_not_written": true})
}
func observeStaticEdgeOverlap(ctx context.Context, o staticEdgeCutoverOptions) error {
	deadline := time.Now().Add(o.Observe)
	for {
		if e := probeStaticEdgeBoth(ctx, o); e != nil {
			return e
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		timer := time.NewTimer(min(5*time.Second, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func verifyStaticEdgeCandidate(ctx context.Context, o staticEdgeCutoverOptions) error {
	cfg, e := loadStaticEdgeContext(o.Candidate)
	if e != nil {
		return e
	}
	if cfg.Transport != "mtls" {
		return errors.New("candidate requires direct mTLS management")
	}
	u, e := url.Parse(cfg.ManagerURL)
	if e != nil || u.Hostname() != o.ToIP {
		return errors.New("candidate manager IP must equal target IP")
	}
	out, e := staticEdgeCall(ctx, cfg, c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: newStaticRequestID(), Operation: "health"})
	if e != nil {
		return e
	}
	if out.Result == nil || !out.Result.Ready || !out.Result.RuntimeMatches || out.Result.Draining || out.Result.PendingRequestID != "" || out.Result.ActiveDigest == "" {
		return errors.New("candidate has no healthy verified active configuration")
	}
	return nil
}
func probeStaticEdgeBoth(ctx context.Context, o staticEdgeCutoverOptions) error {
	for _, ip := range []string{o.FromIP, o.ToIP} {
		for _, h := range o.Hostnames {
			if e := probeStaticEdgeEndpoint(ctx, ip, h, 443, o.ProbePath, o.Timeout); e != nil {
				return e
			}
		}
		for _, spec := range o.Checks {
			host, path, status, e := parseStaticEdgeCheck(spec)
			if e != nil {
				return e
			}
			if e = probeStaticEdgeEndpointStatus(ctx, ip, host, 443, path, o.Timeout, status); e != nil {
				return e
			}
		}
	}
	return nil
}
func parseStaticEdgeCheck(spec string) (string, string, int, error) {
	left, statusText, ok := strings.Cut(spec, "=")
	if !ok {
		return "", "", 0, errors.New("check must be hostname/path=status")
	}
	slash := strings.Index(left, "/")
	if slash < 1 {
		return "", "", 0, errors.New("check requires a hostname and absolute path")
	}
	host, path := left[:slash], left[slash:]
	if e := validateStaticEdgeZone(host); e != nil {
		return "", "", 0, e
	}
	var status int
	if _, e := fmt.Sscanf(statusText, "%d", &status); e != nil || fmt.Sprint(status) != statusText || status < 200 || status > 499 || strings.ContainsAny(path, "?#\r\n") {
		return "", "", 0, errors.New("invalid expected status or check path")
	}
	return host, path, status, nil
}
func probeStaticEdgeEndpoint(ctx context.Context, ip, hostname string, port int, path string, timeout time.Duration) error {
	return probeStaticEdgeEndpointStatus(ctx, ip, hostname, port, path, timeout, 200)
}
func probeStaticEdgeEndpointStatus(ctx context.Context, ip, hostname string, port int, path string, timeout time.Duration, expected int) error {
	tr := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, net.JoinHostPort(ip, fmt.Sprint(port)))
	}, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: hostname}, TLSHandshakeTimeout: timeout}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+net.JoinHostPort(hostname, fmt.Sprint(port))+path, nil)
	if e != nil {
		return e
	}
	req.Host = hostname
	resp, e := client.Do(req)
	if e != nil {
		return fmt.Errorf("direct TLS probe %s via %s: %w", hostname, ip, e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != expected {
		return fmt.Errorf("direct probe %s%s via %s: HTTP %d (expected %d)", hostname, path, ip, resp.StatusCode, expected)
	}
	_, e = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return e
}
func (cli *CLI) staticEdgeExistingCutover(ctx context.Context, o staticEdgeCutoverOptions, op string) error {
	j, e := readStaticEdgeCutoverJournal(o.Operation)
	if e != nil {
		return e
	}
	unlock, e := acquireStaticEdgeCutoverLock(j.Options.Zone)
	if e != nil {
		return e
	}
	defer unlock()
	client, e := newStaticEdgeCloudflareClient(j.Options.Zone, o.Timeout)
	if e != nil {
		return e
	}
	client.zoneID = j.Options.ZoneID
	zid, e := client.ensureZoneID(ctx, j.Options.Zone)
	if e != nil {
		return e
	}
	current := map[string]staticEdgeDNSRecord{}
	for _, h := range j.Options.Hostnames {
		current[h], e = client.staticEdgeA(ctx, zid, h)
		if e != nil {
			return e
		}
	}
	if op == "status" || !o.Execute {
		return cli.writeJSON(map[string]any{"journal": j, "current_records": current, "dry_run": op == "rollback"})
	}
	probes := j.Options
	probes.Timeout = o.Timeout
	if e = probeStaticEdgeBoth(ctx, probes); e != nil {
		return e
	}
	for _, h := range j.Options.Hostnames {
		if !staticEdgeRecordEqual(current[h], j.Records[h]) && !staticEdgeRecordEqual(current[h], j.Records[h].withIP(j.Options.ToIP)) {
			return fmt.Errorf("drift for %s; refusing rollback", h)
		}
	}
	j.Phase = "rollback_pending"
	if e = saveStaticEdgeCutoverJournal(&j); e != nil {
		return e
	}
	for _, h := range j.Options.Hostnames {
		r, e := client.staticEdgeA(ctx, zid, h)
		if e != nil {
			return e
		}
		if staticEdgeRecordEqual(r, j.Records[h]) {
			continue
		}
		if !staticEdgeRecordEqual(r, j.Records[h].withIP(j.Options.ToIP)) {
			return fmt.Errorf("drift before rollback for %s", h)
		}
		writeErr := client.staticEdgePatch(ctx, zid, r, j.Options.FromIP)
		r, e = client.staticEdgeA(ctx, zid, h)
		if e != nil {
			return e
		}
		if !staticEdgeRecordEqual(r, j.Records[h]) {
			return fmt.Errorf("rollback verification failed for %s: %v", h, writeErr)
		}
	}
	j.Phase = "rolled_back"
	j.Error = ""
	if e = saveStaticEdgeCutoverJournal(&j); e != nil {
		return e
	}
	return cli.writeJSON(map[string]any{"operation": j.Operation, "phase": j.Phase, "both_servers_retained": true})
}
