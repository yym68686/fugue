package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"

	"github.com/spf13/cobra"
)

type dnsAvailabilityOptions struct {
	Edges                 []string
	Successes, MinSeconds int
	Wait, DryRun          bool
	Timeout               time.Duration
	ConnectAddress        string
}

type dnsAvailabilityResult struct {
	Hostname                  string                                `json:"hostname"`
	Policy                    model.DNSAvailabilityPolicy           `json:"policy"`
	Phase                     string                                `json:"phase"`
	DryRun                    bool                                  `json:"dry_run"`
	ProducerActivated         bool                                  `json:"producer_activated"`
	RoutingAcceptanceComplete bool                                  `json:"routing_acceptance_complete"`
	Authority                 model.PlatformPublicationPrecondition `json:"authority"`
	DNSPolicyArtifactID       string                                `json:"dns_policy_artifact_id"`
	CandidateArtifactID       string                                `json:"candidate_artifact_id,omitempty"`
	ServingState              *artifactStateView                    `json:"serving_state,omitempty"`
}

func (c *CLI) newAdminDNSAvailabilityCommand() *cobra.Command {
	group := &cobra.Command{Use: "availability", Short: "Configure independently authorized per-host DNS failover"}
	opts := dnsAvailabilityOptions{}
	set := &cobra.Command{Use: "set <hostname>", Short: "Preserve all other constraints, configure one host, and wait for verified production", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(args[0])), ".")
		if !validAvailabilityHostname(host) {
			return fmt.Errorf("an exact non-wildcard DNS hostname is required")
		}
		policy := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: opts.Edges, RecoverySuccesses: opts.Successes, RecoveryMinSeconds: opts.MinSeconds}
		if err := model.ValidateDNSAvailabilityPolicy(&policy); err != nil {
			return err
		}
		if opts.Timeout <= 0 {
			return fmt.Errorf("--timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), opts.Timeout)
		defer cancel()
		client.context = ctx
		if err = configureAvailabilityTransport(client, opts.ConnectAddress); err != nil {
			return err
		}
		result, err := c.setDNSAvailability(ctx, client, host, policy, opts)
		if renderErr := c.renderResourceResult(result); renderErr != nil {
			return renderErr
		}
		return err
	}}
	set.Flags().StringArrayVar(&opts.Edges, "edge", nil, "Authorized edge ID, repeat in preferred fallback order (required)")
	set.Flags().IntVar(&opts.Successes, "recovery-successes", 3, "Distinct successful observations required (2-5)")
	set.Flags().IntVar(&opts.MinSeconds, "recovery-min-seconds", 60, "Minimum observation span (10-300 seconds)")
	set.Flags().BoolVar(&opts.Wait, "wait", true, "Wait for actual full consumer convergence and positive LKG; configuration success alone is not serving success")
	set.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Read-only baseline/declaration check; does not create drafts or validate a successor runtime")
	set.Flags().DurationVar(&opts.Timeout, "timeout", 30*time.Minute, "Total configuration and serving verification deadline")
	set.Flags().StringVar(&opts.ConnectAddress, "api-connect-address", "", "Explicit API IP (no proxy); preserves original HTTPS Host, SNI and certificate validation")
	_ = set.MarkFlagRequired("edge")
	group.AddCommand(set)
	return group
}

func validAvailabilityHostname(host string) bool {
	if len(host) == 0 || len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range label {
			if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-') {
				return false
			}
		}
	}
	return true
}

// This is an explicit transport override, not an alternate API origin or a TLS
// bypass. Never send privileged credentials through an ambient proxy/redirect.
func configureAvailabilityTransport(client *Client, address string) error {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if address != "" {
		if net.ParseIP(address) == nil {
			return fmt.Errorf("--api-connect-address must be an IP address")
		}
		origin, err := url.Parse(client.baseURL)
		if err != nil || origin.Scheme != "https" || origin.Hostname() == "" {
			return fmt.Errorf("explicit API address requires an HTTPS origin")
		}
		dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		transport.DialContext = func(ctx context.Context, network, target string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(target)
			if err != nil || host != origin.Hostname() {
				return nil, fmt.Errorf("unexpected API dial target")
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(address, port))
		}
	}
	client.httpClient = &http.Client{Transport: transport, Timeout: 120 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("API redirects are not allowed") }}
	return nil
}

func availabilityPublication(state platformStateArtifactEnvelope) (model.PlatformPublicationPrecondition, error) {
	if state.Artifact == nil || state.Release == nil || state.Release.ArtifactID != state.Artifact.ID || state.Release.FencingToken <= 0 || state.Release.Status != model.PlatformArtifactReleaseStatusActive {
		return model.PlatformPublicationPrecondition{}, fmt.Errorf("exact active publication is required")
	}
	return model.PlatformPublicationPrecondition{ArtifactID: state.Artifact.ID, ContentHash: state.Artifact.ContentHash, ReleaseID: state.Release.ID, FencingToken: state.Release.FencingToken}, nil
}

func availabilityBaseline(full platformStateArtifactEnvelope, authority model.PlatformPublicationPrecondition, lkg *model.PlatformLKGSnapshot) error {
	a, r := full.Artifact, full.Release
	if _, err := availabilityPublication(full); err != nil {
		return err
	}
	if a.ArtifactKind != model.PlatformArtifactKindReleaseSet || a.ScopeKey != "global" || a.Metadata[platformproducer.PolicyReleaseMetadata] != authority.ReleaseID || r.ReleasedByType != model.ActorTypeBootstrap || r.ReleasedByID != platformproducer.Actor || r.OverrideMode != "" || len(r.BypassedInvariants) != 0 {
		return fmt.Errorf("baseline must be a normal predecessor-owned global producer publication")
	}
	if lkg == nil || lkg.ArtifactID != a.ID || lkg.ContentHash != a.ContentHash || lkg.Generation != a.Generation || lkg.ScopeKey != "global" || lkg.ArtifactKind != a.ArtifactKind || !platformproducer.ValidDigest(lkg.VerificationEvidenceHash) || !lkg.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("exact unexpired positive LKG is required")
	}
	if r.VerificationState == model.PlatformArtifactVerificationStateVerified && lkg.VerifiedByReleaseID == r.ID && r.VerifiedLKGGeneration == a.Generation {
		return nil
	}
	if r.VerificationState != model.PlatformArtifactVerificationStateServingUnverified || r.PinnedRollbackGeneration != a.Generation || r.RollbackTargetGeneration != a.Generation || r.VerifiedLKGGeneration != a.Generation || lkg.VerifiedByReleaseID == "" || lkg.VerifiedByReleaseID == r.ID {
		return fmt.Errorf("unverified baseline is not an exact historical positive-LKG rollback")
	}
	for _, message := range full.Messages {
		if message.MessageType == "rollback" && message.ReleaseID == r.ID && message.ArtifactID == a.ID && message.ArtifactKind == a.ArtifactKind && message.ScopeKey == "global" && message.ReleaseChannel == "full" && message.Generation == a.Generation && message.CreatedAt.Equal(r.ReleasedAt) {
			return nil
		}
	}
	return fmt.Errorf("exact rollback ledger message is required")
}

func cloneAvailabilityContent(content map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(raw, &result)
	return result, err
}

func availabilitySource(content map[string]any, host string, policy model.DNSAvailabilityPolicy) (map[string]any, bool, error) {
	next, err := cloneAvailabilityContent(content)
	if err != nil {
		return nil, false, err
	}
	query, ok := next["dns_query_policy"].(map[string]any)
	if !ok {
		return nil, false, fmt.Errorf("explicit pinned DNS query policy is required")
	}
	var routes []platformconfig.DNSAvailabilityRoute
	raw, err := json.Marshal(query["availability_routes"])
	if err != nil {
		return nil, false, err
	}
	if err = json.Unmarshal(raw, &routes); err != nil {
		return nil, false, err
	}
	for i, route := range routes {
		if route.Hostname != host {
			continue
		}
		if reflect.DeepEqual(route.Policy, policy) {
			return next, false, nil
		}
		routes[i].Policy = policy
		query["availability_routes"] = routes
		return next, true, nil
	}
	routes = append(routes, platformconfig.DNSAvailabilityRoute{Hostname: host, Policy: policy})
	query["availability_routes"] = routes
	return next, true, nil
}

func (c *CLI) setDNSAvailability(ctx context.Context, client *Client, host string, policy model.DNSAvailabilityPolicy, opts dnsAvailabilityOptions) (result dnsAvailabilityResult, err error) {
	result = dnsAvailabilityResult{Hostname: host, Policy: policy, Phase: "read_baseline", DryRun: opts.DryRun}
	progress := func(phase string) { result.Phase = phase; fmt.Fprintln(c.stderr, "DNS availability:", phase) }
	current, err := client.GetPlatformStateArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil {
		return result, err
	}
	result.Authority, err = availabilityPublication(current)
	if err != nil {
		return result, err
	}
	previous, err := platformproducer.Decode(*current.Artifact)
	if err != nil {
		return result, err
	}
	if previous.TargetScope != "global" || previous.Mode != "serving" || previous.Serving == nil || previous.Serving.SinglePublication || previous.PublicationRole != "" {
		return result, fmt.Errorf("an established continuous global producer is required")
	}
	source, err := client.GetPlatformArtifact(previous.DNSPolicyArtifactID)
	if err != nil {
		return result, err
	}
	if source.ContentHash != previous.DNSPolicyDigest || source.Status != model.PlatformArtifactStatusValidated {
		return result, fmt.Errorf("pinned DNS source differs or is not validated")
	}
	result.DNSPolicyArtifactID = source.ID
	nextContent, changed, err := availabilitySource(source.Content, host, policy)
	if err != nil {
		return result, err
	}
	if changed {
		full, readErr := client.GetPlatformStateArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
		if readErr != nil {
			return result, readErr
		}
		if full.Artifact == nil {
			return result, fmt.Errorf("verified full baseline is required")
		}
		lkg, readErr := client.GetPlatformArtifactLKG(full.Artifact.ID)
		if readErr != nil {
			return result, readErr
		}
		if err = availabilityBaseline(full, result.Authority, lkg); err != nil {
			return result, err
		}
		if opts.DryRun {
			result.Phase = "declaration_checked_not_runtime_verified"
			return result, nil
		}
		precondition := model.PlatformProducerReconfiguration{Operation: "dns_availability_recovery", PreviousPolicy: result.Authority, VerificationEvidenceHash: lkg.VerificationEvidenceHash}
		precondition.ServingFull, err = availabilityPublication(full)
		if err != nil {
			return result, err
		}
		digest, err := platformconfig.Digest(map[string]any{"source_id": source.ID, "hostname": host, "policy": policy})
		if err != nil {
			return result, err
		}
		suffix := strings.TrimPrefix(digest, "sha256:")[:24]
		nextContent["generation"] = "dns-availability-" + suffix
		progress("validate_source")
		nextSource, err := ensureAvailabilityArtifact(client, "global", nextContent)
		if err != nil {
			return result, err
		}
		nextPolicy, err := cloneAvailabilityContent(current.Artifact.Content)
		if err != nil {
			return result, err
		}
		nextPolicy["generation"], nextPolicy["dns_policy_artifact_id"], nextPolicy["dns_policy_digest"] = "producer-availability-"+suffix, nextSource.ID, nextSource.ContentHash
		candidate, err := ensureAvailabilityArtifact(client, platformproducer.Scope, nextPolicy)
		if err != nil {
			return result, err
		}
		result.CandidateArtifactID = candidate.ID
		progress("validate_preview")
		if err := validateAvailabilityPreviews(client, current.Artifact.ID, candidate.ID, host, policy); err != nil {
			return result, err
		}
		progress("activate_producer")
		// The API rechecks the exact original producer/full/LKG and signed
		// source delta atomically. Never silently rebase a changed baseline.
		raw, err := json.Marshal(precondition)
		if err != nil {
			return result, err
		}
		var canonical map[string]any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&canonical); err != nil {
			return result, err
		}
		key, err := platformconfig.Digest(map[string]any{"artifact_id": candidate.ID, "content_hash": candidate.ContentHash, "precondition": canonical})
		if err != nil {
			return result, err
		}
		_, err = client.ReleasePlatformArtifact(candidate.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", ProducerReconfiguration: &precondition, IdempotencyKey: "producer-reconfiguration/" + key, Reason: "CLI per-host DNS availability; preserve existing holds and unrelated constraints"})
		if err != nil {
			return result, err
		}
		current, err = client.GetPlatformStateArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
		if err != nil {
			return result, err
		}
		result.Authority, err = availabilityPublication(current)
		if err != nil {
			return result, err
		}
		if result.Authority.ArtifactID != candidate.ID {
			return result, fmt.Errorf("producer successor is no longer selected")
		}
		result.DNSPolicyArtifactID = nextSource.ID
	}
	if opts.DryRun {
		result.Phase = "already_configured_not_runtime_verified"
		return result, nil
	}
	result.ProducerActivated = true
	progress("configured_waiting_for_serving")
	if !opts.Wait {
		return result, nil
	}
	last := "no matching full publication"
	for {
		selected, readErr := client.GetPlatformStateArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
		if readErr != nil {
			return result, readErr
		}
		ref, readErr := availabilityPublication(selected)
		if readErr != nil {
			return result, readErr
		}
		if ref != result.Authority {
			return result, fmt.Errorf("producer authority changed while waiting; rerun to inspect the selected configuration")
		}
		full, readErr := client.GetPlatformStateArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
		if readErr != nil {
			return result, readErr
		}
		if full.Artifact != nil && full.Release != nil && full.Artifact.Metadata[platformproducer.PolicyReleaseMetadata] == result.Authority.ReleaseID && full.Artifact.Metadata[platformproducer.DNSPolicyIDMetadata] == result.DNSPolicyArtifactID {
			view, readErr := c.loadCurrentArtifactState(client, full.Artifact.ID, "full")
			if readErr != nil {
				return result, readErr
			}
			result.ServingState = &view
			last = fmt.Sprintf("full=%s verification=%s missing=%v", full.Release.ID, full.Release.VerificationState, view.MissingEvidence)
			if view.ConsumersConverged && view.VerifiedLKG && view.Current.Artifact != nil && view.Current.Artifact.Metadata[platformproducer.PolicyReleaseMetadata] == result.Authority.ReleaseID && view.Current.Artifact.Metadata[platformproducer.DNSPolicyIDMetadata] == result.DNSPolicyArtifactID {
				result.RoutingAcceptanceComplete = true
				progress("serving_verified")
				return result, nil
			}
		}
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("serving not verified: %s: %w", last, ctx.Err())
		case <-time.After(10 * time.Second):
		}
	}
}

func ensureAvailabilityArtifact(client *Client, scope string, content map[string]any) (model.PlatformArtifact, error) {
	// Normalize nested typed policies to the same sorted-key JSON objects used
	// by API responses, so an interrupted command can reuse its exact drafts.
	var err error
	content, err = cloneAvailabilityContent(content)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	generation, _ := content["generation"].(string)
	query := url.Values{"kind": {model.PlatformArtifactKindPolicySnapshot}, "scope": {scope}, "generation": {generation}, "limit": {"2"}}
	var list platformArtifactListEnvelope
	if err := client.doJSON(http.MethodGet, "/v1/admin/artifacts?"+query.Encode(), nil, &list); err != nil {
		return model.PlatformArtifact{}, err
	}
	if len(list.Artifacts) > 1 {
		return model.PlatformArtifact{}, fmt.Errorf("ambiguous artifact generation")
	}
	var artifact model.PlatformArtifact
	if len(list.Artifacts) == 1 {
		artifact = list.Artifacts[0]
		if artifact.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || artifact.ScopeKey != scope || artifact.Generation != generation {
			return artifact, fmt.Errorf("artifact lookup identity differs")
		}
		existing, err := platformconfig.Digest(artifact.Content)
		if err != nil {
			return artifact, err
		}
		desired, err := platformconfig.Digest(content)
		if err != nil || desired != existing {
			return artifact, fmt.Errorf("existing generation content differs")
		}
	} else {
		var err error
		artifact, err = client.CreatePlatformArtifact(model.PlatformArtifactCreateRequest{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: generation, Content: content})
		if err != nil {
			return artifact, err
		}
	}
	if artifact.Status != model.PlatformArtifactStatusValidated {
		validated, err := client.ValidatePlatformArtifact(artifact.ID, false)
		if err != nil {
			return artifact, err
		}
		if !validated.Pass {
			return artifact, fmt.Errorf("artifact validation rejected: %+v", validated.Results)
		}
		artifact = validated.Artifact
	}
	return artifact, nil
}

type availabilityPreview struct {
	PreviewMode      string                         `json:"preview_mode"`
	BusinessRevision string                         `json:"business_snapshot_revision"`
	Intent           any                            `json:"intent"`
	Issues           any                            `json:"issues"`
	Policy           platformconfig.PolicySnapshot  `json:"policy"`
	Runtime          platformconfig.RuntimeSnapshot `json:"runtime_snapshot"`
	Summary          struct {
		Schema string `json:"schema"`
		Digest string `json:"full_snapshot_digest"`
	} `json:"runtime_summary"`
}

func validateAvailabilityPreviews(client *Client, beforeID, nextID, host string, policy model.DNSAvailabilityPolicy) error {
	read := func(id, view string) (availabilityPreview, error) {
		var preview availabilityPreview
		err := client.doJSON(http.MethodGet, "/v1/admin/platform-config/routes/project?"+url.Values{"producer_policy_artifact_id": {id}, "view": {view}}.Encode(), nil, &preview)
		if err != nil {
			return preview, fmt.Errorf("%s preview for %s: %w", view, id, err)
		}
		return preview, err
	}
	before, err := read(beforeID, "configuration")
	if err != nil {
		return err
	}
	after, err := read(nextID, "configuration")
	if err != nil {
		return err
	}
	runtime, err := read(nextID, "runtime_summary")
	if err != nil {
		return err
	}
	if before.BusinessRevision == "" || after.BusinessRevision == "" || runtime.BusinessRevision == "" || before.Intent == nil || !reflect.DeepEqual(before.Intent, after.Intent) || !reflect.DeepEqual(after.Intent, runtime.Intent) || !reflect.DeepEqual(before.Issues, after.Issues) {
		return fmt.Errorf("preview changed frozen business/static intent or configuration issues")
	}
	if runtime.Summary.Schema != "fugue.runtime-preview-summary/v1" || !platformproducer.ValidDigest(runtime.Summary.Digest) {
		return fmt.Errorf("complete runtime summary digest is required")
	}
	coverage, ok := runtime.Runtime.Facts["dynamic_quality_coverage"].(map[string]any)
	count, _ := coverage["query_count"].(float64)
	if !ok || coverage["mode"] != "all_dynamic" || count < 1 {
		return fmt.Errorf("live dynamic query coverage is required")
	}
	facts := 0
	for _, fact := range runtime.Runtime.DNSSelections {
		if fact.Hostname != host {
			continue
		}
		facts++
		matched := false
		for _, candidate := range fact.Candidates {
			matched = matched || slices.Contains(policy.OrderedEdgeIDs, candidate.EdgeID)
		}
		if !matched {
			return fmt.Errorf("hostname has no authorized availability candidate on DNS node %s", fact.NodeID)
		}
	}
	for _, rule := range runtime.Policy.DNSAnswerRules {
		if rule.Hostname == host && facts > 0 {
			return nil
		}
	}
	return fmt.Errorf("owned dynamic hostname is absent from the runtime preview")
}
