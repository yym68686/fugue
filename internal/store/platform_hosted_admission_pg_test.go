package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"strings"
	"testing"
	"time"
)

// Test the production admission boundary, not the compiler: use an isolated
// schema with real Postgres fact/expectation columns, one untouched old
// physical Edge, one eligible Edge, and one DNS. No live credentials or ACKs.
func qualifyHostedProducerAdmission(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, ddl := range postgresSchemaStatements {
		if strings.HasPrefix(ddl, "CREATE TABLE IF NOT EXISTS fugue_platform_consumer_instances (") || strings.HasPrefix(ddl, "CREATE TABLE IF NOT EXISTS fugue_platform_expected_consumer_sets (") {
			if _, e := db.Exec(ddl); e != nil {
				t.Fatal(e)
			}
		}
	}
	digest := func(v string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(v))) }
	d := digest("issued")
	until := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	host := "qualification.example.test"
	base := pc.PreviousTrafficPublicationReference{ReleaseSetID: "parent", ReleaseID: "release", RouteArtifactID: "routes", TLSArtifactID: "certs", DNSArtifactID: "dns", ReleaseSetDigest: d, RouteArtifactDigest: d, TLSArtifactDigest: d, DNSArtifactDigest: d, FencingToken: 1}
	t.Run("recent refusal is exact and cannot outlive a newer capture", func(t *testing.T) {
		s := &Store{db: db, databaseURL: "fixture"}
		source := pc.HostedAddressSource{Publication: base, Authority: pc.HostedProposalAuthority{ExpiresAt: until}}
		refused := &pc.HostedExternalCompileRefusal{Host: host, TenantID: "tenant", AppID: "app", DesiredPathsDigest: d,
			Conflict: &pc.ExternalTargetConflict{Target: "shared.example.test", RecordType: "A", Field: "addresses", ExpectedAddresses: 3, CandidateAddresses: 2},
			Cause:    errors.New("fixture refusal")}
		refs := hostedExternalRefusals(errors.Join(fmt.Errorf("compiled host: %w", refused), errors.New("other")))
		if len(refs) != 1 || refs[0] != refused {
			t.Fatalf("wrong structured refusals: %+v", refs)
		}
		at := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
		if err := s.rememberHostedExternalRefusals(t.Context(), source, at, refs); err != nil {
			t.Fatal(err)
		}
		read := func() (string, time.Time) {
			t.Helper()
			var raw string
			var observed time.Time
			if err := db.QueryRow(`SELECT value,updated_at FROM fugue_meta WHERE key=$1`, hostedCompileDiagnosticsKey).Scan(&raw, &observed); err != nil {
				t.Fatal(err)
			}
			return raw, observed
		}
		raw, observed := read()
		var report hostedCompileDiagnostics
		if err := json.Unmarshal([]byte(raw), &report); err != nil {
			t.Fatal(err)
		}
		_, scope := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: host, TenantID: "tenant", AppID: "app"})
		got := matchingHostedCompileDiagnostic(report, scope, d, observed)
		if got == nil || got.CandidateAddresses != 2 || got.ExpectedAddresses != 3 ||
			matchingHostedCompileDiagnostic(report, scope, digest("new paths"), observed) != nil ||
			matchingHostedCompileDiagnostic(report, "other hostname", d, observed) != nil {
			t.Fatalf("refusal escaped its exact domain intent: %+v", got)
		}
		if err := s.rememberHostedExternalRefusals(t.Context(), source, at.Add(time.Millisecond), nil); err != nil {
			t.Fatal(err)
		}
		if err := s.rememberHostedExternalRefusals(t.Context(), source, at, refs); err != nil {
			t.Fatal(err)
		}
		if raw, _ := read(); raw != "" {
			t.Fatal("out-of-order capture reintroduced superseded refusal")
		}
	})
	owner := model.EdgeTLSAllowlistEntry{Hostname: host, TenantID: "tenant", AppID: "app"}
	eligible := pc.HostedAddressHold{EdgeID: "alpha", EdgeGroupID: "group", Address: "192.0.2.1"}
	held := pc.HostedAddressHold{EdgeID: "bravo", EdgeGroupID: "group", Address: "192.0.2.2"}
	record := pc.DNSIntent{Hostname: host, Type: "A", AppID: "app", TenantID: "tenant", TTL: 90, Values: []string{eligible.Address, held.Address}}
	p := pc.HostedScopeProposal{Schema: pc.HostedProposalSchema, Host: host, Before: pc.HostedDispatchRows{Owner: owner}, Authority: pc.HostedProposalAuthority{ExpiresAt: until}, Base: base, Physical: []pc.HostedAddressHold{eligible, held}, Eligible: []pc.HostedAddressHold{eligible}, Records: []pc.DNSIntent{record}}
	p.Prepared = pc.HostedDispatchRows{Owner: owner, Routes: []model.EdgeRouteIntent{{Hostname: host, PathPrefix: "/", AppID: "app"}}}
	stage := routeartifact.HostedPhase{Groups: map[string]routeartifact.HostedGroupContent{"group": {Routes: []model.EdgeRouteBinding{{Hostname: host, TenantID: "tenant", AppID: "app", EdgeGroupID: "group", PathPrefix: "/"}}, TLSAllowlist: []model.EdgeTLSAllowlistEntry{owner}}}, DNS: pc.HostedDNSContent{
		Views: []pc.DNSQueryView{{NodeID: "charlie", EdgeGroupID: "group", Zone: "example.test", Records: []model.EdgeDNSRecord{{Name: host, Type: "A", TenantID: "tenant", AppID: "app"}}}},
		Rules: []pc.DNSAnswerRule{{NodeID: "charlie", Hostname: host, Type: "A"}},
		Plan:  pc.DNSReadinessPlan{Records: []pc.DNSReadinessRecord{{Hostname: host, Targets: []pc.DNSReadinessTarget{{Family: "A", Address: eligible.Address, ProbeIDs: []string{"root"}}}}}, Probes: []pc.DNSReadinessProbe{{ID: "root", Hostname: host, Path: "/"}}}}}
	c := HostedSuccessorClaim{Scope: "host:qual", Host: host, Project: "project", Owner: d, Paths: d, Draft: "selected-qual", DraftDigest: d, Prepared: d, Forward: digest("fwd"), Phase: "prepare", Sequence: 1}
	wire, e := json.Marshal(map[string]any{"schema": "fugue.hosted-program/v1", "host": host, "base": base, "records": p.Records, "prepared": stage, "forward": stage, "draft_digest": d})
	if e != nil {
		t.Fatal(e)
	}
	im := routeartifact.CompiledHostedProgram{Wire: wire, Digest: digest(string(wire)), Prepared: c.Prepared, Forward: c.Forward}
	in := ProjectHostRouteInputs{Project: model.Project{ID: c.Project, TenantID: "tenant"}, Domain: model.AppDomain{Hostname: host, AppID: "app", TenantID: "tenant"}}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, e := db.Exec(q, args...); e != nil {
			t.Fatal(e)
		}
	}
	exec("INSERT INTO fugue_platform_artifacts VALUES($1,$2,'hosted_path_proposal',$3,'draft',clock_timestamp())", c.Draft, d, c.Scope)
	exec(hostedPrepareOfferCAS, c.Scope, c.Draft, d, c.Project, c.Owner, c.Paths, c.Prepared, c.Forward, until)
	exec(putHostedProgramSQL, c.Scope, c.Sequence, c.Draft, d, im.Digest, im.Wire, c.Prepared, c.Forward)
	kinds := []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindDNSAnswerBundle}
	for i, kind := range kinds {
		component := model.PlatformConsumerComponentEdgeWorker
		nodes := []string{"alpha", "bravo"}
		if i == 2 {
			component = model.PlatformConsumerComponentDNSServer
			nodes = []string{"charlie"}
		}
		var members []model.PlatformExpectedConsumer
		for _, node := range nodes {
			id := component + ":" + node
			m := model.PlatformExpectedConsumer{ConsumerID: id, NodeID: node, Component: component, ArtifactKind: kind, ScopeKey: "global", Cohort: "group", ExpectedGeneration: "gen", Required: true}
			members = append(members, m)
			caps := []string{platformcontrol.TrafficReleaseCapabilityV1, hostedMemberProtocol}
			if node == "bravo" {
				caps = caps[:1]
			}
			raw, _ := json.Marshal(caps)
			ks, _ := json.Marshal([]string{kind})
			exec(`INSERT INTO fugue_platform_consumer_instances(id,consumer_id,component,node_id,artifact_kind,scope_key,release_set_id,expected_consumer_set_id,fencing_token,supported_kinds_json,compatibility_capabilities_json,credential_id,token_id,issued_at,nonce,generation_sequence,sequence,evidence_hash,identity_verified,desired_generation,actual_generation,apply_status,probe_status,last_heartbeat_at,updated_at,protocol_version,schema_version)
    VALUES($1,$2,$3,$4,$5,'global','parent',$6,1,$7,$8,'cred','token',clock_timestamp(),'nonce',1,1,'evidence',true,'gen','gen','applied','passed',clock_timestamp(),clock_timestamp(),'v1','v1')`, id+kind, id, component, node, kind, "set-"+kind, string(ks), string(raw))
		}
		ms, _ := json.Marshal(members)
		exec(`INSERT INTO fugue_platform_expected_consumer_sets(id,release_set_id,artifact_release_id,artifact_kind,scope_key,expected_generation,topology_revision,revision,heartbeat_deadline,convergence_deadline,consumers_json,created_at,updated_at) VALUES($1,'parent','release',$2,'global','gen','topology',$3,now(),now(),$4,now(),now())`, "set-"+kind, kind, i+1, string(ms))
	}
	check := func(count, dirs int) {
		t.Helper()
		tx, e := db.BeginTx(t.Context(), nil)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback()
		if e = pgAdmitHostedOffer(t.Context(), tx, model.PlatformArtifact{ID: "parent", ScopeKey: "global"}, p, in, c, im, []pc.HostedAddressHold{held}); e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(); e != nil {
			t.Fatal(e)
		}
		var n, k int
		if e = db.QueryRow(`SELECT (SELECT count(*) FROM fugue_hosted_selections WHERE hostname=$1 AND phase='prepare' AND fence=1),(SELECT count(*) FROM fugue_hosted_member_directory WHERE hostname=$1)`, host).Scan(&n, &k); e != nil || n != count || k != dirs {
			t.Fatal("incorrect admission or membership directory", e, n, k)
		}
	}
	check(0, 0)
	checkBlockers := func(want []string) {
		t.Helper()
		tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		unit, err := routeartifact.OpenHostedPhase(p, c.DraftDigest, im.Digest, wire, "prepare")
		if err != nil {
			t.Fatal(err)
		}
		blockers, err := pgHostedPreparationBlockers(t.Context(), tx, model.PlatformArtifact{ID: "parent", ScopeKey: "global"}, p, unit, time.Now().UTC())
		if err != nil || len(blockers) != len(want) {
			t.Fatalf("incorrect read-only admission evidence: %+v %v; want %v", blockers, err, want)
		}
		for i, b := range blockers {
			if b.Code != "member_capability_missing" || b.NodeID != "bravo" || b.ArtifactKind != want[i] || b.Capability != hostedMemberProtocol {
				t.Fatalf("incorrect blocker: %+v", b)
			}
		}
	}
	checkBlockers(kinds[:2])
	caps, _ := json.Marshal([]string{platformcontrol.TrafficReleaseCapabilityV1, hostedMemberProtocol})
	exec("UPDATE fugue_platform_consumer_instances SET compatibility_capabilities_json=$1 WHERE node_id='bravo' AND artifact_kind=$2", string(caps), kinds[1])
	checkBlockers(kinds[:1])
	check(0, 0)
	exec("UPDATE fugue_platform_consumer_instances SET compatibility_capabilities_json=$1 WHERE node_id='bravo' AND artifact_kind=$2", string(caps), kinds[0])
	checkBlockers(nil)
	check(1, 5)
	check(1, 5)
	var facts int
	if e = db.QueryRow("SELECT count(*) FROM fugue_hosted_selection_receipts WHERE hostname=$1", host).Scan(&facts); e != nil || facts != 0 {
		t.Fatal("capability counted as application", e, facts)
	}
	// The real producer callback may advance only after the ledger has five
	// separate fresh *test* observations, not merely five capable processes.
	var rawParts []byte
	var decision string
	if e = db.QueryRow("SELECT members,decision FROM fugue_hosted_selections WHERE hostname=$1", host).Scan(&rawParts, &decision); e != nil {
		t.Fatal(e)
	}
	var members map[string]string
	if e = json.Unmarshal(rawParts, &members); e != nil || len(members) != 5 {
		t.Fatal("incomplete member manifest", e)
	}
	verifyParts := func(phase string, count int, state string, drift bool) {
		t.Helper()
		tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		unit, err := routeartifact.OpenHostedPhase(p, c.DraftDigest, im.Digest, wire, phase)
		if err != nil {
			t.Fatal(err)
		}
		var manifest []byte
		if err := tx.QueryRow("SELECT members FROM fugue_hosted_selections WHERE hostname=$1", host).Scan(&manifest); err != nil {
			t.Fatal(err)
		}
		out, err := pgHostedPublishedParts(t.Context(), tx, model.PlatformArtifact{ID: base.ReleaseSetID, ScopeKey: "global"}, p, unit, manifest, c, phase, until)
		if drift {
			if !errors.Is(err, ErrConflict) {
				t.Fatal("stale set/child passed", err, out)
			}
			return
		}
		if err != nil || out.State != state || out.HostSelection == nil || out.HostSelection.Passing != count || out.HostSelection.Required != 5 || out.HostSelection.Phase != phase {
			t.Fatal("incorrect publication status", err, out)
		}
		seenPassing := 0
		for _, member := range out.HostSelection.Members {
			if member.State == "passed" {
				seenPassing++
			} else if member.State != "waiting" {
				t.Fatalf("unexpected test receipt state: %+v", member)
			}
			if member.NodeID == "" || member.ArtifactKind == "" || (member.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle) != (member.Zone != "") {
				t.Fatalf("receipt lost physical identity: %+v", member)
			}
		}
		if len(out.HostSelection.Members) != 5 || seenPassing != count {
			t.Fatalf("receipt membership differs from aggregate: %+v", out.HostSelection)
		}
	}
	verifyParts("prepare", 0, "pending", false)
	for slot, content := range members {
		tx, e := db.BeginTx(t.Context(), nil)
		if e != nil {
			t.Fatal(e)
		}
		r := hostedPartReceipt{Host: host, Fence: 1, Decision: decision, Part: slot, Content: content, Evidence: digest("local-observation-" + slot), Passing: true, ObservedAt: time.Now().UTC(), ValidUntil: until}
		if e = pgRecordHostedPart(t.Context(), tx, r); e != nil {
			tx.Rollback()
			t.Fatal(e)
		}
		if e = tx.Commit(); e != nil {
			t.Fatal(e)
		}
	}
	verifyParts("prepare", 5, "pending", false)
	// Stop claiming that the entire global DNS release was verified
	// once the process owns individual records. It may only renew
	// its own protocol capability, never substitute for host receipts.
	kind := model.PlatformArtifactKindDNSAnswerBundle
	exec("UPDATE fugue_platform_consumer_instances SET issued_at=now()-interval '5 min', last_heartbeat_at=now()-interval '5 min' WHERE artifact_kind=$1", kind)
	check(1, 5)
	seen := func(credential, child string) {
		exec(hostedExecutorSeenSQL, model.PlatformConsumerComponentDNSServer+":charlie", kind, credential, base.ReleaseSetID, child, "set-"+kind, base.FencingToken, time.Now().UTC(), until)
	}
	seen("different-pod", base.DNSArtifactID)
	check(1, 5)
	seen("cred", "retired-child")
	check(1, 5)
	seen("cred", base.DNSArtifactID)
	exec("UPDATE fugue_platform_consumer_instances SET probe_status='failed' WHERE artifact_kind=$1", kind)
	check(1, 5)
	exec("UPDATE fugue_platform_consumer_instances SET probe_status='passed' WHERE artifact_kind=$1", kind)
	check(0, 5)
	var fence int64
	var phase string
	if e = db.QueryRow("SELECT fence,phase FROM fugue_hosted_selections WHERE hostname=$1", host).Scan(&fence, &phase); e != nil || fence != 2 || phase != "forward" {
		t.Fatal("producer did not advance the verified prepared head", e, phase, fence)
	}
	intent := pc.PlatformIntent{Scope: pc.GlobalScopeKey, Routes: []pc.RouteIntent{{Hostname: host, PathPrefix: "/", AppID: "app"}, {Hostname: "other.example.test", PathPrefix: "/", AppID: "another"}}}
	s := Store{db: db}
	verifyParts("forward", 0, "pending", false)
	for slot, content := range members { // Test fixture uses identical forward/prepare bytes, not identical fences.
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var forward string
		if err = tx.QueryRow("SELECT decision FROM fugue_hosted_selections WHERE hostname=$1", host).Scan(&forward); err == nil {
			err = pgRecordHostedPart(t.Context(), tx, hostedPartReceipt{Host: host, Fence: fence, Decision: forward, Part: slot, Content: content, Evidence: digest(forward + slot), Passing: true, ObservedAt: time.Now().UTC(), ValidUntil: until})
		}
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	verifyParts("forward", 5, "published", false)
	exec("UPDATE fugue_hosted_member_directory SET child_id='wrong' WHERE hostname=$1 AND kind=$2", host, kind)
	verifyParts("forward", 5, "pending", true)
	exec("UPDATE fugue_hosted_member_directory SET child_id=$3 WHERE hostname=$1 AND kind=$2", host, kind, base.DNSArtifactID)
	exec("UPDATE fugue_platform_expected_consumer_sets SET revision=revision+1 WHERE artifact_kind=$1", kind)
	verifyParts("forward", 5, "pending", true)
	// A simplistic fixture has no real tenant-owned draft. Its persisted
	// head may never fall back to global diagnostics or disclose a prior
	// tenant's decision, even for a request naming the same hostname.
	bind := func(ProjectHostRouteInputs, pc.HostedScopeProposal) (string, string, string, error) {
		t.Fatal("foreign binding was disclosed")
		return "", "", "", nil
	}
	project := model.Project{ID: c.Project, TenantID: "other-tenant"}
	plan := model.ProjectHostRoutePlan{HostnameOwnerAppID: "other-owner", Edit: model.ProjectHostRouteEdit{Hostname: host}}
	v, owned, err := s.ReadHostedRouteStatus(t.Context(), project, plan, bind)
	if err != nil || !owned || v.State != "pending" || v.HostSelection != nil {
		t.Fatal("foreign head exposed or ignored", v, owned, err)
	}
	plan.Edit.Hostname = "no-head.example.test"
	if _, owned, err = s.ReadHostedRouteStatus(t.Context(), project, plan, bind); err != nil || owned {
		t.Fatal("other host inherited selection", owned, err)
	}
	targets, _, e := s.HostedWithdrawalHints(t.Context(), intent, "")
	if e != nil || len(targets) != 1 || targets[0] != host {
		t.Fatal("reversion to global baseline not discovered", targets, e)
	}
	intent.Routes = append(intent.Routes, pc.RouteIntent{Hostname: host, PathPrefix: "/other", AppID: "changed"})
	if targets, _, e := s.HostedWithdrawalHints(t.Context(), intent, ""); e != nil || len(targets) != 0 {
		t.Fatal("unrelated change dispatched withdrawal", targets, e)
	}
}
