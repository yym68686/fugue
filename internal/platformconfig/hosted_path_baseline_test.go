package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestHostedPathBaselineStagesOnlyPriorShapeWithCurrentApplication(t *testing.T) {
	for _, test := range []string{"valid", "different_deployment", "foreign_owner", "tls_change", "disabled_path", "inconsistent_evidence", "missing_evidence", "wrong_origin_ref", "stale_report_not_renewed", "existing_root_conflict"} {
		t.Run(test, func(t *testing.T) {
			base := applicationDNSFixture()
			base.Scope = GlobalScopeKey
			r := &base.Routes[0]
			r.RecordOriginForTest(t, "/", "runtime-a")
			r.RoutePolicy = "edge_enabled"
			r.CacheNamespace = "prior-cache"
			r.DeploymentGeneration = "old-deployment"
			base.DNS[0].RecordKind = "hosted"
			base.TLS = []TLSIntent{{Hostname: r.Hostname, AppID: r.AppID, TenantID: r.TenantID, DomainRef: "domain-ref", Policy: "automatic"}}
			base = NormalizePlatformIntent(base)
			desired := base
			desired.Generation = "new-input"
			old := base.Routes[0]
			host := old.Hostname
			head := old
			head.AppID = "another-app"
			head.RecordOriginForTest(t, "/", old.RuntimeID)
			v := old
			v.RecordOriginForTest(t, "/service", old.RuntimeID)
			v.DeploymentGeneration = "current-deployment"
			v.CacheNamespace = "current-cache"
			z := v
			z.RecordOriginForTest(t, "/live", old.RuntimeID)
			desired.Routes = []RouteIntent{head, v, z}
			d := base.DNS[0]
			d.Type = "FUGUE_ROUTE"
			d.Values = []string{}
			d.Route = &DNSRouteIntent{DNSApplicationIntent: *d.Application, Hostnames: []string{host}, ManagedHost: true, Bindings: []DNSRouteBinding{{Hostname: host, PathPrefix: "/", AppID: head.AppID}, {Hostname: host, PathPrefix: "/service", AppID: v.AppID}, {Hostname: host, PathPrefix: "/live", AppID: z.AppID}}}
			d.Application = nil
			desired.DNS = []DNSIntent{d}
			now := time.Unix(1700000000, 0).UTC()
			w := OriginObservation{Ref: v.OriginRef, ObservedAt: now.Add(-time.Minute), Status: "active", RuntimeID: v.RuntimeID, RuntimeClusterNode: "node-a"}
			other := w
			other.Ref = z.OriginRef
			in := RuntimeSnapshot{CapturedAt: &now, IntentGeneration: desired.Generation, Origins: []OriginObservation{w, other}}
			switch test {
			case "different_deployment":
				desired.Routes[2].DeploymentGeneration = "other"
			case "foreign_owner":
				desired.Routes[2].TenantID = "someone-else"
			case "tls_change":
				desired.TLS = append([]TLSIntent(nil), base.TLS...)
				desired.TLS[0].Policy = "other"
			case "disabled_path":
				desired.Routes[1].Enabled = false
			case "inconsistent_evidence":
				in.Origins[1].ObservedAt = now
			case "missing_evidence":
				in.Origins = in.Origins[:1]
			case "wrong_origin_ref":
				desired.Routes[2].OriginRef = v.OriginRef
			case "stale_report_not_renewed":
				in.Origins[0].Status = "unavailable"
				in.Origins[1].Status = "unavailable"
			case "existing_root_conflict":
				c := w
				c.Ref = old.OriginRef
				c.RuntimeClusterNode = "another-node"
				in.Origins = append(in.Origins, c)
			}
			input, _ := json.Marshal([]any{base, desired, in})
			stage, e := StageHostedPathBaseline(base, desired, []string{host})
			var facts RuntimeSnapshot
			if e == nil {
				facts, e = StageHostedOriginFacts(desired, stage, in, []string{host})
			}
			if test != "valid" && test != "stale_report_not_renewed" {
				if e == nil {
					t.Fatal("unsafe baseline accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			root := stage.Routes[0]
			last := facts.Origins[len(facts.Origins)-1]
			if len(stage.Routes) != 1 || root.AppID != old.AppID || root.OriginRef != old.OriginRef || root.DeploymentGeneration != v.DeploymentGeneration || root.CacheNamespace != v.CacheNamespace || last.Ref != old.OriginRef || last.Status != in.Origins[0].Status || last.ObservedAt != w.ObservedAt || facts.IntentGeneration != stage.Generation || !reflect.DeepEqual(stage.DNS, base.DNS) {
				t.Fatal("changed custody, application version or observed readiness")
			}
			after, _ := json.Marshal([]any{base, desired, in})
			if string(input) != string(after) {
				t.Fatal("mutated saved intent or input")
			}
		})
	}
}

func (r *RouteIntent) RecordOriginForTest(t *testing.T, path, runtime string) {
	t.Helper()
	r.PathPrefix = path
	r.RuntimeID = runtime
	var err error
	r.OriginRef, err = OriginObservationRef(r.Hostname, path, r.AppID, r.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}
}
