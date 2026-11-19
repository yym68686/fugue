package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/releaseflow"
	"fugue/internal/releaseproof"
	"fugue/internal/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
)

// These checks can veto a stale withdrawal; they never create serving
// authority, change weights, or renew the expiry of a previous publication.
type withdrawalPath struct{ tenant, app, host, path string }

var errObsoleteWithdrawal = errors.New("recapture obsolete traffic withdrawal")

func withdrawalPaths(child model.PlatformArtifact, negative bool) (map[withdrawalPath]bool, error) {
	raw, err := json.Marshal(child.Content["routes"])
	if err != nil {
		return nil, err
	}
	var routes []platformconfig.CompiledRoute
	if err = json.Unmarshal(raw, &routes); err != nil {
		return nil, err
	}
	out := map[withdrawalPath]bool{}
	for _, r := range routes {
		if !r.Enabled || r.AppID == "" || r.TenantID == "" {
			continue
		}
		if negative {
			if r.Status != model.EdgeRouteStatusUnavailable || r.StatusReason != platformconfig.StableNotReadyRouteReason || len(r.Upstreams) != 0 {
				continue
			}
		} else if (r.Status != "" && r.Status != model.EdgeRouteStatusActive) || len(r.Upstreams) == 0 {
			continue
		}
		out[withdrawalPath{r.TenantID, r.AppID, r.Hostname, r.PathPrefix}] = true
	}
	return out, nil
}

func (s *Server) recheckTrafficWithdrawal(ctx context.Context, next, full model.PlatformArtifact) error {
	if next.ScopeKey != full.ScopeKey {
		return fmt.Errorf("traffic withdrawal scopes differ")
	}
	// DNS-only producers cannot withdraw HTTP routes.
	if next.Content["publication_role"] == platformconfig.PublicationRoleCellDNS {
		return nil
	}
	child, err := s.consumerAssignmentChild(next, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return err
	}
	paths, err := withdrawalPaths(child, true)
	if err != nil || len(paths) == 0 {
		return err
	}
	old, err := s.consumerAssignmentChild(full, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return err
	}
	positive, err := withdrawalPaths(old, false)
	if err != nil {
		return err
	}
	apps := map[string]string{}
	for path := range paths {
		if positive[path] {
			apps[path.app] = path.tenant
		}
	}
	if len(apps) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pa, err := s.store.GetPlatformArtifactByIdentity(model.PlatformArtifactKindPolicySnapshot, next.ScopeKey, next.Metadata["policy_generation"])
	if err != nil {
		return err
	}
	raw, err := json.Marshal(pa.Content)
	if err != nil {
		return err
	}
	var policy platformconfig.PolicySnapshot
	if err = json.Unmarshal(raw, &policy); err != nil {
		return err
	}
	b, err := s.store.CaptureRouteBusinessSnapshot(ctx)
	if err != nil {
		return err
	}
	managed := map[string]bool{model.DefaultManagedRuntimeID: true}
	for _, r := range b.Runtimes {
		managed[r.ID] = r.Type == model.RuntimeTypeManagedOwned || r.Type == model.RuntimeTypeManagedShared
	}
	targets := map[string]model.App{}
	for _, a := range b.Apps {
		if tenant, ok := apps[a.ID]; ok && tenant == a.TenantID {
			targets[a.ID] = a
		}
	}
	stable := map[string]model.AppTrafficPolicy{}
	for _, p := range b.TrafficPolicies {
		if apps[p.AppID] == p.TenantID {
			stable[p.AppID] = p
		}
	}
	releases := map[string]model.AppRelease{}
	for _, r := range b.Releases {
		if r.ID == stable[r.AppID].StableReleaseID {
			releases[r.ID] = r
		}
	}
	checked := map[string]model.AppTrafficPolicy{}
	for _, rule := range policy.TrafficConstraints {
		if tenant, ok := apps[rule.AppID]; !ok || rule.TenantID != tenant {
			continue
		}
		app, ok := targets[rule.AppID]
		r := releases[rule.StableReleaseID]
		p := stable[rule.AppID]
		if !ok || p.ID != rule.ID || p.StableReleaseID != rule.StableReleaseID || p.Mode != rule.Mode || p.CandidateReleaseID != rule.CandidateReleaseID || p.StableWeight != rule.StableWeight || p.CandidateWeight != rule.CandidateWeight || p.StickyCookie != rule.StickyCookie || p.StickyHeader != rule.StickyHeader || r.ID == "" || r.AppID != app.ID || r.TenantID != app.TenantID {
			return fmt.Errorf("%w: app=%s stable selection changed", errObsoleteWithdrawal, rule.AppID)
		}
		checked[app.ID] = p
		if !managed[r.RuntimeID] {
			continue
		}
		ready, reason, err := s.recheckManagedStable(ctx, app, r)
		if err != nil {
			return fmt.Errorf("cannot confirm traffic withdrawal for app=%s release=%s: %w", app.ID, r.ID, err)
		}
		if ready {
			return fmt.Errorf("%w: app=%s release=%s is now ready", errObsoleteWithdrawal, app.ID, r.ID)
		}
		if s.log != nil {
			s.log.Printf("confirmed traffic withdrawal app=%s release=%s artifact=%s reason=%s", app.ID, r.ID, next.ID, reason)
		}
	}
	if len(checked) != len(apps) {
		return fmt.Errorf("recapture withdrawal: stable policy binding absent")
	}
	for app, p := range checked {
		current, err := s.store.GetAppTrafficPolicy(p.TenantID, false, app)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, p) {
			return fmt.Errorf("%w: policy changed during observation for app=%s", errObsoleteWithdrawal, app)
		}
	}
	return ctx.Err()
}

func (s *Server) recheckManagedStable(ctx context.Context, app model.App, r model.AppRelease) (bool, string, error) {
	client, err := s.managedAppStatusClient()
	if err != nil {
		return false, "", err
	}
	defer client.closeIdleConnections()
	cluster, err := client.getClusterID(ctx)
	if err != nil || cluster == "" {
		return false, "", fmt.Errorf("cluster identity unavailable: %v", err)
	}
	target := releaseflow.CanonicalStableTarget(app, r)
	ns := runtime.NamespaceForTenant(app.TenantID)
	if len(validation.IsDNS1123Label(ns)) != 0 || len(validation.IsDNS1123Subdomain(target.DeploymentName)) != 0 || len(validation.IsDNS1123Subdomain(target.ServiceName)) != 0 {
		return false, "", fmt.Errorf("release workload identity unavailable")
	}
	snap, err := readWithdrawalResources(ctx, client, ns, target.DeploymentName, target.ServiceName)
	confirmed, identityErr := client.getClusterID(ctx)
	if identityErr != nil || confirmed != cluster {
		return false, "", fmt.Errorf("cluster identity changed during withdrawal observation")
	}
	if err != nil {
		if errors.Is(err, errWithdrawalWorkloadAbsent) {
			return false, "referenced workload resource absent", nil
		}
		return false, "", err
	}
	_, proof := releaseproof.Observe(app, r, true, snap, time.Now().UTC(), s.observedRuntimeImageRefsEquivalent)
	return proof.Ready, proof.Reason, nil
}

var errWithdrawalWorkloadAbsent = errors.New("target Deployment or Service absent")

func readWithdrawalResources(ctx context.Context, c *managedAppStatusClient, ns, dep, svc string) (releaseproof.Snapshot, error) {
	snap := releaseproof.Snapshot{}
	read := func(path string, v any) error { return c.doJSON(ctx, path, v) }
	workload := func(path string, v any) error {
		err := read(path, v)
		var status *kubeStatusError
		if errors.As(err, &status) && status.StatusCode == http.StatusNotFound {
			return errWithdrawalWorkloadAbsent
		}
		return err
	}
	var d releaseproof.Deployment
	var service releaseproof.Service
	dp, sp := "/apis/apps/v1/namespaces/"+ns+"/deployments/"+dep, "/api/v1/namespaces/"+ns+"/services/"+svc
	if err := workload(dp, &d); err != nil {
		return snap, err
	}
	if err := workload(sp, &service); err != nil {
		return snap, err
	}
	if d.Metadata.Namespace != ns || service.Metadata.Namespace != ns || d.Metadata.Name != dep || service.Metadata.Name != svc {
		return snap, fmt.Errorf("withdrawal target identities changed")
	}
	var list kubeTypedResourceList[releaseproof.EndpointSlice]
	epPath := "/apis/discovery.k8s.io/v1/namespaces/" + ns + "/endpointslices?labelSelector=" + url.QueryEscape("kubernetes.io/service-name="+svc)
	if err := read(epPath, &list); err != nil {
		return snap, err
	}
	if list.Metadata.Continue != "" {
		return snap, fmt.Errorf("incomplete withdrawal endpoint inventory")
	}
	key := releaseproof.NamespacedKey
	snap = releaseproof.Snapshot{Namespaces: map[string]struct{}{ns: {}}, Deployments: map[string]releaseproof.Deployment{key(ns, dep): d}, Services: map[string]releaseproof.Service{key(ns, svc): service}, EndpointSlicesAvailable: true, EndpointSlices: map[string][]releaseproof.EndpointSlice{key(ns, svc): list.Items}, Pods: map[string]releaseproof.Pod{}, ReplicaSets: map[string]releaseproof.ReplicaSet{}}
	for _, slice := range list.Items {
		for _, ep := range slice.Endpoints {
			ref := ep.TargetRef
			if ref.Namespace != ns || ref.Kind != "Pod" || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 {
				continue
			} // Observe rejects the original invalid endpoint.
			k := key(ns, ref.Name)
			if _, ok := snap.Pods[k]; ok {
				continue
			}
			var pod releaseproof.Pod
			if err := read("/api/v1/namespaces/"+ns+"/pods/"+ref.Name, &pod); err != nil {
				return snap, err
			}
			snap.Pods[k] = pod
			owner, ok := releaseproof.SingleControllerOwner(pod.Metadata.OwnerReferences, "apps/v1", "ReplicaSet")
			if !ok || len(validation.IsDNS1123Subdomain(owner.Name)) != 0 {
				continue
			}
			k = key(ns, owner.Name)
			if _, ok := snap.ReplicaSets[k]; ok {
				continue
			}
			var rs releaseproof.ReplicaSet
			if err := read("/apis/apps/v1/namespaces/"+ns+"/replicasets/"+owner.Name, &rs); err != nil {
				return snap, err
			}
			snap.ReplicaSets[k] = rs
		}
	}
	var d2 releaseproof.Deployment
	var s2 releaseproof.Service
	var l2 kubeTypedResourceList[releaseproof.EndpointSlice]
	if err := read(dp, &d2); err != nil {
		return snap, err
	}
	if err := read(sp, &s2); err != nil {
		return snap, err
	}
	if err := read(epPath, &l2); err != nil {
		return snap, err
	}
	if d.Metadata.ResourceVersion == "" || service.Metadata.ResourceVersion == "" || d.Metadata.UID != d2.Metadata.UID || d.Metadata.ResourceVersion != d2.Metadata.ResourceVersion || service.Metadata.UID != s2.Metadata.UID || service.Metadata.ResourceVersion != s2.Metadata.ResourceVersion || l2.Metadata.Continue != "" || !reflect.DeepEqual(list.Items, l2.Items) {
		return snap, fmt.Errorf("recapture withdrawal: workload changed during observation")
	}
	return snap, nil
}
