package platformconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"reflect"
	"strings"

	"fugue/internal/model"
)

func CloneCachePolicies(in []model.CachePolicy) []model.CachePolicy {
	out := append([]model.CachePolicy(nil), in...)
	for i := range out {
		out[i].PathPatterns = append([]string(nil), out[i].PathPatterns...)
		out[i].MethodAllowlist = append([]string(nil), out[i].MethodAllowlist...)
		out[i].StatusAllowlist = append([]int(nil), out[i].StatusAllowlist...)
		out[i].VaryAllowlist = append([]string(nil), out[i].VaryAllowlist...)
	}
	return out
}

// ValidateRouteBehavior checks the same fields before compilation and before
// projection. An artifact cannot silently omit an invalid ingress boundary.
func ValidateRouteBehavior(routes []RouteIntent, policies []model.CachePolicy) error {
	if len(policies) > 128 {
		return fmt.Errorf("at most 128 cache policies are supported")
	}
	byID := make(map[string]model.CachePolicy, len(policies))
	for _, policy := range policies {
		key := strings.ToLower(policy.ID)
		if policy.ID == "" || policy.ID != strings.TrimSpace(policy.ID) {
			return fmt.Errorf("cache policy requires a canonical id")
		}
		if _, duplicate := byID[key]; duplicate {
			return fmt.Errorf("cache policy ids must be unique")
		}
		switch policy.Kind {
		case model.CachePolicyKindStaticAssets, model.CachePolicyKindHTMLDocuments, model.CachePolicyKindDisabled:
		default:
			return fmt.Errorf("unsupported cache policy kind")
		}
		if policy.TTLSeconds < 0 || policy.TTLSeconds > 31536000 || policy.StaleWhileRevalidateSeconds < 0 || policy.StaleWhileRevalidateSeconds > 31536000 {
			return fmt.Errorf("cache policy durations must be within 0..31536000 seconds")
		}
		if policy.PurgeMode != "" && policy.PurgeMode != model.CachePolicyPurgeModeGeneration && policy.PurgeMode != model.CachePolicyPurgeModeNone {
			return fmt.Errorf("unsupported cache purge mode")
		}
		if len(policy.PathPatterns) > 128 || len(policy.MethodAllowlist) > 2 || len(policy.StatusAllowlist) > 100 || len(policy.VaryAllowlist) > 32 {
			return fmt.Errorf("cache policy rule list is too large")
		}
		for _, pattern := range policy.PathPatterns {
			if _, err := path.Match(pattern, "/"); err != nil || pattern == "" || pattern != strings.TrimSpace(pattern) {
				return fmt.Errorf("cache policy path pattern is invalid")
			}
		}
		for _, method := range policy.MethodAllowlist {
			if method != http.MethodGet && method != http.MethodHead {
				return fmt.Errorf("cache policy methods must be GET or HEAD")
			}
		}
		for _, status := range policy.StatusAllowlist {
			if status < 200 || status > 599 {
				return fmt.Errorf("cache policy status is invalid")
			}
		}
		for _, value := range append(append([]string{}, policy.VaryAllowlist...), policy.BrowserCacheControl, policy.EdgeCacheControl) {
			if strings.ContainsAny(value, "\r\n\x00") {
				return fmt.Errorf("cache policy contains invalid header characters")
			}
		}
		byID[key] = policy
	}
	for _, route := range routes {
		if route.CachePolicyID != "" {
			policy, exists := byID[strings.ToLower(route.CachePolicyID)]
			if !exists || route.CachePolicyID != strings.TrimSpace(route.CachePolicyID) {
				return fmt.Errorf("route references an unknown cache policy")
			}
			if policy.Kind != model.CachePolicyKindDisabled && strings.TrimSpace(route.CacheNamespace) == "" {
				return fmt.Errorf("cached route requires an explicit cache namespace")
			}
			if policy.HostnameScope != "" && !strings.EqualFold(strings.Trim(policy.HostnameScope, "."), strings.Trim(route.Hostname, ".")) {
				return fmt.Errorf("cache hostname scope does not match route")
			}
		}
		if len(route.RequestBodyPolicies) > 0 {
			raw, err := json.Marshal(route.RequestBodyPolicies)
			if err != nil {
				return fmt.Errorf("request body policies cannot be encoded")
			}
			parsed, err := model.ParseEdgeRequestBodyPolicies(string(raw))
			if err != nil {
				return err
			}
			// The executor uses sorted methods and paths for matching. Require
			// canonical rules rather than silently changing the signed intent.
			if !reflect.DeepEqual(parsed, route.RequestBodyPolicies) {
				return fmt.Errorf("request body policies must be canonical with explicit retry_after_seconds")
			}
		}
	}
	return nil
}
