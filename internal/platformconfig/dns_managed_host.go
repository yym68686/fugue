package platformconfig

import (
	"encoding/json"
	"fmt"

	"fugue/internal/model"
)

const managedHostDNSBindingsField = "managed_host_dns_bindings"

// DNSManagedHostContinuityCapabilityV1 selects exact path probes from the
// signed route publication while preserving the active DNS record's owner.
const DNSManagedHostContinuityCapabilityV1 = "dns_managed_host_continuity_v1"

// A route publication can explicitly authorize the same managed DNS owner to
// follow its exact set of paths. Neither a TLS allowlist nor same-tenant routes
// alone authorize a change to an older DNS configuration.
type managedHostDNSBinding struct {
	Hostname string            `json:"hostname"`
	AppID    string            `json:"app_id"`
	TenantID string            `json:"tenant_id"`
	Bindings []DNSRouteBinding `json:"bindings"`
}

func addManagedHostDNSBindings(payload map[string]any, intent PlatformIntent, tls []model.EdgeTLSAllowlistEntry) error {
	if intent.PublicationRole == PublicationRoleCellDNS {
		return nil
	}
	byHost := make(map[string]model.EdgeTLSAllowlistEntry, len(tls))
	for _, entry := range tls {
		byHost[entry.Hostname] = entry
	}
	bindings := []managedHostDNSBinding{}
	for _, record := range intent.DNS {
		if record.Route == nil || !record.Route.ManagedHost {
			continue
		}
		if !verifiedManagedHostOwner(record, byHost[record.Hostname]) {
			return fmt.Errorf("managed host DNS binding requires the verified TLS owner for %s", record.Hostname)
		}
		bindings = append(bindings, managedHostDNSBinding{Hostname: record.Hostname, AppID: record.AppID, TenantID: record.TenantID, Bindings: record.Route.Bindings})
	}
	if len(bindings) != 0 {
		payload[managedHostDNSBindingsField] = bindings
	}
	return nil
}

func verifiedManagedHostOwner(record DNSIntent, entry model.EdgeTLSAllowlistEntry) bool {
	return entry.Hostname == record.Hostname && entry.AppID == record.AppID && entry.TenantID == record.TenantID &&
		entry.Status == model.AppDomainStatusVerified
}

func readManagedHostDNSBindings(route model.PlatformArtifact) (map[string]managedHostDNSBinding, error) {
	byHost := map[string]managedHostDNSBinding{}
	content, ok := route.Content[managedHostDNSBindingsField]
	if !ok {
		return byHost, nil
	}
	raw, err := json.Marshal(content)
	var bindings []managedHostDNSBinding
	if err != nil || json.Unmarshal(raw, &bindings) != nil || len(bindings) > 10000 {
		return nil, fmt.Errorf("managed host DNS bindings are malformed")
	}
	for _, binding := range bindings {
		if binding.Hostname == "" || byHost[binding.Hostname].Hostname != "" {
			return nil, fmt.Errorf("managed host DNS binding identity is invalid or duplicated")
		}
		byHost[binding.Hostname] = binding
	}
	return byHost, nil
}

// Adapt only the readiness requirements for one verified route publication.
// The DNS record, endpoints, quorum, and selection remain owned by the signed
// DNS artifact. The result still requires a complete set of exact path probes
// from the selected publication, and does not transfer or refresh any proof.
func runtimeManagedHostDNSRecord(record DNSIntent, binding managedHostDNSBinding, tls model.EdgeTLSAllowlistEntry) (DNSIntent, error) {
	if record.Application == nil && (record.Route == nil || !record.Route.ManagedHost) {
		return record, nil
	}
	if binding.Hostname == "" && record.Application != nil {
		return record, nil
	}
	if !verifiedManagedHostOwner(record, tls) || tls.TLSStatus != model.AppDomainTLSStatusReady {
		return record, fmt.Errorf("managed host DNS publication TLS owner differs for %s", record.Hostname)
	}
	options := *DNSPlacementOptions(record)
	if binding.Hostname == "" {
		// Pre-feature and rollback publications do not declare a shared binding.
		// The regular validator must therefore find only the original sole app.
		record.Type, record.Values, record.Route, record.Application = "FUGUE_APP", []string{record.AppID}, nil, &options
		return record, nil
	}
	if binding.Hostname != record.Hostname || binding.AppID != record.AppID || binding.TenantID != record.TenantID {
		return record, fmt.Errorf("managed host DNS publication owner differs for %s", record.Hostname)
	}
	record.Type, record.Values, record.Application = "FUGUE_ROUTE", nil, nil
	record.Route = &DNSRouteIntent{DNSApplicationIntent: options, Hostnames: []string{record.Hostname}, Bindings: binding.Bindings, ManagedHost: true}
	return record, validateDNSRouteConfiguration(record)
}
