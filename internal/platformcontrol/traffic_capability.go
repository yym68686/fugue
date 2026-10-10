package platformcontrol

// TrafficReleaseCapabilityV1 describes executable support for this fact's
// artifact kind: signed parent authority, current assignment, explicit rollback
// and durable recovery. DNS also enforces per-value and runtime proof expiry;
// Edge verifies actual Caddy/route/TLS and reports failures. It never means that
// the reported artifact has been applied, probed or verified as an LKG.
const TrafficReleaseCapabilityV1 = "traffic_release_v1"

// CellRoutesCapabilityV1 adds the explicitly signed route/TLS-only publication
// boundary. It never authorizes removal of DNS from a complete traffic set.
const CellRoutesCapabilityV1 = "cell_routes_v1"

// CellDNSCapabilityV1 verifies independent DNS membership and exact embedded Cell route/TLS references.
const CellDNSCapabilityV1 = "cell_dns_v1"

// DNSAuthorityTransitionCapabilityV1 verifies signed previous global sources,
// explicit alias equivalence and coherent per-physical-target proof alternatives.
const DNSAuthorityTransitionCapabilityV1 = "dns_authority_transition_v1"

// DNSRouteSourcesCapabilityV1 verifies approved source configuration separately
// from exact observed serving publication bindings and DNS candidate policy.
const DNSRouteSourcesCapabilityV1 = "dns_route_sources_v1"

const PhysicalNetworkBoundedCapabilityV3 = "physical_network_bounded_v3"
const PhysicalNetworkDeliveryCapabilityV4 = "physical_network_delivery_v4"
const PhysicalOrderCapabilityV1 = "physical_order_v1"
const PhysicalOrderProjectionCapabilityV1 = "physical_order_projection_v1"
