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
