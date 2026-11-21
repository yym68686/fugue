// Package authority defines the wire syntax shared by topology and durable Front state.
package authority

// IDPattern accepts explicit neutral cell identities and the legacy
// group namespace during migration. Neither form encodes placement or country
// semantics; callers must verify exact signed scope and current authority.
const IDPattern = `^(?:edge-group-|cell-)[a-z0-9]+(?:-[a-z0-9]+)*$`
