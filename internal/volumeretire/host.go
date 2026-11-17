// Package volumeretire contains the node-side, identity-bound local volume
// retirement executor. It is embedded in the CLI so it also works when the
// application API or its database is unavailable.
package volumeretire

import _ "embed"

// HostScript is sent over an explicitly selected SSH connection. Inspect mode
// only reads metadata. Mutation requires a Kubernetes-fenced retirement plan.
//
//go:embed host.py
var HostScript string

// InventoryScript reduces streamed Kubernetes list responses before SSH output.
//
//go:embed inventory.py
var InventoryScript string
