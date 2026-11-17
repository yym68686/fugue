// Package cnpgcontract exposes the single declarative, verified operator receipt.
// The migration implementation must not maintain a second image allowlist.
package cnpgcontract

import _ "embed"

//go:embed release.json
var Release []byte
