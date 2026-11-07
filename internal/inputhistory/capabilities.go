// Package inputhistory describes binary protocol support, not retirement
// readiness. A report cannot attest to a complete dependency set, prepared pins,
// old-process drainage, retention, or any physical space recovery.
package inputhistory

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
)

const CapabilityArgument = "--compiler-input-history-capabilities"

//go:embed capabilities.json
var contract []byte

type Report struct {
	Schema         string   `json:"schema"`
	Role           string   `json:"role"`
	Capabilities   []string `json:"capabilities"`
	ContractSHA256 string   `json:"contract_sha256"`
}

// BinaryCapabilities is also exposed by the private runtime storage provider.
// It reads no configuration, database, key, input body or network resource.
func BinaryCapabilities(role string) (Report, error) {
	var declaration struct {
		Schema  string              `json:"schema"`
		Version int                 `json:"version"`
		Roles   map[string][]string `json:"roles"`
	}
	if json.Unmarshal(contract, &declaration) != nil || declaration.Schema != "fugue.compiler-input-history.capability-contract.v1" || declaration.Version != 1 || len(declaration.Roles[role]) == 0 {
		return Report{}, errors.New("compiler input binary capability contract unavailable")
	}
	sum := sha256.Sum256(contract)
	return Report{Schema: "fugue.compiler-input-history.binary-capabilities.v1", Role: role,
		Capabilities: append([]string(nil), declaration.Roles[role]...), ContractSHA256: hex.EncodeToString(sum[:])}, nil
}

// WriteCapabilities handles ONLY this exact diagnostic invocation. A malformed
// capability request must not fall through to service startup or a maintenance
// operation. Call before flag/config/environment parsing and Store.Init.
func WriteCapabilities(args []string, role string, out io.Writer) (bool, error) {
	found := false
	for _, arg := range args {
		found = found || arg == CapabilityArgument
	}
	if !found {
		return false, nil
	}
	if len(args) != 1 || args[0] != CapabilityArgument {
		return true, errors.New("compiler input capability diagnostic accepts no other arguments")
	}
	report, err := BinaryCapabilities(role)
	if err != nil {
		return true, err
	}
	return true, json.NewEncoder(out).Encode(report)
}
