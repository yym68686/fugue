package api

import (
	_ "embed"
	"encoding/json"
)

//go:embed node_dns_probe.py
var nodeDNSProbePython string

func nodeDNSProbePythonLiteral() string {
	encoded, _ := json.Marshal(nodeDNSProbePython)
	return string(encoded)
}

//go:embed node_resource_policy.py
var nodeResourcePolicyPython string
