// Package prewarmconfig defines declarative TLS observation targets. They
// are never shell input or rules that change tenant routing.
package prewarmconfig

import (
	"encoding/json"
	"errors"
	"fugue/internal/tlsissuance"
)

const Annotation = "fugue.pro/private-prewarm-hosts-v1"

func Parse(value string) ([]string, error) {
	var hosts []string
	if len(value) > 4096 || json.Unmarshal([]byte(value), &hosts) != nil || len(hosts) == 0 || len(hosts) > 16 {
		return nil, errors.New("invalid private TLS target list")
	}
	seen := map[string]bool{}
	for _, h := range hosts {
		if !tlsissuance.ValidHostname(h) || seen[h] {
			return nil, errors.New("invalid or duplicate private TLS target")
		}
		seen[h] = true
	}
	return hosts, nil
}
