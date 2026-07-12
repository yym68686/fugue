package edge

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"testing"

	"fugue/internal/config"
)

func TestBuildCaddyConfigDisablesHTTP3ForTCPOnlyListeners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		listenAddr    string
		tlsMode       string
		proxyProtocol bool
	}{
		{
			name:       "systemd public listener",
			listenAddr: ":443",
			tlsMode:    caddyTLSModePublicOnDemand,
		},
		{
			name:          "blue green worker slot",
			listenAddr:    ":18443",
			tlsMode:       caddyTLSModeInternal,
			proxyProtocol: true,
		},
		{
			name:       "plain HTTP development listener",
			listenAddr: "127.0.0.1:18080",
			tlsMode:    caddyTLSModeOff,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service := NewService(config.EdgeConfig{
				APIURL:                         "https://api.example.invalid",
				EdgeToken:                      "edge-secret",
				EdgeGroupID:                    "edge-group-default",
				ListenAddr:                     "127.0.0.1:7832",
				CaddyEnabled:                   true,
				CaddyAdminURL:                  "http://127.0.0.1:2019",
				CaddyListenAddr:                tt.listenAddr,
				CaddyTLSMode:                   tt.tlsMode,
				CaddyProxyListenAddr:           "127.0.0.1:7833",
				CaddyProxyProtocolEnabled:      tt.proxyProtocol,
				CaddyProxyProtocolTrustedCIDRs: []string{"127.0.0.1/32"},
			}, log.New(ioDiscard{}, "", 0))

			configBody, _, err := service.buildCaddyConfig(testBundle("routegen_tcp_only"))
			if err != nil {
				t.Fatalf("build Caddy config: %v", err)
			}

			var document map[string]any
			if err := json.Unmarshal(configBody, &document); err != nil {
				t.Fatalf("decode Caddy config: %v", err)
			}
			apps := document["apps"].(map[string]any)
			httpApp := apps["http"].(map[string]any)
			servers := httpApp["servers"].(map[string]any)
			server := servers["fugue_edge"].(map[string]any)
			protocols, ok := server["protocols"].([]any)
			if !ok {
				t.Fatalf("Caddy server protocols missing or malformed: %#v", server["protocols"])
			}
			got := make([]string, 0, len(protocols))
			for _, protocol := range protocols {
				got = append(got, fmt.Sprint(protocol))
			}
			if want := []string{"h1", "h2"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("Caddy TCP listener protocols = %v, want %v", got, want)
			}
		})
	}
}
