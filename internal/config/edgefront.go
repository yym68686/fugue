package config

import (
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgefront"
)

type EdgeFrontConfig = edgefront.Config

func EdgeFrontFromEnv() EdgeFrontConfig {
	nodeHost := getenv("FUGUE_EDGE_FRONT_NODE_HOST", "127.0.0.1")
	defaultSlot := getenv("FUGUE_EDGE_FRONT_DEFAULT_SLOT", "a")
	return EdgeFrontConfig{
		HTTPListenAddr:  getenv("FUGUE_EDGE_FRONT_HTTP_LISTEN_ADDR", ":80"),
		HTTPSListenAddr: getenv("FUGUE_EDGE_FRONT_HTTPS_LISTEN_ADDR", ":443"),
		HealthAddr:      getenv("FUGUE_EDGE_FRONT_HEALTH_LISTEN_ADDR", ":7831"),
		HTTPMode:        getenv("FUGUE_EDGE_FRONT_HTTP_MODE", edgefront.HTTPModeRedirect),
		ActiveSlotFile:  getenv("FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE", "/var/lib/fugue/edge-blue-green/active-slot"),
		DefaultSlot:     defaultSlot,
		DialTimeout:     getenvDuration("FUGUE_EDGE_FRONT_DIAL_TIMEOUT", 5*time.Second),
		ShutdownTimeout: getenvDuration("FUGUE_EDGE_FRONT_SHUTDOWN_TIMEOUT", 10*time.Second),
		ProxyProtocol:   getenvBool("FUGUE_EDGE_FRONT_PROXY_PROTOCOL", true),
		Slots: map[string]edgefront.SlotTargets{
			"a": {
				HTTPAddress:  edgeFrontTargetAddr("FUGUE_EDGE_FRONT_SLOT_A_HTTP_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_A_HTTP_PORT", 18080),
				HTTPSAddress: edgeFrontTargetAddr("FUGUE_EDGE_FRONT_SLOT_A_HTTPS_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_A_HTTPS_PORT", 18443),
			},
			"b": {
				HTTPAddress:  edgeFrontTargetAddr("FUGUE_EDGE_FRONT_SLOT_B_HTTP_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_B_HTTP_PORT", 28080),
				HTTPSAddress: edgeFrontTargetAddr("FUGUE_EDGE_FRONT_SLOT_B_HTTPS_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_B_HTTPS_PORT", 28443),
			},
		},
	}
}

func edgeFrontTargetAddr(addrEnv string, nodeHost string, portEnv string, defaultPort int) string {
	if value := strings.TrimSpace(os.Getenv(addrEnv)); value != "" {
		return value
	}
	port := defaultPort
	if raw := strings.TrimSpace(os.Getenv(portEnv)); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 65535 {
			port = parsed
		}
	}
	host := strings.TrimSpace(nodeHost)
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}
