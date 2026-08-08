package edgegroupfront

import (
	"bufio"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func ConfigFromEnv() Config {
	nodeHost := envValue("FUGUE_EDGE_FRONT_NODE_HOST", "127.0.0.1")
	defaultSlot := envValue("FUGUE_EDGE_FRONT_DEFAULT_SLOT", "a")
	nodeEnv := readEnvFile(envValue("FUGUE_EDGE_FRONT_NODE_ENV_FILE", envValue("FUGUE_EDGE_NODE_ENV_FILE", "/etc/fugue/edge-node.env")))
	return Config{
		HTTPListenAddr:     envValue("FUGUE_EDGE_FRONT_HTTP_LISTEN_ADDR", ":80"),
		HTTPSListenAddr:    envValue("FUGUE_EDGE_FRONT_HTTPS_LISTEN_ADDR", ":443"),
		HealthAddr:         envValue("FUGUE_EDGE_FRONT_HEALTH_LISTEN_ADDR", ":7831"),
		EdgeID:             envValue("FUGUE_EDGE_FRONT_EDGE_ID", nodeHost),
		EdgeGroupID:        envFileValue(nodeEnv, "FUGUE_EDGE_FRONT_EDGE_GROUP_ID", envFileValue(nodeEnv, "FUGUE_EDGE_GROUP_ID", "")),
		NodeHost:           nodeHost,
		HTTPMode:           envValue("FUGUE_EDGE_FRONT_HTTP_MODE", HTTPModeRedirect),
		ActiveSlotFile:     envValue("FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE", "/var/lib/fugue/edge-blue-green/active-slot"),
		DefaultSlot:        defaultSlot,
		DialTimeout:        envDuration("FUGUE_EDGE_FRONT_DIAL_TIMEOUT", 5*time.Second),
		ShutdownTimeout:    envDuration("FUGUE_EDGE_FRONT_SHUTDOWN_TIMEOUT", 10*time.Second),
		ProxyProtocol:      envBool("FUGUE_EDGE_FRONT_PROXY_PROTOCOL", true),
		ProcNetSNMPPath:    envValue("FUGUE_EDGE_FRONT_PROC_NET_SNMP_PATH", "/proc/net/snmp"),
		ProcNetNetstatPath: envValue("FUGUE_EDGE_FRONT_PROC_NET_NETSTAT_PATH", "/proc/net/netstat"),
		Slots: map[string]SlotTargets{
			"a": {HTTPAddress: targetAddress("FUGUE_EDGE_FRONT_SLOT_A_HTTP_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_A_HTTP_PORT", 18080), HTTPSAddress: targetAddress("FUGUE_EDGE_FRONT_SLOT_A_HTTPS_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_A_HTTPS_PORT", 18443)},
			"b": {HTTPAddress: targetAddress("FUGUE_EDGE_FRONT_SLOT_B_HTTP_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_B_HTTP_PORT", 28080), HTTPSAddress: targetAddress("FUGUE_EDGE_FRONT_SLOT_B_HTTPS_ADDR", nodeHost, "FUGUE_EDGE_FRONT_SLOT_B_HTTPS_PORT", 28443)},
		},
	}
}

func targetAddress(addressEnv, nodeHost, portEnv string, defaultPort int) string {
	if value := strings.TrimSpace(os.Getenv(addressEnv)); value != "" {
		return value
	}
	port := defaultPort
	if parsed, err := strconv.Atoi(strings.TrimSpace(os.Getenv(portEnv))); err == nil && parsed > 0 && parsed <= 65535 {
		port = parsed
	}
	host := strings.TrimSpace(nodeHost)
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func envValue(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("invalid duration in %s=%q, using fallback %s", key, value, fallback)
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		log.Printf("invalid boolean in %s=%q, using fallback %v", key, value, fallback)
		return fallback
	}
	return parsed
}

func envFileValue(values map[string]string, key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	if value := strings.TrimSpace(values[key]); value != "" {
		return value
	}
	return strings.TrimSpace(fallback)
}

func readEnvFile(path string) map[string]string {
	file, err := os.Open(strings.TrimSpace(path))
	if err != nil {
		return nil
	}
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
			value = value[1 : len(value)-1]
		}
		values[key] = value
	}
	return values
}
