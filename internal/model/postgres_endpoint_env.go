package model

import (
	"net"
	"net/url"
	"strings"
)

// RebindPostgresEndpointEnvironment keeps a replaced managed cluster's known
// connection hosts on its durable endpoint. It changes no credentials, database,
// port or URL options, and never infers ownership of an external hostname.
// The caller's environment remains untouched.
func RebindPostgresEndpointEnvironment(env map[string]string, pg AppPostgresSpec, namespace string) map[string]string {
	endpoint := strings.TrimSpace(pg.EndpointServiceName)
	cluster := strings.TrimSpace(pg.ServiceName)
	if len(env) == 0 || endpoint == "" || cluster == "" || endpoint == cluster {
		return env
	}
	out := env
	changed := false
	set := func(key, value string) {
		if value == env[key] {
			return
		}
		if !changed {
			out = make(map[string]string, len(env))
			for k, v := range env {
				out[k] = v
			}
			changed = true
		}
		out[key] = value
	}
	for _, key := range []string{"DB_HOST", "PGHOST", "POSTGRES_HOST", "DATABASE_HOST"} {
		value, ok := env[key]
		if !ok {
			continue
		}
		host, port, err := net.SplitHostPort(value)
		if err == nil {
			if next := rebindPostgresEndpointHost(host, endpoint, cluster, namespace); next != host {
				set(key, net.JoinHostPort(next, port))
			}
		} else {
			set(key, rebindPostgresEndpointHost(value, endpoint, cluster, namespace))
		}
	}
	for _, key := range []string{"DATABASE_URL", "DB_URL", "POSTGRES_URL", "POSTGRESQL_URL"} {
		value, ok := env[key]
		if !ok {
			continue
		}
		u, err := url.Parse(value)
		if err != nil {
			continue
		}
		scheme := strings.ToLower(strings.SplitN(u.Scheme, "+", 2)[0])
		if scheme != "postgres" && scheme != "postgresql" {
			continue
		}
		host := u.Hostname()
		if next := rebindPostgresEndpointHost(host, endpoint, cluster, namespace); next != host {
			port := u.Port()
			u.Host = next
			if port != "" {
				u.Host = net.JoinHostPort(next, port)
			}
			set(key, u.String())
		}
	}
	return out
}

func rebindPostgresEndpointHost(host, endpoint, cluster, namespace string) string {
	suffixes := []string{""}
	if namespace = strings.TrimSpace(namespace); namespace != "" {
		suffixes = append(suffixes, "."+namespace, "."+namespace+".svc", "."+namespace+".svc.cluster.local")
	}
	for _, suffix := range suffixes {
		for _, name := range []string{cluster, PostgresRWServiceName(cluster), endpoint, PostgresRWServiceName(endpoint)} {
			candidate := name + suffix
			if strings.EqualFold(host, candidate) {
				return endpoint + suffix
			}
			if suffix != "" && strings.EqualFold(host, candidate+".") {
				return endpoint + suffix + "."
			}
		}
	}
	return host
}
