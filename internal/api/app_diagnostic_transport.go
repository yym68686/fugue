package api

import "net/http"

// Diagnostic clients must not keep observing an old Pod through a cached
// connection after its Service endpoints change. Repeated probes can otherwise
// keep that connection alive indefinitely and prevent safe workload draining.
// DisableKeepAlives closes only completed responses; it does not interrupt a
// response stream that the caller is still reading.
func newAppDiagnosticHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	return &http.Client{Transport: transport}
}
