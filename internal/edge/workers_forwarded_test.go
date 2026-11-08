package edge

import (
	"net/http/httptest"
	"net/http/httputil"
	"testing"
)

func TestEdgePreservesPublicSchemeAcrossPrivateHop(t *testing.T) {
	for _, scheme := range []string{"https", "http", "malformed"} {
		in := httptest.NewRequest("GET", "http://internal.example/path", nil)
		in.Header.Set("X-Forwarded-Proto", scheme)
		out := in.Clone(in.Context())
		setEdgeXForwarded(&httputil.ProxyRequest{In: in, Out: out})
		want := scheme
		if scheme == "malformed" {
			want = "http"
		}
		if got := out.Header.Get("X-Forwarded-Proto"); got != want {
			t.Fatalf("scheme=%s forwarded=%s", scheme, got)
		}
	}
}
