package dnsserver

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
)

func TestRuntimeFactsEncodingNegotiation(t *testing.T) {
	for _, tc := range []struct {
		values []string
		gzip   bool
	}{
		{nil, false}, {[]string{"identity"}, false}, {[]string{"br"}, false},
		{[]string{"gzip"}, true}, {[]string{"br, GZip; q=0.5"}, true},
		{[]string{"gzip;q=0"}, false}, {[]string{"gzip;q=0.000"}, false},
		{[]string{"gzip;q=0.001"}, true}, {[]string{"gzip;q=1.000"}, true},
		{[]string{"*"}, true}, {[]string{"*;q=0"}, false},
		{[]string{"*;q=1, gzip;q=0"}, false}, {[]string{"*;q=0, gzip;q=1"}, true},
		{[]string{"gzip;q=1", "gzip;q=0"}, false},
		{[]string{"gzip;q=0", "gzip;q=1"}, false},
		{[]string{"gzip;q=NaN"}, false}, {[]string{"gzip;q=1.001"}, false},
		{[]string{"gzip;q=-1"}, false}, {[]string{"gzip;q=0.1234"}, false},
		{[]string{"gzip;q=1e0"}, false}, {[]string{"gzip;q=1;q=0"}, false},
		{[]string{"gzip;q="}, false}, {[]string{"gzip;unknown=1"}, false},
	} {
		t.Run(strings.Join(tc.values, "/"), func(t *testing.T) {
			if got := acceptsRuntimeFactsGzip(http.Header{"Accept-Encoding": tc.values}); got != tc.gzip {
				t.Fatalf("headers %q: gzip=%v, want %v", tc.values, got, tc.gzip)
			}
		})
	}
}

func TestRuntimeFactsGzipPoolPreservesConcurrentResponses(t *testing.T) {
	var workers sync.WaitGroup
	for worker := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := range 8 {
				raw := []byte(strings.Repeat(fmt.Sprintf("proof-%d-%d;", worker, iteration), 8192))
				encoded, err := gzipRuntimeFacts(raw)
				if err != nil {
					t.Error(err)
					return
				}
				// Another call may reuse the writer while this response is retained.
				if _, err := gzipRuntimeFacts([]byte("another observation")); err != nil {
					t.Error(err)
					return
				}
				reader, err := gzip.NewReader(bytes.NewReader(encoded))
				if err != nil {
					t.Error(err)
					return
				}
				decoded, err := io.ReadAll(reader)
				closeErr := reader.Close()
				if err != nil || closeErr != nil || !bytes.Equal(decoded, raw) || len(encoded) >= len(raw)/2 {
					t.Errorf("corrupt or ineffective compression: raw=%d encoded=%d read=%v close=%v", len(raw), len(encoded), err, closeErr)
					return
				}
			}
		}()
	}
	workers.Wait()
}

func TestDNSRuntimeFactsGzipThroughHTTPProxyRetainsFreshVerification(t *testing.T) {
	s, state, _ := runtimeFactsFixture(t)
	done := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Handler().ServeHTTP(w, r)
		done <- struct{}{}
	}))
	defer backend.Close()
	target, _ := url.Parse(backend.URL)
	proxyTransport := &http.Transport{}
	defer proxyTransport.CloseIdleConnections()
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = proxyTransport
	front := httptest.NewServer(proxy)
	defer front.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	read := func(encoding string, wantStatus int, compressed bool) dnsfacts.Snapshot {
		t.Helper()
		request, _ := http.NewRequest(http.MethodGet, front.URL+"/runtime-facts", nil)
		if encoding != "" {
			request.Header.Set("Accept-Encoding", encoding)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(response.Body)
		response.Body.Close()
		<-done
		if err != nil || response.StatusCode != wantStatus || response.Uncompressed != compressed {
			t.Fatalf("status=%d decompressed=%v err=%v", response.StatusCode, response.Uncompressed, err)
		}
		var snapshot dnsfacts.Snapshot
		if wantStatus == http.StatusOK {
			if response.Header.Get("Content-Type") != "application/json" || response.Header.Get("Cache-Control") != "private, no-store" || response.Header.Get("Vary") != "Accept-Encoding" {
				t.Fatal("encoding changed response metadata", response.Header)
			}
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				t.Fatal(err)
			}
		}
		return snapshot
	}
	compressed := read("", http.StatusOK, true) // Go negotiates and decodes gzip.
	identity := read("identity", http.StatusOK, false)
	if !compressed.Ready || !reflect.DeepEqual(identity.Facts, compressed.Facts) || !reflect.DeepEqual(identity.Assignment, compressed.Assignment) || identity.ObservedAt != compressed.ObservedAt || identity.CheckpointValidUntil != compressed.CheckpointValidUntil {
		t.Fatal("compression renewed or changed observation authority")
	}
	read("gzip;q=0", http.StatusOK, false)
	s.listenerFailed.Store(true)
	if read("", http.StatusOK, true).Ready {
		t.Fatal("compression hid a failed listener")
	}
	s.listenerFailed.Store(false)
	expired := *state
	expired.facts = append([]dnsReadinessFact(nil), state.facts...)
	for i := range expired.facts {
		expired.facts[i].Proof.ValidUntil = time.Now().Add(-time.Second)
	}
	s.platformServing.Store(&expired)
	if read("", http.StatusOK, true).Ready {
		t.Fatal("compression renewed expired evidence")
	}
	s.platformServing.Store(state)
	s.Config.BundleRevokedKeyIDs = []string{"key"}
	read("", http.StatusServiceUnavailable, false)
	s.Config.BundleRevokedKeyIDs = nil
	s.platformServing.Store(nil)
	read("", http.StatusServiceUnavailable, false)
}
