package edge

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestProxyResponseWritersPreserveFinalStatusAfterContinueAndEarlyHints(t *testing.T) {
	for _, cache := range []bool{false, true} {
		name := "observation"
		if cache {
			name = "cache capture"
		}
		t.Run(name, func(t *testing.T) {
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Link", "</style.css>; rel=preload")
				w.WriteHeader(http.StatusEarlyHints)
				// Reading the body sends 100 Continue from the real net/http server.
				b, err := io.ReadAll(r.Body)
				if err != nil || len(b) == 0 {
					t.Error("request body lost", err)
				}
				w.Header().Set("X-Final-Response", "created")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte("created"))
			}))
			defer origin.Close()
			target, _ := url.Parse(origin.URL)
			proxy := httputil.NewSingleHostReverseProxy(target)
			observed := make(chan int, 1)
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if cache {
					capture := newEdgeHTTPCacheCapture(w, 1024)
					proxy.ServeHTTP(capture, r)
					observed <- capture.statusCode
					return
				}
				capture := newEdgeProxyObservationResponseWriter(w, time.Now(), &edgeProxyObservation{})
				proxy.ServeHTTP(capture, r)
				observed <- capture.statusCode()
			}))
			defer front.Close()
			request, _ := http.NewRequest(http.MethodPost, front.URL, strings.NewReader(strings.Repeat("x", 1<<20)))
			request.Header.Set("Expect", "100-continue")
			client := &http.Client{Timeout: 5 * time.Second}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			if response.StatusCode != http.StatusCreated || string(body) != "created" || response.Header.Get("X-Final-Response") != "created" {
				t.Fatalf("informational response replaced final status: code=%d body=%q header=%q", response.StatusCode, body, response.Header.Get("X-Final-Response"))
			}
			if status := <-observed; status != http.StatusCreated {
				t.Fatalf("recorded informational status as final: %d", status)
			}
		})
	}
}
