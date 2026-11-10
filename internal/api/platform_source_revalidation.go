package api

import (
	"net/http"
	"time"

	"fugue/internal/dnsroutewire"
	"fugue/internal/model"
)

// Only call after all current authorization and input-admission checks. The
// validator covers actual response content, including envelopes and LKG, rather
// than trusting a claimed selection/content hash. Only observation time varies.
func revalidateDNSSourceResponse(w http.ResponseWriter, r *http.Request, response model.PlatformConsumerDNSRouteSourcesResponse, retained *model.PlatformLKGSnapshot) bool {
	observed := response.Snapshot.ObservedAt
	etag, err := dnsroutewire.Validator(response, retained)
	if err != nil {
		return false
	}
	w.Header().Set("ETag", etag)
	w.Header().Set(dnsroutewire.ObservedAtHeader, observed.UTC().Format(time.RFC3339Nano))
	if r.Header.Get("If-None-Match") == etag && !observed.IsZero() {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}
