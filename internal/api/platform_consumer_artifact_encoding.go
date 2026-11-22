package api

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

// Limit concurrent encoders and retained scratch independently. Contended
// requests still receive the original identity representation. Never pool a
// response, authenticated payload, or transport destination.
var signedDNSGzipActive = make(chan struct{}, 2)
var signedDNSGzipIdle = make(chan *gzip.Writer, 2)

// Called only after the complete per-request authorization, artifact/input
// checks and final owner recheck. No cache hits, partial disclosure or 304s.
func writeAssignedConsumerArtifact(w http.ResponseWriter, r *http.Request, result consumerArtifactLookup) {
	tag := strconv.Quote(result.Artifact.ContentHash)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("ETag", tag)
	if result.Artifact.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
		w.Header().Add("Vary", "Accept-Encoding")
		// Exactly the opt-in produced by Go's default HTTP transport. Complex
		// preference lists, duplicates and exclusions use legacy behavior.
		values := r.Header.Values("Accept-Encoding")
		if len(values) == 1 && strings.EqualFold(strings.TrimSpace(values[0]), "gzip") && r.Header.Get("Range") == "" {
			select {
			case signedDNSGzipActive <- struct{}{}:
				defer func() { <-signedDNSGzipActive }()
				var z *gzip.Writer
				select {
				case z = <-signedDNSGzipIdle:
					z.Reset(w)
				default:
					z, _ = gzip.NewWriterLevel(w, gzip.BestSpeed)
				}
				defer func() {
					z.Reset(io.Discard)
					select {
					case signedDNSGzipIdle <- z:
					default:
					}
				}()
				w.Header().Set("ETag", "W/"+tag)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Encoding", "gzip")
				w.WriteHeader(http.StatusOK)
				if err := json.NewEncoder(z).Encode(result); err == nil {
					_ = z.Close()
				}
				return
			default:
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}
