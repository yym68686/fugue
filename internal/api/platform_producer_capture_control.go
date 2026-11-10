package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

const maxProducerCaptureControlBytes = 2048

type producerCaptureControlUpdate func(context.Context, platformproducer.CaptureDeferralControlRequest, model.Principal) (store.ProducerCaptureDeferralReceipt, error)

func (s *Server) handleUpdateProducerCaptureDeferral(w http.ResponseWriter, r *http.Request) {
	serveProducerCaptureControl(w, r, s.store.UpdateProducerCaptureDeferral)
}

// This transport can only address an existing control. It has no prepare,
// installer or enrollment callback, and cannot supply availability authority.
func serveProducerCaptureControl(w http.ResponseWriter, r *http.Request, update producerCaptureControlUpdate) {
	w.Header().Set("Cache-Control", "private, no-store")
	p := mustPrincipal(r) // the generated route uses the normal API authenticator
	if !p.IsPlatformAdmin() || !p.HasExplicitScope("producer_capture.control") || p.ActorType == "" || p.ActorID == "" {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with explicit producer_capture.control required")
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		httpx.WriteError(w, http.StatusUnsupportedMediaType, "application/json required")
		return
	}
	if r.URL.RawQuery != "" {
		httpx.WriteError(w, http.StatusBadRequest, "control accepts no query parameters")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxProducerCaptureControlBytes)
	request, err := decodeProducerCaptureControl(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			httpx.WriteError(w, http.StatusRequestEntityTooLarge, "control body exceeds 2048 bytes")
		} else {
			httpx.WriteError(w, http.StatusBadRequest, "invalid capture control request")
		}
		return
	}
	receipt, err := update(r.Context(), request, p)
	if err != nil {
		// Do not serialize a partial receipt or a raw database error. In
		// particular a failed/uncertain commit never becomes HTTP success.
		switch {
		case errors.Is(err, sql.ErrNoRows), errors.Is(err, store.ErrNotFound):
			httpx.WriteError(w, http.StatusNotFound, "capture control not found")
		case errors.Is(err, store.ErrConflict):
			httpx.WriteError(w, http.StatusConflict, "capture control target conflict")
		case errors.Is(err, store.ErrInvalidInput):
			httpx.WriteError(w, http.StatusBadRequest, "invalid capture control request")
		default:
			httpx.WriteError(w, http.StatusServiceUnavailable, "capture control transaction unavailable or uncertain")
		}
		return
	}
	if r.Context().Err() != nil || receipt.Control != request || receipt.Record.Request.OperationID != request.OperationID || receipt.Record.RequestDigest != request.RequestDigest || receipt.Record.Validate() != nil || receipt.Version < 1 || receipt.ActorType != p.ActorType || receipt.ActorID != p.ActorID {
		httpx.WriteError(w, http.StatusServiceUnavailable, "capture control transaction receipt unavailable")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, receipt)
}

// Four exact string fields only. DecodeJSON alone permits duplicate keys and
// alternate case field names, which are ambiguous for an administrative target.
func decodeProducerCaptureControl(r io.Reader) (platformproducer.CaptureDeferralControlRequest, error) {
	var request platformproducer.CaptureDeferralControlRequest
	d := json.NewDecoder(r)
	start, err := d.Token()
	if err != nil {
		return request, err
	}
	if start != json.Delim('{') {
		return request, fmt.Errorf("control must be an object")
	}
	fields := map[string]*string{"schema_version": &request.SchemaVersion, "operation_id": &request.OperationID, "request_digest": &request.RequestDigest, "action": &request.Action}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return request, err
		}
		name, ok := key.(string)
		field := fields[name]
		if !ok || field == nil || seen[name] {
			return request, fmt.Errorf("unknown or repeated control field")
		}
		value, err := d.Token()
		if err != nil {
			return request, err
		}
		text, ok := value.(string)
		if !ok {
			return request, fmt.Errorf("control fields must be strings")
		}
		*field, seen[name] = text, true
	}
	if end, err := d.Token(); err != nil {
		return request, err
	} else if end != json.Delim('}') {
		return request, fmt.Errorf("invalid control object")
	}
	if _, err := d.Token(); err != io.EOF {
		if err != nil {
			return request, err
		}
		return request, fmt.Errorf("control must be one object")
	}
	return request, request.Validate()
}
