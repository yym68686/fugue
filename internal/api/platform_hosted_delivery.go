package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"fugue/internal/store"
	"fugue/internal/tlsissuance"
)

func (s *Server) handleGetPlatformConsumerHostedSelection(w http.ResponseWriter, r *http.Request) {
	s.readPlatformConsumerHosted(w, r, false)
}

func (s *Server) handleListPlatformConsumerHostedSelections(w http.ResponseWriter, r *http.Request) {
	s.readPlatformConsumerHosted(w, r, true)
}

func (s *Server) handleRecordPlatformConsumerHostedReceipt(w http.ResponseWriter, r *http.Request) {
	s.readPlatformConsumerHosted(w, r, false)
}

func (s *Server) readPlatformConsumerHosted(w http.ResponseWriter, r *http.Request, list bool) {
	w.Header().Set("Cache-Control", "private, no-store")
	who, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok || who.ScopeKey != pc.GlobalScopeKey || who.Component != model.PlatformConsumerComponentDNSServer && who.Component != model.PlatformConsumerComponentEdgeWorker {
		httpx.WriteError(w, 403, "global physical component identity required")
		return
	}
	host, id := r.PathValue("hostname"), r.PathValue("artifact_id")
	set := r.URL.Query()["expected_consumer_set_id"]
	query := r.URL.Query()
	cursor := query["after"]
	if !list && !tlsissuance.ValidHostname(host) || id == "" || len(set) != 1 || set[0] == "" || len(cursor) > 0 && (!list || len(cursor) != 1 || !tlsissuance.ValidHostname(cursor[0])) || len(query) != 1+len(cursor) {
		httpx.WriteError(w, 400, "exact hostname, child and expected set required")
		return
	}
	ctx := r.Context()
	var observation routeartifact.HostedObservation
	post := r.Method == http.MethodPost
	if post {
		// A node token alone does not identify the serving DNS process
		// behind the public Service; even a negative can invalidate A.
		if who.Component == model.PlatformConsumerComponentDNSServer && !strings.HasPrefix(who.CredentialID, "kubernetes:") {
			httpx.WriteError(w, 403, "scoped DNS receipt requires a bound serving Pod")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
		if list || httpx.DecodeJSON(r, &observation) != nil || observation.Host != host || observation.Member != who.ConsumerID() || observation.Fence < 1 || observation.Decision == "" {
			httpx.WriteError(w, 400, "invalid scoped observation")
			return
		}
	}
	items, _, err := s.readCheckedConsumerAssignments(ctx, who, true, time.Now().UTC(), s.store.NewPlatformArtifactReadSession)
	if err != nil {
		httpx.WriteError(w, 503, "serving assignment unavailable")
		return
	}
	var selected *consumerArtifactLookup
	for i := range items {
		a := items[i].Assignment
		if a.ArtifactID == id && a.ExpectedConsumerSetID == set[0] && a.ReleaseChannel == model.PlatformArtifactReleaseChannelFull {
			if selected != nil {
				httpx.WriteError(w, 503, "ambiguous serving assignment")
				return
			}
			selected = &items[i]
		}
	}
	if selected == nil {
		httpx.WriteError(w, 404, "no hosted selection for this recipient")
		return
	}
	if list {
		page, e := s.store.ListHostedMemberSelections(ctx, who, selected.Assignment, query.Get("after"))
		if e != nil {
			httpx.WriteError(w, 503, "host selections unavailable")
			return
		}
		load := s.store.NewPlatformArtifactReadSessionContext(ctx)
		if e = s.checkConsumerInputReadsWithLoader(ctx, who, []consumerArtifactLookup{*selected}, newConsumerArtifactReader(load), true, load); e != nil || who.ExpiresAtUnix <= time.Now().Unix() {
			httpx.WriteError(w, 503, "serving membership changed")
			return
		}
		httpx.WriteJSON(w, 200, page)
		return
	}
	bind := func(in store.ProjectHostRouteInputs, p pc.HostedScopeProposal) (string, error) {
		b, e := matchHostedProjectIntent(in, p)
		return b.Direction, e
	}
	if post {
		load := s.store.NewPlatformArtifactReadSessionContext(ctx)
		if err = s.checkConsumerInputReadsWithLoader(ctx, who, []consumerArtifactLookup{*selected}, newConsumerArtifactReader(load), true, load); err != nil {
			httpx.WriteError(w, 409, "serving input dependencies changed")
			return
		}
		h := platformcontrol.PlatformConsumerHeartbeatEnvelope{}
		if observation.Failure == "" {
			h.ApplyStatus, h.ProbeStatus = "applied", "passed"
		}
		if status, _ := s.validateDNSHeartbeatBackendSelection(ctx, who, h); status != http.StatusOK {
			httpx.WriteError(w, status, "reporter is not the selected DNS backend")
			return
		}
		physicalCtx, ok := s.admitPhysicalWorker(w, r, who, observation.Failure == "")
		if !ok {
			return
		}
		if err = s.store.RecordHostedMemberObservation(physicalCtx, host, who, selected.Assignment, observation, bind); err != nil {
			if errors.Is(err, store.ErrWorkerReporterChanged) {
				writeWorkerTransportUnavailable(w)
				return
			}
			s.writeStoreError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	p, err := s.store.ReadHostedMemberSelection(ctx, host, who, selected.Assignment, bind)
	if errors.Is(err, store.ErrNotFound) {
		httpx.WriteError(w, 404, "no hosted selection for this recipient")
		return
	}
	if err != nil {
		httpx.WriteError(w, 503, "host selection dependencies changed")
		return
	}
	load := s.store.NewPlatformArtifactReadSessionContext(ctx)
	if err = s.checkConsumerInputReadsWithLoader(ctx, who, []consumerArtifactLookup{*selected}, newConsumerArtifactReader(load), true, load); err != nil {
		httpx.WriteError(w, 503, "serving input dependencies changed")
		return
	}
	if err = s.store.CheckHostedMemberSelection(ctx, p); err != nil || who.ExpiresAtUnix <= time.Now().Unix() {
		httpx.WriteError(w, 503, "hostname decision changed")
		return
	}
	a, err := routeartifact.SignHostedPacket(p, s.bundleKeyring(), time.Now().UTC())
	if err != nil {
		httpx.WriteError(w, 503, "hosted selection unavailable")
		return
	}
	httpx.WriteJSON(w, 200, a)
}
