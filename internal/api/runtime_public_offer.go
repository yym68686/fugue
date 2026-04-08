package api

import (
	"net/http"
	"strconv"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleSetRuntimePublicOffer(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.write scope")
		return
	}
	runtimeObj, ok := s.runtimeSharingOwner(w, principal, r.PathValue("id"))
	if !ok {
		return
	}

	var req struct {
		ReferenceBundle                 model.BillingResourceSpec `json:"reference_bundle"`
		ReferenceMonthlyPriceMicroCents int64                     `json:"reference_monthly_price_microcents"`
		Free                            bool                      `json:"free"`
		FreeCPU                         bool                      `json:"free_cpu"`
		FreeMemory                      bool                      `json:"free_memory"`
		FreeStorage                     bool                      `json:"free_storage"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	updatedRuntime, err := s.store.SetRuntimePublicOffer(runtimeObj.ID, runtimeObj.TenantID, model.RuntimePublicOffer{
		ReferenceBundle:                 req.ReferenceBundle,
		ReferenceMonthlyPriceMicroCents: req.ReferenceMonthlyPriceMicroCents,
		Free:                            req.Free,
		FreeCPU:                         req.FreeCPU,
		FreeMemory:                      req.FreeMemory,
		FreeStorage:                     req.FreeStorage,
		PriceBook: model.BillingPriceBook{
			Currency:      model.DefaultBillingCurrency,
			HoursPerMonth: model.DefaultBillingHoursPerMonth,
		},
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "runtime.share.public_offer", "runtime", updatedRuntime.ID, updatedRuntime.TenantID, map[string]string{
		"free":                               boolString(updatedRuntime.PublicOffer != nil && updatedRuntime.PublicOffer.Free),
		"reference_monthly_price_microcents": int64String(publicOfferMonthlyPrice(updatedRuntime.PublicOffer)),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runtime": updatedRuntime})
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func int64String(value int64) string {
	return strconv.FormatInt(value, 10)
}

func publicOfferMonthlyPrice(offer *model.RuntimePublicOffer) int64 {
	if offer == nil {
		return 0
	}
	return offer.ReferenceMonthlyPriceMicroCents
}
