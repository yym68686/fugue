package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"fugue/internal/model"
)

// The billing snapshot is read inside the ledger transaction. Combining its
// independent relations avoids six inter-region round trips without caching
// balances or changing the transaction's repeatable-read snapshot.
func (s *Store) pgLoadBillingSummaryInputsTx(ctx context.Context, tx *sql.Tx, ids []string) (model.State, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `
SELECT jsonb_build_object(
  'tenants', COALESCE((SELECT jsonb_agg(jsonb_build_object('id', id)) FROM fugue_tenants WHERE id = ANY($1::text[])), '[]'::jsonb),
  'apps', COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY a.created_at) FROM (
    SELECT id, tenant_id, project_id, name, description,
      CASE WHEN jsonb_typeof(spec_json) = 'object'
        THEN spec_json - ARRAY['env','files','generated_env','command','args']
        ELSE spec_json END AS spec,
      status_json AS status, created_at, updated_at
    FROM fugue_apps WHERE tenant_id = ANY($1::text[])
  ) a), '[]'::jsonb),
  'backing_services', COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY s.created_at) FROM (
    SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json AS spec,
      current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
    FROM fugue_backing_services WHERE tenant_id = ANY($1::text[])
  ) s), '[]'::jsonb),
  'service_bindings', COALESCE((SELECT jsonb_agg(to_jsonb(b) ORDER BY b.created_at) FROM (
    SELECT id, tenant_id, app_id, service_id, alias, env_json AS env, created_at, updated_at
    FROM fugue_service_bindings WHERE tenant_id = ANY($1::text[])
  ) b), '[]'::jsonb),
  'runtimes', COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.created_at) FROM (
    SELECT id, tenant_id, name, type, access_mode, public_offer_json AS public_offer, created_at, updated_at
    FROM fugue_runtimes
  ) r), '[]'::jsonb),
  'billing_events', COALESCE((SELECT jsonb_agg(to_jsonb(e)) FROM unnest($1::text[]) AS requested(tenant_id)
    CROSS JOIN LATERAL (
      SELECT id, tenant_id, type, amount_microcents, balance_after_microcents, metadata_json AS metadata, created_at
      FROM fugue_billing_events WHERE tenant_id = requested.tenant_id
      ORDER BY created_at DESC, id DESC LIMIT $2
    ) e), '[]'::jsonb)
)`, ids, billingHistoryLimit).Scan(&raw)
	if err != nil {
		return model.State{}, fmt.Errorf("read billing snapshot inputs: %w", err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		return model.State{}, fmt.Errorf("decode billing snapshot inputs: %w", err)
	}
	for i := range state.Apps {
		model.ApplyAppSpecDefaults(&state.Apps[i].Spec)
	}
	for i := range state.Runtimes {
		runtime := &state.Runtimes[i]
		runtime.AccessMode = normalizeRuntimeAccessMode(runtime.Type, runtime.AccessMode)
		offer := model.RuntimePublicOffer{}
		if runtime.PublicOffer != nil {
			offer = *runtime.PublicOffer
		}
		offer.PriceBook = normalizeRuntimePublicOfferPriceBook(offer.PriceBook)
		runtime.PublicOffer = &offer
	}
	return state, nil
}
