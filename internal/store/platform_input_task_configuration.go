package store

import (
	"context"
	"encoding/json"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

// These tasks fetch DiscoveryBundle (whose platform routes read the global
// verified ReleaseSet), or restart the edge/DNS readers of that publication.
// Local resource/inventory tasks must not acquire an unrelated input barrier.
// Scoped reader assignments and independent cold-start admission remain
// separate entry points; this is not a claim of full reader coverage.
func compilerInputTaskUsesConfiguration(taskType string, payload any) bool {
	switch taskType {
	case model.NodeUpdateTaskTypeRefreshJoinConfig, model.NodeUpdateTaskTypeRefreshDesiredState,
		model.NodeUpdateTaskTypeReloadLKGBundle, model.NodeUpdateTaskTypeRestartK3SAgent,
		model.NodeUpdateTaskTypeUpgradeK3SAgent:
		return true
	case model.NodeUpdateTaskTypeRestartStatelessNodeService:
		var service string
		switch p := payload.(type) {
		case map[string]string:
			service = p["service"]
		case map[string]any:
			service, _ = p["service"].(string)
		}
		return service == "fugue-edge.service" || service == "fugue-dns.service" || service == "fugue-node-updater.timer"
	}
	return false
}

func compilerInputTaskConfigurationReferences(ctx context.Context, tx inputQueryer, taskType string, payload any) ([]inputarchive.HotReference, error) {
	if !compilerInputTaskUsesConfiguration(taskType, payload) {
		return nil, nil
	}
	refs := []inputarchive.HotReference{}
	rows, err := tx.QueryContext(ctx, `SELECT artifact_id,content_hash FROM fugue_platform_lkg_snapshots
 WHERE artifact_kind=$1 AND scope_key='global'`, model.PlatformArtifactKindReleaseSet)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, hash string
		if err := rows.Scan(&id, &hash); err != nil {
			return nil, err
		}
		refs = append(refs, inputarchive.HotReference{Kind: "artifact", ID: id, ExpectedHash: hash})
	}
	// References carry the selector's digest for graph hash validation as well
	// as the later physical selector witness. A replaced row cannot redirect
	// this same-snapshot task to an artifact with a different signed digest.
	return refs, rows.Err()
}

// Selector absence matters too: an initial empty installation can acquire an
// LKG between preparation and claim. A separate tuple witness (including an
// absent sentinel) binds the selection, not just the artifact it once selected.
func compilerInputTaskConfigurationSelectorKey() string {
	raw, _ := json.Marshal([]string{model.PlatformArtifactKindReleaseSet, "global"})
	return string(raw)
}
