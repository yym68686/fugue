package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
)

// Readiness still reads every identity, health field and SHA-256 of the actual
// node JSON in its repeatable-read transaction. Only unchanged bytes are reused;
// no readiness result, membership, fence, timestamp or authorization is cached.
var edgeReadinessBodies = newObservationPageBodyCache(4<<20, 4096)

const edgeReadinessIdentityColumns = "edge_id, edge_group_id, slot, instance_uid, release_epoch"

var edgeReadinessReferencesSQL = "SELECT " + strings.Replace(edgeInstanceSelectColumns, "node_json,", "octet_length(convert_to(node_json::text,'UTF8')), encode(sha256(convert_to(node_json::text,'UTF8')),'hex'),", 1) + " FROM fugue_edge_node_instances ORDER BY edge_group_id, edge_id, slot, instance_uid, release_epoch"

type edgeReadinessReference struct {
	instance model.EdgeNodeInstance
	size     int
	hash     string
	body     []byte
}

// db must be the caller's fixed repeatable-read snapshot. Body refetches cannot
// mix a newer node with earlier membership or health fields.
func pgReadEdgeReadinessInstances(ctx context.Context, db sqlQueryer, cache *observationPageBodyCache) ([]model.EdgeNodeInstance, error) {
	rows, err := db.QueryContext(ctx, edgeReadinessReferencesSQL)
	if err != nil {
		return nil, fmt.Errorf("read edge readiness references: %w", err)
	}
	defer rows.Close()
	refs := []*edgeReadinessReference{}
	missing := []*edgeReadinessReference{}
	for rows.Next() {
		ref := &edgeReadinessReference{}
		instance := &ref.instance
		var healthSince, heartbeat sql.NullTime
		if err := rows.Scan(&instance.EdgeID, &instance.EdgeGroupID, &instance.Slot, &instance.InstanceUID, &instance.ReleaseEpoch,
			&ref.size, &ref.hash, &instance.FailureClass, &instance.EffectiveHealthy, &instance.ConsecutiveHealthy,
			&instance.ConsecutiveUnhealthy, &healthSince, &heartbeat, &instance.CreatedAt, &instance.UpdatedAt); err != nil {
			return nil, err
		}
		hash, err := hex.DecodeString(ref.hash)
		if err != nil || len(hash) != sha256.Size || hex.EncodeToString(hash) != ref.hash || ref.size < 0 {
			return nil, fmt.Errorf("invalid edge readiness body reference")
		}
		if healthSince.Valid {
			instance.HealthStateSince = healthSince.Time.UTC()
		}
		if heartbeat.Valid {
			instance.LastHeartbeatAt = heartbeat.Time.UTC()
		}
		instance.CreatedAt, instance.UpdatedAt = instance.CreatedAt.UTC(), instance.UpdatedAt.UTC()
		if body, ok := cache.get(ref.hash, ref.hash); ok && len(body) == ref.size {
			ref.body = body
		} else {
			missing = append(missing, ref)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for first := 0; first < len(missing); first += 128 {
		if err := pgReadEdgeReadinessBodies(ctx, db, missing[first:min(first+128, len(missing))], cache); err != nil {
			return nil, err
		}
	}
	instances := make([]model.EdgeNodeInstance, 0, len(refs))
	for _, ref := range refs {
		// Decode into a new value on every use, including cache hits, so caller
		// mutations never affect another request and malformed data still fails.
		if err := json.Unmarshal(ref.body, &ref.instance.Node); err != nil {
			return nil, fmt.Errorf("decode edge readiness node: %w", err)
		}
		instance, err := normalizeStoredEdgeNodeInstance(ref.instance)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

func pgReadEdgeReadinessBodies(ctx context.Context, db sqlQueryer, refs []*edgeReadinessReference, cache *observationPageBodyCache) error {
	args := make([]any, 0, 5*len(refs))
	tuples := make([]string, 0, len(refs))
	pending := make(map[string]*edgeReadinessReference, len(refs))
	for _, ref := range refs {
		key := edgeNodeInstanceKey(ref.instance)
		if _, exists := pending[key]; exists {
			return fmt.Errorf("duplicate edge readiness identity")
		}
		pending[key] = ref
		tuples = append(tuples, "("+sqlPlaceholderList(len(args)+1, 5)+")")
		i := ref.instance
		args = append(args, i.EdgeID, i.EdgeGroupID, i.Slot, i.InstanceUID, i.ReleaseEpoch)
	}
	rows, err := db.QueryContext(ctx, "SELECT "+edgeReadinessIdentityColumns+", node_json::text FROM fugue_edge_node_instances WHERE ("+edgeReadinessIdentityColumns+") IN ("+strings.Join(tuples, ",")+")", args...)
	if err != nil {
		return fmt.Errorf("read edge readiness bodies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var identity model.EdgeNodeInstance
		var body []byte
		if err := rows.Scan(&identity.EdgeID, &identity.EdgeGroupID, &identity.Slot, &identity.InstanceUID, &identity.ReleaseEpoch, &body); err != nil {
			return err
		}
		key := edgeNodeInstanceKey(identity)
		ref, ok := pending[key]
		if !ok {
			return fmt.Errorf("unexpected edge readiness body")
		}
		hash := sha256.Sum256(body)
		if len(body) != ref.size || hex.EncodeToString(hash[:]) != ref.hash {
			return fmt.Errorf("edge readiness body differs from fresh reference")
		}
		ref.body = body
		cache.put(ref.hash, ref.hash, body)
		delete(pending, key)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(pending) != 0 {
		return fmt.Errorf("edge readiness body missing")
	}
	return rows.Close()
}
