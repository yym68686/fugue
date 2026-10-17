package store

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

// ListImageReplicasByImageIDs applies the ordinary replica filters to a bounded
// set of image identities in one read. It never expands an empty input into an
// unrestricted scan, and returns the same per-image ordering as ListImageReplicas.
func (s *Store) ListImageReplicasByImageIDs(filter model.ImageReplicaFilter, imageIDs []string) (map[string][]model.ImageReplica, error) {
	filter = normalizeImageReplicaFilter(filter)
	ids := make(map[string]struct{}, len(imageIDs))
	for _, id := range imageIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			ids[id] = struct{}{}
		}
	}
	out := make(map[string][]model.ImageReplica, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		clauses, args := imageReplicaFilterClauses(filter)
		// One array parameter avoids PostgreSQL's parameter-count limit for
		// large image histories. All other filters retain their existing scope.
		args = append(args, sortedTrimmedStringKeys(ids))
		clauses = append(clauses, "image_id = ANY($"+strconv.Itoa(len(args))+"::text[])")
		query := "SELECT " + imageReplicaColumns() + " FROM fugue_image_replicas WHERE " + strings.Join(clauses, " AND ") + " ORDER BY COALESCE(last_verified_at, updated_at) DESC, updated_at DESC"
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, mapDBErr(err)
		}
		defer rows.Close()
		for rows.Next() {
			replica, err := scanImageReplica(rows)
			if err != nil {
				return nil, mapDBErr(err)
			}
			out[replica.ImageID] = append(out[replica.ImageID], replica)
		}
		if err := rows.Err(); err != nil {
			return nil, mapDBErr(err)
		}
		return out, nil
	}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, replica := range state.ImageReplicas {
			id := strings.TrimSpace(replica.ImageID)
			if _, selected := ids[id]; selected && imageReplicaMatchesFilter(replica, filter) {
				out[id] = append(out[id], replica)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, replicas := range out {
		sort.Slice(replicas, func(i, j int) bool {
			left, right := imageReplicaSeenAt(replicas[i]), imageReplicaSeenAt(replicas[j])
			if !left.Equal(right) {
				return left.After(right)
			}
			return replicas[i].UpdatedAt.After(replicas[j].UpdatedAt)
		})
	}
	return out, nil
}
