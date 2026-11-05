package store

import (
	"context"

	"fugue/internal/inputarchive"
)

// Identity-only lookups share the same builders as individual resolution. A
// generation retains its exact kind and scope; no native generation is guessed.
// The caller caps the combined batch at 32 and owns the read-only RR snapshot.
func resolveCompilerInputHotLedgerBatch(ctx context.Context, db inputQueryer, generations, releases []inputarchive.HotReference, out map[inputarchive.HotReference]inputarchive.HotNode) error {
	return resolveCompilerInputHotLedgerBatchObserved(ctx, db, generations, releases, out, nil)
}

func resolveCompilerInputHotLedgerBatchObserved(ctx context.Context, db inputQueryer, generations, releases []inputarchive.HotReference, out map[inputarchive.HotReference]inputarchive.HotNode, cost *CompilerInputHotReadCost) error {
	if len(generations) > 0 {
		ids, kinds, scopes := []string{}, []string{}, []string{}
		for _, ref := range generations {
			ids, kinds, scopes = append(ids, ref.ID), append(kinds, ref.ArtifactKind), append(scopes, ref.ScopeKey)
		}
		rows, err := queryCompilerInputHotRows(ctx, db, cost, `SELECT r.generation,r.artifact_kind,r.scope_key,a.id,a.content_hash
 FROM unnest($1::text[],$2::text[],$3::text[]) AS r(generation,artifact_kind,scope_key)
 JOIN fugue_platform_artifacts a ON a.artifact_kind=r.artifact_kind AND a.scope_key=r.scope_key AND a.generation=r.generation`, ids, kinds, scopes)
		if err != nil {
			return err
		}
		for nextCompilerInputHotRow(rows, cost) {
			ref := inputarchive.HotReference{Kind: "generation"}
			var id, hash string
			if err = rows.Scan(&ref.ID, &ref.ArtifactKind, &ref.ScopeKey, &id, &hash); err != nil {
				break
			}
			out[ref] = compilerInputHotGenerationNode(ref, id, hash)
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	if len(releases) > 0 {
		ids := []string{}
		for _, ref := range releases {
			ids = append(ids, ref.ID)
		}
		rows, err := queryCompilerInputHotRows(ctx, db, cost, `SELECT id,artifact_id,artifact_kind,scope_key,generation,candidate_generation,
 serving_unverified_generation,verified_lkg_generation,pinned_rollback_generation,rollback_target_generation
 FROM fugue_platform_artifact_releases WHERE id=ANY($1::text[])`, ids)
		if err != nil {
			return err
		}
		for nextCompilerInputHotRow(rows, cost) {
			ref := inputarchive.HotReference{Kind: "release"}
			var artifact, kind, scope string
			generations := make([]string, 6)
			if err = rows.Scan(&ref.ID, &artifact, &kind, &scope, &generations[0], &generations[1], &generations[2], &generations[3], &generations[4], &generations[5]); err != nil {
				break
			}
			out[ref] = compilerInputHotReleaseNode(ref, artifact, kind, scope, generations)
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func compilerInputHotGenerationNode(ref inputarchive.HotReference, artifact, hash string) inputarchive.HotNode {
	ref.ExpectedHash = ""
	node := inputarchive.HotNode{Identity: ref, ContentHash: hash, References: []inputarchive.HotReference{}}
	if artifact != "" {
		node.References = append(node.References, inputarchive.HotReference{Kind: "artifact", ID: artifact})
	}
	return node
}

func compilerInputHotReleaseNode(ref inputarchive.HotReference, artifact, kind, scope string, generations []string) inputarchive.HotNode {
	ref.ExpectedHash = ""
	node := inputarchive.HotNode{Identity: ref, References: []inputarchive.HotReference{}}
	if artifact != "" {
		node.References = append(node.References, inputarchive.HotReference{Kind: "artifact", ID: artifact})
	}
	for _, generation := range generations {
		if generation != "" {
			node.References = append(node.References, inputarchive.HotReference{Kind: "generation", ID: generation, ArtifactKind: kind, ScopeKey: scope})
		}
	}
	return node
}
