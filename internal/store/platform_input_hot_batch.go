package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

// Only artifact/input transport is batched; both still use the same typed
// dependency decoder and manifest validation as single-identity reads. No
// original compiler input bytes are fetched. All statements use the caller's
// single read-only Repeatable Read snapshot and existing indexes.
func resolveCompilerInputHotBatch(ctx context.Context, tx inputQueryer, refs []inputarchive.HotReference) (map[inputarchive.HotReference]inputarchive.HotNode, error) {
	return resolveCompilerInputHotBatchObserved(ctx, tx, refs, nil)
}

func resolveCompilerInputHotBatchObserved(ctx context.Context, tx inputQueryer, refs []inputarchive.HotReference, cost *CompilerInputHotCost) (map[inputarchive.HotReference]inputarchive.HotNode, error) {
	if len(refs) < 1 || len(refs) > 32 {
		return nil, ErrInvalidInput
	}
	out := map[inputarchive.HotReference]inputarchive.HotNode{}
	artifacts, inputs := []string{}, []string{}
	generations, releases := []inputarchive.HotReference{}, []inputarchive.HotReference{}
	for _, ref := range refs {
		switch ref.Kind {
		case "artifact":
			artifacts = append(artifacts, ref.ID)
		case "input":
			inputs = append(inputs, ref.ID)
		case "generation":
			generations = append(generations, ref)
		case "release":
			releases = append(releases, ref)
		default:
			node, err := resolveCompilerInputHotNode(ctx, tx, ref)
			if err == nil {
				out[ref] = node
			} else if !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, ErrConflict) && !errors.Is(err, ErrInvalidInput) {
				return nil, err
			}
		}
	}
	var artifactCost, inputCost, ledgerCost *CompilerInputHotReadCost
	if cost != nil {
		artifactCost, inputCost, ledgerCost = &cost.Artifacts, &cost.Inputs, &cost.Ledger
	}
	if err := resolveCompilerInputHotLedgerBatchObserved(ctx, tx, generations, releases, out, ledgerCost); err != nil {
		return nil, err
	}
	if len(artifacts) > 0 {
		// jsonb_each materializes even discarded answer values and can spill
		// them to disk. Decode the object once, then use its key set to preserve
		// JSON null separately from a missing dependency key.
		rows, err := queryCompilerInputHotRows(ctx, tx, artifactCost, `SELECT id,artifact_kind,content_hash,
 left((CASE WHEN artifact_kind='dns_answer_bundle' AND jsonb_typeof(content_json)='object'
 THEN COALESCE((SELECT jsonb_object_agg(fields.key,fields.value)
 FROM jsonb_to_record(content_json) AS dependency(lineage jsonb,policy jsonb,
 cell_route_publications jsonb,previous_traffic_publication jsonb,cell_dns_source jsonb)
 CROSS JOIN LATERAL (SELECT array_agg(key) AS present FROM jsonb_object_keys(content_json) AS object_keys(key)) keys
 CROSS JOIN LATERAL (VALUES
 ('lineage',COALESCE(dependency.lineage,'null'::jsonb)),('policy',COALESCE(dependency.policy,'null'::jsonb)),
 ('cell_route_publications',COALESCE(dependency.cell_route_publications,'null'::jsonb)),
 ('previous_traffic_publication',COALESCE(dependency.previous_traffic_publication,'null'::jsonb)),
 ('cell_dns_source',COALESCE(dependency.cell_dns_source,'null'::jsonb))) fields(key,value)
 WHERE fields.key=ANY(keys.present)),'{}'::jsonb)
 ELSE content_json END)::text,$2),left(metadata_json::text,16385),octet_length(content_json::text)
 FROM fugue_platform_artifacts WHERE id=ANY($1::text[])`, artifacts, compilerInputHotArtifactBytes+1)
		if err != nil {
			return nil, err
		}
		// A single artifact is capped at 8MiB and a batch at 32MiB of text.
		// Refuse large metadata rather than retaining unbounded configurations.
		bytes := 0
		for nextCompilerInputHotRow(rows, artifactCost) {
			var a model.PlatformArtifact
			var content, metadata []byte
			var originalBytes int64
			if err = rows.Scan(&a.ID, &a.ArtifactKind, &a.ContentHash, &content, &metadata, &originalBytes); err != nil {
				break
			}
			if artifactCost != nil {
				artifactCost.SourceBytes += originalBytes
				artifactCost.TransportBytes += int64(len(content) + len(metadata))
			}
			// Count the full source size against the unchanged per-batch cap,
			// even though unrelated DNS answer views are not transported.
			bytes += int(originalBytes) + len(metadata)
			if bytes > 32<<20 {
				err = ErrConflict
				break
			}
			started := time.Now()
			node, decodeErr := decodeCompilerInputHotBatchArtifact(a, content, metadata, originalBytes)
			if artifactCost != nil {
				artifactCost.DecodeUS += time.Since(started).Microseconds()
			}
			if decodeErr == nil {
				out[node.Identity] = node
			}
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	if len(inputs) > 0 {
		rows, err := queryCompilerInputHotRows(ctx, tx, inputCost, `SELECT m.content_hash,m.format,m.size_bytes,m.chunk_size,m.chunk_count,m.created_at,m.storage_location,
 c.n,c.first,c.last,c.bytes
 FROM fugue_platform_input_manifests m LEFT JOIN LATERAL (
  SELECT count(*) n,COALESCE(min(ordinal),-1) first,COALESCE(max(ordinal),-1) last,COALESCE(sum(octet_length(data)),0) bytes
  FROM (
   SELECT ordinal,data FROM fugue_platform_input_chunks WHERE content_hash=m.content_hash AND m.storage_location='legacy' AND m.format IN ('runtime-snapshot-json-v1','runtime-snapshot-json-chunk-codec-v2')
   UNION ALL
   SELECT ordinal,data FROM fugue_platform_input_hot_chunks WHERE content_hash=m.content_hash AND m.storage_location='hot' AND m.format IN ('runtime-snapshot-json-v1','runtime-snapshot-json-chunk-codec-v2')
  ) chunks
 ) c ON m.format IN ('runtime-snapshot-json-v1','runtime-snapshot-json-chunk-codec-v2')
 WHERE m.content_hash=ANY($1::text[])`, inputs)
		if err != nil {
			return nil, err
		}
		found := map[string]bool{}
		for nextCompilerInputHotRow(rows, inputCost) {
			var m inputManifest
			var count, first, last, encoded sql.NullInt64
			if err = rows.Scan(&m.Digest, &m.Format, &m.Size, &m.ChunkSize, &m.Count, &m.Created, &m.Location, &count, &first, &last, &encoded); err != nil {
				break
			}
			found[m.Digest] = true
			if validateCompilerInputManifest(m) != nil {
				continue
			}
			node := inputarchive.HotNode{Identity: inputarchive.HotReference{Kind: "input", ID: m.Digest}, ContentHash: m.Digest,
				Format: m.Format, Location: m.Location, LogicalBytes: m.Size, References: []inputarchive.HotReference{}}
			if m.Format == inputFormatV1 || m.Format == inputFormatCodecV2 {
				if !count.Valid || !first.Valid || !last.Valid || !encoded.Valid || count.Int64 != int64(m.Count) || first.Int64 != 0 || last.Int64 != count.Int64-1 {
					continue
				}
				node.EncodedBytes, node.NeedsLocalCopy = encoded.Int64, m.Location == "legacy"
			}
			out[node.Identity] = node
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		// Very old inline bodies have no manifest. This query reads only their
		// recorded lengths; a malformed existing manifest is never bypassed.
		missing := []string{}
		for _, digest := range inputs {
			if !found[digest] {
				missing = append(missing, digest)
			}
		}
		if len(missing) > 0 {
			rows, err := queryCompilerInputHotRows(ctx, tx, inputCost, `SELECT content_hash,size_bytes FROM fugue_platform_artifact_contents WHERE content_hash=ANY($1::text[])`, missing)
			if err != nil {
				return nil, err
			}
			for nextCompilerInputHotRow(rows, inputCost) {
				node := inputarchive.HotNode{Identity: inputarchive.HotReference{Kind: "input"}, Format: "legacy-inline", Location: "artifact-contents", References: []inputarchive.HotReference{}}
				if err = rows.Scan(&node.Identity.ID, &node.LogicalBytes); err != nil {
					break
				}
				node.ContentHash = node.Identity.ID
				out[node.Identity] = node
			}
			if err == nil {
				err = rows.Err()
			}
			if closeErr := rows.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func decodeCompilerInputHotBatchArtifact(a model.PlatformArtifact, content, metadata []byte, originalBytes int64) (inputarchive.HotNode, error) {
	if originalBytes > compilerInputHotArtifactBytes || len(content) > compilerInputHotArtifactBytes || len(metadata) > 16<<10 || json.Unmarshal(content, &a.Content) != nil || json.Unmarshal(metadata, &a.Metadata) != nil {
		return inputarchive.HotNode{}, ErrConflict
	}
	return compilerInputArtifactHotNode(a)
}
