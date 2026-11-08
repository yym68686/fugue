package store

// Keep the five typed DNS dependency keys, including explicit JSON null, using
// scalar operators only. jsonb_to_record materializes a record and can spill a
// single large dependency field under the inventory's fixed zero-temp guard.
// Non-DNS and non-object sources retain their original representation.
//
// This read-only expression has the same value as the immutable v1 index
// projection. Its derivation trigger and cache verification remain unchanged;
// a current cached value is still valid and a missing/stale cache uses this
// source projection. Full source bytes still own the artifact and batch caps.
const compilerInputHotReadProjection = `CASE WHEN artifact_kind='dns_answer_bundle' AND jsonb_typeof(content_json)='object'
 THEN (CASE WHEN content_json ? 'lineage' THEN jsonb_build_object('lineage',content_json->'lineage') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'policy' THEN jsonb_build_object('policy',content_json->'policy') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'cell_route_publications' THEN jsonb_build_object('cell_route_publications',content_json->'cell_route_publications') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'previous_traffic_publication' THEN jsonb_build_object('previous_traffic_publication',content_json->'previous_traffic_publication') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'cell_dns_source' THEN jsonb_build_object('cell_dns_source',content_json->'cell_dns_source') ELSE '{}'::jsonb END)
 ELSE content_json END`
