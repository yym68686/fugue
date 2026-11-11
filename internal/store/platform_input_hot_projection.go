package store

// Keep the five typed DNS dependency keys, including explicit JSON null, using
// scalar operators only. jsonb_to_record materializes a record and can spill a
// single large dependency field under the inventory's fixed zero-temp guard.
// Large route/TLS objects need only lineage and policy. Before omitting other
// fields, exclude sources that could contain float64 overflow. JSONB prints
// numeric values in decimal notation; overflow needs at least 309 consecutive
// integer digits. The conservative text check can also match a string or a long
// fraction, which simply keeps the full source. Only shallow objects take the
// projection; deeper values retain the generic decoder's original depth rules.
//
// The immutable v1 index can still supply its larger original route/TLS value:
// it uses the same dependency decoder. Its derivation and verification stay
// unchanged. Full source bytes still own the artifact and batch caps.
// compiler_input_source_text is a single per-row conversion shared with the
// original byte charge. The caller's OFFSET 0 lateral boundary prevents SQL
// inlining from repeating that conversion for each size/number/output check.
const compilerInputHotReadProjection = `CASE WHEN artifact_kind='dns_answer_bundle' AND jsonb_typeof(content_json)='object'
 THEN ((CASE WHEN content_json ? 'lineage' THEN jsonb_build_object('lineage',content_json->'lineage') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'policy' THEN jsonb_build_object('policy',content_json->'policy') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'cell_route_publications' THEN jsonb_build_object('cell_route_publications',content_json->'cell_route_publications') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'previous_traffic_publication' THEN jsonb_build_object('previous_traffic_publication',content_json->'previous_traffic_publication') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'cell_dns_source' THEN jsonb_build_object('cell_dns_source',content_json->'cell_dns_source') ELSE '{}'::jsonb END))::text
 WHEN artifact_kind IN ('edge_route_bundle','caddy_route_config') AND jsonb_typeof(content_json)='object'
   AND octet_length(compiler_input_source_text)>=65536
   AND compiler_input_source_text !~ '[0-9]{155}[0-9]{154}'
   AND NOT jsonb_path_exists(content_json,
     'strict $.**{64} ? (@.type() == "object" || @.type() == "array")')
 THEN ((CASE WHEN content_json ? 'lineage' THEN jsonb_build_object('lineage',content_json->'lineage') ELSE '{}'::jsonb END)
   || (CASE WHEN content_json ? 'policy' THEN jsonb_build_object('policy',content_json->'policy') ELSE '{}'::jsonb END))::text
 ELSE compiler_input_source_text END`
