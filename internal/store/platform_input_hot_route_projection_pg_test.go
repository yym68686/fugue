package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

const hotRouteTestTextSource = ` CROSS JOIN LATERAL (SELECT content_json::text AS compiler_input_source_text OFFSET 0) hot_source`

func TestHotRouteReadProjectionPostgresMatchesFullSource(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit='0'`); err != nil {
		t.Fatal(err)
	}
	boundary := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 1024), new(big.Int).Lsh(big.NewInt(1), 970))
	below := new(big.Int).Sub(new(big.Int).Set(boundary), big.NewInt(1)).String()
	above := new(big.Int).Add(new(big.Int).Set(boundary), big.NewInt(1)).String()
	compiled := compilerInputHotLineageFixture(t, true)
	for _, source := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.TLSArtifact} {
		for _, fields := range []string{
			`"ignored":null`, `"ignored":[1.0,-0,9007199254740993,1e-9999]`,
			`"ignored":{"nested":[1e309]}`, `"ignored":{"nested":[-1e309]}`,
			`"ignored":` + below, `"ignored":-` + below,
			`"ignored":` + boundary.String(), `"ignored":-` + boundary.String(),
			`"ignored":` + above, `"ignored":-` + above,
			`"ignored":1e309,"ignored":0`, `"ignored":"1e309"`,
			`"lineage":null`, `"lineage":[]`, `"policy":null`,
			`"policy":{"nested":1e309}`, `"policy":{"n":9007199254740993}`,
		} {
			base, _ := json.Marshal(source.Content)
			content := string(base[:len(base)-1]) + `,"padding":"` + strings.Repeat("x", 80<<10) + `",` + fields + `}`
			var full, projected []byte
			if err := tx.QueryRow(`SELECT compiler_input_source_text,(`+compilerInputHotReadProjection+`)
 FROM (SELECT $1::text AS artifact_kind,$2::jsonb AS content_json) source`+hotRouteTestTextSource, source.ArtifactKind, content).Scan(&full, &projected); err != nil {
				t.Fatal("bounded local JSONB projection", err)
			}
			metadata, _ := json.Marshal(source.Metadata)
			a := source
			a.Content = nil
			want, we := referenceHotFullObjectDecode(a, full, metadata, int64(len(full)))
			got, ge := decodeCompilerInputHotBatchArtifact(a, projected, metadata, int64(len(full)))
			if (we == nil) != (ge == nil) || we != nil && we.Error() != ge.Error() || we == nil && !reflect.DeepEqual(want, got) {
				t.Fatalf("%s %s changed dependencies or rejection: %v / %v", a.ArtifactKind, fields, we, ge)
			}
			// Unsafe ignored numbers must not disappear behind a tiny projection.
			var parsed map[string]any
			if json.Unmarshal(full, &parsed) != nil && string(full) != string(projected) {
				t.Fatal("unsafe source was projected instead of keeping original rejection")
			}
		}
	}
	// Small, nonobject and unrelated sources retain their full representation.
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindPolicySnapshot} {
		for _, content := range []string{`null`, `[]`, `{"ignored":42}`, `{"ignored":"` + strings.Repeat("x", 80<<10) + `"}`} {
			var same bool
			if err := tx.QueryRow(`SELECT (`+compilerInputHotReadProjection+`)=compiler_input_source_text
 FROM (SELECT $1::text AS artifact_kind,$2::jsonb AS content_json) source`+hotRouteTestTextSource, kind, content).Scan(&same); err != nil {
				t.Fatal(err)
			}
			if (kind == model.PlatformArtifactKindPolicySnapshot || len(content) < 65536) && !same {
				t.Fatal("fallback representation changed")
			}
		}
	}
}

func TestHotRouteReadProjectionPostgresMeasuresTransport(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	compiled := compilerInputHotLineageFixture(t, true)
	for _, objects := range []bool{false, true} {
		t.Run(fmt.Sprintf("objects-%t", objects), func(t *testing.T) {
			source := compiled.RouteArtifact
			source.ID, source.ContentHash = "neutral-large-route", "sha256:"+strings.Repeat("a", 64)
			contentMap := make(map[string]any, len(source.Content)+1)
			for k, v := range source.Content {
				contentMap[k] = v
			}
			source.Content = contentMap
			source.Content["ignored"] = strings.Repeat("x", 512<<10)
			if objects {
				view := make([]any, 2000)
				for i := range view {
					view[i] = map[string]any{"host": fmt.Sprintf("host-%d.example.test", i), "ports": []int{80, 443}, "enabled": true, "weight": 1.0, "details": map[string]any{"path": "/", "origin": "http://origin.example.test:8080"}}
				}
				source.Content["ignored"] = view
			}
			content, _ := json.Marshal(source.Content)
			metadata, _ := json.Marshal(source.Metadata)
			if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,$2,'neutral-scope',$1,'validated',$3,$4,$5,now(),now())`, fmt.Sprintf("neutral-measurement-%t", objects), source.ArtifactKind, source.ContentHash, content, metadata); err != nil {
				t.Fatal(err)
			}
			source.Content = nil
			var want inputarchive.HotNode
			for _, projected := range []bool{false, true} {
				tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(`SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit='0'; SET LOCAL jit=off`); err != nil {
					t.Fatal(err)
				}
				field := `compiler_input_source_text`
				if projected {
					field = compilerInputHotReadProjection
				}
				started, bytes := time.Now(), 0
				for i := 0; i < 16; i++ {
					var raw []byte
					var original int64
					if err := tx.QueryRow(`SELECT (`+field+`),octet_length(compiler_input_source_text)
 FROM fugue_platform_artifacts`+hotRouteTestTextSource+` WHERE id=$1`, fmt.Sprintf("neutral-measurement-%t", objects)).Scan(&raw, &original); err != nil {
						t.Fatal(err)
					}
					got, err := decodeCompilerInputHotBatchArtifact(source, raw, metadata, original)
					if err != nil || projected && !reflect.DeepEqual(want, got) {
						t.Fatal("transport projection changed dependency result", err)
					}
					want = got
					bytes += len(raw)
				}
				t.Logf("projected=%t rows=16 sourceBytes=%d returnedBytes=%d query+decode=%s", projected, len(content)*16, bytes, time.Since(started))
				if projected && bytes >= len(content)*4 {
					t.Fatal("irrelevant wide view was still transported")
				}
				_ = tx.Rollback()
			}
		})
	}
}

func TestHotRouteReadProjectionPostgresDeepSourcesKeepOriginalDecoder(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	// Build JSONB directly so this also covers stored structures deeper than
	// PostgreSQL's text-input parser or JSONPath recursion can safely consume.
	if _, err := db.Exec(`CREATE FUNCTION pg_temp.neutral_deep_route(depth integer) RETURNS jsonb LANGUAGE plpgsql AS $$
 DECLARE value jsonb := '0'::jsonb;
 BEGIN
 FOR i IN 1..depth LOOP value := jsonb_build_array(value); END LOOP;
 RETURN jsonb_build_object('ignored',value,'padding',repeat('x',70000));
 END $$`); err != nil {
		t.Fatal(err)
	}
	for _, depth := range []int{63, 64, 65, 1500, 10001} {
		var full, projected []byte
		if err := db.QueryRow(`SELECT compiler_input_source_text,(`+compilerInputHotReadProjection+`)
 FROM (SELECT 'edge_route_bundle'::text AS artifact_kind, pg_temp.neutral_deep_route($1) AS content_json) source`+hotRouteTestTextSource, depth).Scan(&full, &projected); err != nil {
			t.Fatal("deep source fallback", depth, err)
		}
		if depth >= 64 && string(full) != string(projected) {
			t.Fatal("deep source bypassed the original decoder", depth)
		}
		if depth < 64 && len(projected) >= len(full) {
			t.Fatal("shallow boundary did not use the projection", depth)
		}
		var fullValue map[string]any
		fullErr := json.Unmarshal(full, &fullValue)
		a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle}
		gotErr := decodeCompilerInputHotArtifactJSON(&a, projected, []byte(`{}`))
		if (fullErr == nil) != (gotErr == nil) {
			t.Fatal("original depth refusal changed", depth, fullErr, gotErr)
		}
	}
}
