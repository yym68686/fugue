package schemamigrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
)

// This closed catalog contract prevents a separately established ledger cost
// proof from silently acquiring extra allocation-bearing behavior. It is not
// itself an allocation/WAL/bootstrap bound, and never installs or repairs DDL.
func verifyCompilerInputResourceContract(ctx context.Context, tx interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, table string) error {
	var columns [][]any
	var constraints, keys []string
	foreignTriggers := 0
	switch table {
	case "fugue_platform_input_resource_pool":
		columns = [][]any{{"singleton", "boolean", true}, {"config_json", "text", true}}
		constraints = []string{
			"p:PRIMARY KEY (singleton)", "c:CHECK (singleton)",
			"c:CHECK (((octet_length(config_json) >= 1) AND (octet_length(config_json) <= 8192)))",
		}
		keys = []string{"singleton"}
	case "fugue_platform_input_resource_reservations":
		columns = [][]any{{"reservation_id", "text", true}, {"ticket", "bigint", true}, {"operation", "text", true},
			{"request_sha256", "text", true}, {"created_at", "timestamp with time zone", true}, {"allocation_json", "text", true}}
		constraints = []string{
			"p:PRIMARY KEY (reservation_id)", "u:UNIQUE (ticket)",
			"c:CHECK ((reservation_id ~ '^[0-9a-f]{64}$'::text))",
			"c:CHECK (((ticket >= 1) AND (ticket <= 1024)))",
			"c:CHECK ((operation = ANY (ARRAY['pending-root'::text, 'pending-policy'::text])))",
			"c:CHECK ((request_sha256 ~ '^[0-9a-f]{64}$'::text))",
			"c:CHECK (((octet_length(allocation_json) >= 1) AND (octet_length(allocation_json) <= 4096)))",
		}
		keys = []string{"reservation_id", "ticket"}
		foreignTriggers = 2
	case "fugue_platform_input_resource_outcomes":
		columns = [][]any{{"reservation_id", "text", true}, {"outcome", "text", true}}
		constraints = []string{
			"p:PRIMARY KEY (reservation_id)",
			"f:FOREIGN KEY (reservation_id) REFERENCES fugue_platform_input_resource_reservations(reservation_id)",
			"c:CHECK ((outcome = ANY (ARRAY['not-attempted'::text, 'completion-not-confirmed'::text, 'committed'::text])))",
		}
		keys = []string{"reservation_id"}
		foreignTriggers = 2
	default:
		return fmt.Errorf("compiler input resource contract unavailable")
	}
	sort.Strings(constraints)
	columnJSON, _ := json.Marshal(columns)
	constraintJSON, _ := json.Marshal(constraints)
	keyJSON, _ := json.Marshal(keys)
	var valid bool
	// Exact positive column set, native heap/default storage, no rewrite rules,
	// inheritance, generated expressions or defaults that could execute code.
	if err := tx.QueryRowContext(ctx, `SELECT
 n.nspname=current_schema() AND c.relkind='r' AND c.relpersistence='p'
 AND c.relam=(SELECT oid FROM pg_am WHERE amname='heap') AND c.reltablespace=0
 AND c.reloptions IS NULL AND c.relreplident='d'
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relhasrules
 AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
 AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid)
 AND COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull) ORDER BY a.attnum)
  FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0),'[]'::jsonb)=$2::jsonb
 AND NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
  WHERE a.attrelid=c.oid AND a.attnum>0 AND
   (a.attisdropped OR a.atthasdef OR a.attidentity<>'' OR a.attgenerated<>'' OR a.attndims<>0
    OR a.atttypmod<>-1 OR a.attstorage<>t.typstorage OR a.attcompression<>'' OR a.attcollation<>t.typcollation))
 AND NOT EXISTS(SELECT 1 FROM pg_attrdef WHERE adrelid=c.oid)
 AND NOT EXISTS(SELECT 1 FROM pg_class toast WHERE toast.oid=c.reltoastrelid
  AND (toast.relkind<>'t' OR toast.relpersistence<>'p' OR toast.reltablespace<>0 OR toast.reloptions IS NOT NULL))
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`, table, string(columnJSON)).Scan(&valid); err != nil || !valid {
		return fmt.Errorf("compiler input resource column/storage contract unavailable")
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(contype::text||':'||pg_get_constraintdef(oid) ORDER BY contype::text||':'||pg_get_constraintdef(oid)) FILTER(WHERE contype<>'n'),'[]'::jsonb)=$2::jsonb
 AND COALESCE(bool_and(convalidated AND NOT condeferrable AND NOT condeferred
  AND COALESCE((to_jsonb(pg_constraint)->>'conenforced')::boolean,true)
  AND (contype<>'c' OR NOT connoinherit)),false)
 FROM pg_constraint WHERE conrelid=to_regclass($1)`, table, string(constraintJSON)).Scan(&valid); err != nil || !valid {
		return fmt.Errorf("compiler input resource constraint contract unavailable")
	}
	// All table indexes must be the exact one-column, default-opclass unique
	// btrees. Extra/expression/include/partial indexes are cost-bearing drift.
	if err := tx.QueryRowContext(ctx, `SELECT
 COALESCE(jsonb_agg(a.attname ORDER BY a.attname),'[]'::jsonb)=$2::jsonb
 AND COALESCE(bool_and(i.indisunique AND i.indimmediate AND i.indisvalid AND i.indisready AND i.indislive
  AND NOT i.indnullsnotdistinct AND NOT i.indisclustered AND NOT i.indisreplident
  AND i.indnatts=1 AND i.indnkeyatts=1 AND i.indexprs IS NULL AND i.indpred IS NULL
  AND i.indoption[0]=0 AND i.indcollation[0]=a.attcollation
  AND x.relkind='i' AND x.relpersistence='p' AND x.reltablespace=0 AND x.reloptions IS NULL
  AND am.amname='btree' AND o.opcdefault AND o.opcintype=a.atttypid
  AND o.opcnamespace='pg_catalog'::regnamespace),false)
 FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid JOIN pg_am am ON am.oid=x.relam
 LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
 JOIN pg_opclass o ON o.oid=i.indclass[0] WHERE i.indrelid=to_regclass($1)`, table, string(keyJSON)).Scan(&valid); err != nil || !valid {
		return fmt.Errorf("compiler input resource index contract unavailable")
	}
	// The only permitted triggers are the already body-verified append guard
	// and this schema's one nondeferrable FK's native referential checks.
	if err := tx.QueryRowContext(ctx, `SELECT count(*)=$2 AND COALESCE(bool_and(
 t.tgnargs=0 AND octet_length(t.tgargs)=0 AND t.tgqual IS NULL AND t.tgattr=''::int2vector
 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AND NOT t.tgdeferrable AND NOT t.tginitdeferred
 AND CASE WHEN NOT t.tgisinternal THEN t.tgname='fugue_input_resource_append_guard' AND t.tgtype=58 AND t.tgenabled='A'
 ELSE t.tgenabled='O' AND p.pronamespace='pg_catalog'::regnamespace
  AND t.tgconstraint=(SELECT oid FROM pg_constraint WHERE contype='f' AND conrelid=to_regclass('fugue_platform_input_resource_outcomes')
   AND confrelid=to_regclass('fugue_platform_input_resource_reservations'))
  AND CASE WHEN $1='fugue_platform_input_resource_outcomes' THEN
   (p.proname='RI_FKey_check_ins' AND t.tgtype=5 OR p.proname='RI_FKey_check_upd' AND t.tgtype=17)
   AND t.tgconstrrelid=to_regclass('fugue_platform_input_resource_reservations')
  ELSE (p.proname='RI_FKey_noaction_del' AND t.tgtype=9 OR p.proname='RI_FKey_noaction_upd' AND t.tgtype=17)
   AND t.tgconstrrelid=to_regclass('fugue_platform_input_resource_outcomes') END END),false)
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=to_regclass($1)`, table, 1+foreignTriggers).Scan(&valid); err != nil || !valid {
		return fmt.Errorf("compiler input resource trigger contract unavailable")
	}
	return nil
}
