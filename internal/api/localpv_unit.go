package api

import _ "embed"

//go:embed localpv_unit.py
var localPVUnitProgram string

func localPVUnitShellLibrary() string {
	return `
localpv_boot_unit_observation() {
  if ! command -v python3 >/dev/null 2>&1; then
    printf '%s\n' '{"state":"failed","reason":"python3 unavailable","changed":"false"}'
    return
  fi
  python3 - reconcile \
    --unit-path "${FUGUE_LOCALPV_UNIT_PATH:-/etc/systemd/system/${FUGUE_LOCALPV_LOOP_SERVICE}}" \
    --mode "${1:-observe}" \
    --lock-path "${FUGUE_LOCALPV_UNIT_LOCK_PATH:-/run/lock/fugue-localpv-unit.lock}" <<'PY_LOCALPV_BOOT_UNIT'
` + localPVUnitProgram + `
PY_LOCALPV_BOOT_UNIT
}

reconcile_localpv_boot_unit() {
  local observation=""
  observation="$(localpv_boot_unit_observation "${FUGUE_LOCALPV_UNIT_MODE:-apply}")" || {
    log "LocalPV boot unit inspection failed; existing storage preserved"
    return 0
  }
  FUGUE_LOCALPV_UNIT_LAST_APPLY="${observation}"
  log "LocalPV boot unit: ${observation}"
}
`
}
