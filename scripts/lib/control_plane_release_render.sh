#!/usr/bin/env bash

# Side-effect-free argv construction for the release-domain render gate. This
# seam does not execute Helm or claim manifest equivalence by itself.
# The callback is responsible for privately consuming command stdout. Both the
# stored base release and dry-run target contain sensitive rendered values and
# must never be logged or uploaded as raw artifacts.

control_plane_release_with_private_manifest_render_argv() {
  (( $# >= 2 )) || return 2

  local live_revision="$1"
  local consumer="$2"
  local argument=""
  local reset_then_reuse_count=0
  local -a CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV=()
  local -a CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV=()
  local -a CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV=()
  local -a CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV=()
  shift 2
  CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV=("$@")

  [[ "${live_revision}" =~ ^[1-9][0-9]*$ ]] || return 2
  [[ -n "${consumer}" ]] && declare -F "${consumer}" >/dev/null 2>&1 || return 2
  (( ${#CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[@]} >= 7 )) || return 2
  [[ "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[0]}" == "helm" &&
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[1]}" == "upgrade" &&
    -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[2]}" &&
    -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[3]}" &&
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[4]}" == "-n" &&
    -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[5]}" &&
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[6]}" == "--reset-then-reuse-values" ]] || return 2

  for argument in "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[@]}"; do
    case "${argument}" in
      --reset-then-reuse-values)
        reset_then_reuse_count=$((reset_then_reuse_count + 1))
        ;;
      --dry-run|--dry-run=*|--output|--output=*|-o|-o=*|-o?*|\
      --hide-secret|--hide-secret=*|--install|--install=*|-i|-i=*|\
      --debug|--debug=*)
        return 2
        ;;
    esac
  done
  (( reset_then_reuse_count == 1 )) || return 2

  CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV=(
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[@]}"
    --dry-run=server
    --output json
  )
  CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV=(
    helm get all "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[2]}"
    -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[5]}"
    --revision "${live_revision}"
    --template '{{ .Release.Manifest }}{{ range .Release.Hooks }}{{ printf "\n---\n%s\n" .Manifest }}{{ end }}'
  )
  CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV=(
    "${CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV[@]}"
  )

  readonly -a CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV
  readonly -a CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV
  readonly -a CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV
  readonly -a CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV

  # Bracket the pinned base read with two independently executed target dry
  # runs. A later consumer compares their canonical manifests, so lookup or
  # cluster-state drift during the evidence window fails closed.
  "${consumer}" target "${CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV[@]}" || return
  "${consumer}" base "${CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV[@]}" || return
  "${consumer}" repeated-target "${CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV[@]}"
}
