# Called with errexit disabled by the task dispatcher. Every fallible operation
# must therefore be checked explicitly. Never rerun the installer: it rewrites
# service arguments and environment files, even with SKIP_START=true.
k3s_upgrade_validate_versions() {
  python3 - "$1" "$2" <<'PY_K3S_VERSIONS'
import re, sys
pattern = r"v(1)\.([0-9]+)\.([0-9]+)\+k3s([1-9][0-9]*)"
def version(raw):
    match = re.fullmatch(pattern, raw)
    if not match:
        raise ValueError("an exact stable K3s version is required")
    return tuple(map(int, match.groups()))
try:
    current, target = map(version, sys.argv[1:])
    if target < current or target[1] - current[1] not in (0, 1):
        raise ValueError("K3s downgrades and skipped minor versions are refused")
except ValueError as exc:
    print(str(exc), file=sys.stderr)
    sys.exit(1)
PY_K3S_VERSIONS
}

k3s_upgrade_node_uid() {
  k3s kubectl --kubeconfig "${upgrade_kubeconfig}" --request-timeout=5s \
    get node "${upgrade_node}" -o json | python3 -c '
import json, sys
node = json.load(sys.stdin)
expected_version, expected_uid = sys.argv[1:]
uid = node["metadata"]["uid"]
if (not uid or (expected_uid and uid != expected_uid) or
    node["status"]["nodeInfo"]["kubeletVersion"] != expected_version or
    not any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"]["conditions"])):
    sys.exit("node identity, Ready condition, or kubelet version has not converged")
print(uid)
' "$1" "${2:-}"
}

k3s_upgrade_container_ids() {
  k3s crictl ps --state running -o json | python3 -c '
import json, sys
containers = json.load(sys.stdin)["containers"]
ids = sorted(c["id"] for c in containers)
if any(not isinstance(i, str) or not i for i in ids):
    sys.exit("invalid CRI container identity")
print("\n".join(ids))
'
}

k3s_upgrade_process_version() {
  # K3s release launchers execute an extracted binary under data/<hash>/bin.
  # Comparing its inode with /usr/local/bin/k3s incorrectly rejects real K3s.
  [[ "$1" =~ ^[1-9][0-9]*$ ]] || return 1
  [ "$("/proc/$1/exe" --version | awk 'NR == 1 {print $3}')" = "$2" ]
}

upgrade_k3s_agent() (
  set -o pipefail
  local target="${FUGUE_NODE_UPDATE_TASK_TARGET_K3S_VERSION:-}"
  local current="" binary="" stage="" expected_hash="" asset="" node_uid="" started="" pid=""
  local upgrade_node="" upgrade_kubeconfig="${FUGUE_NODE_UPDATER_KUBELET_KUBECONFIG:-/var/lib/rancher/k3s/agent/kubelet.kubeconfig}"
  local mutated=false phase=validate_versions
  trap 'upgrade_rc=$?; if [ "${upgrade_rc}" -ne 0 ]; then log_task "K3s upgrade failed at ${phase}; binary_replaced=${mutated}; automatic downgrade disabled" || true; fi; if [ -n "${stage}" ]; then rm -f -- "${stage}/candidate" "${stage}/checksums" "${stage}/containers.before" "${stage}/containers.after"; if [ "${mutated}" = false ]; then rm -f -- "${stage}/previous"; rmdir -- "${stage}"; fi; fi' EXIT
  command -v python3 >/dev/null || return 1
  current="$(k3s_version | awk 'NR == 1 {print $3}')" || return 1
  k3s_upgrade_validate_versions "${current}" "${target}" || return 1
  phase=preflight
  # A failed download or restart must not be hidden by the final log call.
  if ! systemctl is-active --quiet k3s-agent || systemctl is-active --quiet k3s; then
    log_task "K3s upgrade refused: expected an active agent, not a server"
    return 1
  fi
  if [ "$(systemctl show k3s-agent -p KillMode --value)" != process ]; then
    log_task "K3s upgrade refused: service does not preserve child processes"
    return 1
  fi
  [ -r "${upgrade_kubeconfig}" ] || return 1
  upgrade_node="$(node_deep_health_cluster_node_name)" || return 1
  [ -n "${upgrade_node}" ] || return 1
  node_uid="$(k3s_upgrade_node_uid "${current}")" || return 1
  k3s kubectl --kubeconfig "${upgrade_kubeconfig}" --request-timeout=5s version -o json | \
    python3 -c 'import json,sys; v=json.load(sys.stdin)["serverVersion"]; minor=int(v["minor"].rstrip("+")); target=int(sys.argv[1].split(".")[1]); sys.exit(0 if minor-3 <= target <= minor else "upgrade the server before its agents")' \
    "${target}" || return 1
  binary="$(readlink -f "$(command -v k3s)")" || return 1
  [ -f "${binary}" ] && [ -x "${binary}" ] || return 1
  pid="$(systemctl show k3s-agent -p MainPID --value)" || return 1
  k3s_upgrade_process_version "${pid}" "${current}" || return 1
  if [ "${current}" = "${target}" ]; then
    log_task "K3s agent already has the verified target version and Ready identity"
    return 0
  fi
  command -v flock >/dev/null && command -v sha256sum >/dev/null || return 1
  mkdir -p "${FUGUE_NODE_UPDATER_WORK_DIR}" || return 1
  exec 9>"${FUGUE_NODE_UPDATER_WORK_DIR}/k3s-upgrade.lock" || return 1
  flock -n 9 || return 1
  case "$(uname -m)" in
    x86_64) asset=k3s; local arch=amd64 ;;
    aarch64|arm64) asset=k3s-arm64; local arch=arm64 ;;
    *) log_task "K3s upgrade refused: unsupported architecture"; return 1 ;;
  esac
  stage="$(mktemp -d "$(dirname "${binary}")/.fugue-k3s-upgrade.XXXXXX")" || return 1
  phase=download_verify
  k3s_upgrade_container_ids >"${stage}/containers.before" || return 1
  local release="https://github.com/k3s-io/k3s/releases/download/${target/+/%2B}"
  curl --fail --silent --show-error --location --retry 2 --connect-timeout 10 --max-time 180 \
    "${release}/sha256sum-${arch}.txt" -o "${stage}/checksums" || return 1
  expected_hash="$(awk -v asset="${asset}" '$2 == asset || $2 == "*"asset {print $1}' "${stage}/checksums")" || return 1
  [[ "${expected_hash}" =~ ^[0-9a-f]{64}$ ]] || return 1
  curl --fail --silent --show-error --location --retry 2 --connect-timeout 10 --max-time 180 \
    "${release}/${asset}" -o "${stage}/candidate" || return 1
  [ "$(sha256sum "${stage}/candidate" | awk '{print $1}')" = "${expected_hash}" ] || return 1
  chmod 0755 "${stage}/candidate" || return 1
  [ "$("${stage}/candidate" --version | awk 'NR == 1 {print $3}')" = "${target}" ] || return 1
  # Check the same node again immediately before the only disruptive action.
  k3s_upgrade_node_uid "${current}" "${node_uid}" >/dev/null || return 1
  cp -p -- "${binary}" "${stage}/previous" || return 1
  mv -f -- "${stage}/candidate" "${binary}" || return 1
  mutated=true
  phase=restart
  # Do not automatically downgrade after a restart failure: cross-minor rollback
  # requires the matching datastore snapshot. Retain the old binary for recovery.
  systemctl --no-block restart k3s-agent || return 1
  phase=convergence
  started="$(date +%s)" || return 1
  while [ "$(( $(date +%s) - started ))" -lt 900 ]; do
    pid="$(systemctl show k3s-agent -p MainPID --value)" || return 1
    if k3s_upgrade_process_version "${pid}" "${target}" && \
       systemctl is-active --quiet k3s-agent && \
       k3s_upgrade_node_uid "${target}" "${node_uid}" >/dev/null 2>&1; then
      phase=container_continuity
      k3s_upgrade_container_ids >"${stage}/containers.after" || return 1
      python3 - "${stage}/containers.before" "${stage}/containers.after" <<'PY_K3S_CONTAINERS' || return 1
import pathlib, sys
before, after = (set(pathlib.Path(p).read_text().split()) for p in sys.argv[1:])
if not before <= after:
    sys.exit("K3s upgrade stopped: pre-existing running containers disappeared")
PY_K3S_CONTAINERS
      log_task "K3s agent upgrade verified: ${target}; node identity and running containers retained; previous binary: ${stage}/previous"
      return 0
    fi
    sleep 5
  done
  log_task "K3s upgrade did not converge; stopping without an automatic downgrade; previous binary: ${stage}/previous"
  return 1
)
