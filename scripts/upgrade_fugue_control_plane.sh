#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[fugue-upgrade] %s\n' "$*"
}

log_stderr() {
  printf '[fugue-upgrade] %s\n' "$*" >&2
}

fail() {
  printf '[fugue-upgrade] ERROR: %s\n' "$*" >&2
  exit 1
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    fail "missing required env ${name}"
  fi
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

CONTROL_PLANE_AUTOMATION_TMP_DIR=""
UPGRADE_OVERRIDE_VALUES_FILE=""
DNS_STATIC_RECORDS_FILE=""
PLATFORM_ROUTES_FILE=""
LOCAL_CONTROL_PLANE_AUTOMATION_DIR="${FUGUE_LOCAL_CONTROL_PLANE_AUTOMATION_DIR:-${HOME}/.config/fugue/control-plane-automation}"
LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR="${FUGUE_LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR:-/root/.config/fugue/control-plane-automation}"
CONTROL_PLANE_HOSTS_ENV_LOADED="false"
PRIMARY_CONTROL_PLANE_SSH_OPTS=()
PRIMARY_CONTROL_PLANE_SSH_HOST=""
PRIMARY_CONTROL_PLANE_SSH_USER=""
PRIMARY_CONTROL_PLANE_SSH_PORT=""
PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS=""
PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS="${FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS:-5}"
# Kubelet delays clearing DiskPressure for evictionPressureTransitionPeriod
# (5m by default on our k3s nodes), so keep a wider recovery window here.
PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS="${FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS:-600}"
PRIMARY_NODE_READY_POLL_SECONDS="${FUGUE_PRIMARY_NODE_READY_POLL_SECONDS:-5}"
PRIMARY_NODE_READY_TIMEOUT_SECONDS="${FUGUE_PRIMARY_NODE_READY_TIMEOUT_SECONDS:-300}"
LOCAL_KUBE_API_READY_POLL_SECONDS="${FUGUE_LOCAL_KUBE_API_READY_POLL_SECONDS:-2}"
LOCAL_KUBE_API_READY_TIMEOUT_SECONDS="${FUGUE_LOCAL_KUBE_API_READY_TIMEOUT_SECONDS:-180}"
PRIMARY_POSTGRES_DATA_ROOT="${FUGUE_PRIMARY_POSTGRES_DATA_ROOT:-/var/lib/fugue/postgres}"
PRIMARY_POSTGRES_IMAGE="${FUGUE_PRIMARY_POSTGRES_IMAGE:-docker.io/library/postgres:16-alpine}"
FUGUE_DEFAULT_REGISTRY_PULL_BASE="${FUGUE_DEFAULT_REGISTRY_PULL_BASE:-registry.fugue.internal:5000}"
DNS_HELM_SET_ARGS=()

detect_primary_private_ip() {
  ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}'
}

detect_existing_registry_pull_base() {
  if [[ ! -r /etc/rancher/k3s/registries.yaml ]]; then
    return 1
  fi
  local value=""
  value="$(awk '
    $1 == "mirrors:" { in_mirrors = 1; next }
    in_mirrors && /^[[:space:]]*"/ {
      value = $1
      gsub(/"/, "", value)
      sub(/:$/, "", value)
      print value
      exit
    }
  ' /etc/rancher/k3s/registries.yaml)"
  if [[ -z "${value}" ]]; then
    return 1
  fi
  if is_legacy_nodeport_registry_pull_base "${value}"; then
    return 1
  fi
  printf '%s' "${value}"
}

is_legacy_nodeport_registry_pull_base() {
  local value="$1"
  local host="${value%:*}"
  local port="${value##*:}"
  if [[ "${host}" == "${value}" || "${port}" != "${FUGUE_REGISTRY_NODEPORT:-30500}" ]]; then
    return 1
  fi
  case "${host}" in
    10.*|192.168.*|127.*|100.64.*|100.65.*|100.66.*|100.67.*|100.68.*|100.69.*|100.70.*|100.71.*|100.72.*|100.73.*|100.74.*|100.75.*|100.76.*|100.77.*|100.78.*|100.79.*|100.80.*|100.81.*|100.82.*|100.83.*|100.84.*|100.85.*|100.86.*|100.87.*|100.88.*|100.89.*|100.90.*|100.91.*|100.92.*|100.93.*|100.94.*|100.95.*|100.96.*|100.97.*|100.98.*|100.99.*|100.100.*|100.101.*|100.102.*|100.103.*|100.104.*|100.105.*|100.106.*|100.107.*|100.108.*|100.109.*|100.110.*|100.111.*|100.112.*|100.113.*|100.114.*|100.115.*|100.116.*|100.117.*|100.118.*|100.119.*|100.120.*|100.121.*|100.122.*|100.123.*|100.124.*|100.125.*|100.126.*|100.127.*)
      return 0
      ;;
    172.16.*|172.17.*|172.18.*|172.19.*|172.20.*|172.21.*|172.22.*|172.23.*|172.24.*|172.25.*|172.26.*|172.27.*|172.28.*|172.29.*|172.30.*|172.31.*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

detect_primary_mesh_ip() {
  if ! command_exists tailscale; then
    return 1
  fi
  tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}'
}

detect_cluster_join_server() {
  local secret_name="${FUGUE_RELEASE_FULLNAME}-config"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get secret "${secret_name}" -o jsonpath='{.data.FUGUE_CLUSTER_JOIN_SERVER}' 2>/dev/null | base64 --decode 2>/dev/null || true
}

registry_endpoint_from_join_server() {
  local join_server="$1"
  local host=""
  join_server="${join_server#*://}"
  join_server="${join_server%%/*}"
  host="${join_server%%:*}"
  if [[ -n "${host}" ]]; then
    printf '%s:%s' "${host}" "${FUGUE_REGISTRY_NODEPORT}"
  fi
}

registry_endpoint_url_value() {
  local endpoint="$1"
  case "${endpoint}" in
    http://*|https://*)
      printf '%s' "${endpoint}"
      ;;
    *)
      printf 'http://%s' "${endpoint}"
      ;;
  esac
}

ensure_local_registry_mirror_config() {
  local registry_base="$1"
  local endpoint="$2"
  local endpoint_url=""
  local tmp=""
  local target="/etc/rancher/k3s/registries.yaml"

  registry_base="$(printf '%s' "${registry_base}" | awk '{$1=$1; print}')"
  endpoint="$(printf '%s' "${endpoint}" | awk '{$1=$1; print}')"
  if [[ -z "${registry_base}" || -z "${endpoint}" ]]; then
    return 0
  fi
  if [[ "$(id -u)" != "0" ]]; then
    log "skip local registries.yaml migration because upgrade is not running as root"
    return 0
  fi
  if [[ ! -d /etc/rancher/k3s ]]; then
    log "skip local registries.yaml migration because /etc/rancher/k3s is absent"
    return 0
  fi

  endpoint_url="$(registry_endpoint_url_value "${endpoint}")"
  tmp="$(mktemp)"
  cat >"${tmp}" <<EOF
mirrors:
  "${registry_base}":
    endpoint:
      - "${endpoint_url}"
configs:
  "${registry_base}":
    tls:
      insecure_skip_verify: true
EOF

  if [[ -r "${target}" ]] && cmp -s "${tmp}" "${target}"; then
    rm -f "${tmp}"
    log "local registries.yaml already points ${registry_base} at ${endpoint_url}"
    return 0
  fi

  install -m 0644 "${tmp}" "${target}"
  rm -f "${tmp}"
  log "migrated local registries.yaml to mirror ${registry_base} via ${endpoint_url}"

  if command_exists systemctl; then
    if systemctl is-active --quiet k3s; then
      log "restarting local k3s so containerd reloads registry mirror configuration"
      if command_exists timeout; then
        timeout --kill-after=15s 120s systemctl restart k3s
      else
        systemctl restart k3s
      fi
      wait_for_local_kube_api_ready
    elif systemctl is-active --quiet k3s-agent; then
      log "restarting local k3s-agent so containerd reloads registry mirror configuration"
      if command_exists timeout; then
        timeout --kill-after=15s 120s systemctl restart k3s-agent
      else
        systemctl restart k3s-agent
      fi
    fi
  fi
}

detect_kubectl() {
  if [[ -n "${KUBECTL_BIN:-}" ]]; then
    printf '%s' "${KUBECTL_BIN}"
    return
  fi
  if command_exists kubectl; then
    printf 'kubectl'
    return
  fi
  if command_exists k3s; then
    printf 'k3s kubectl'
    return
  fi
  fail "kubectl is not available"
}

retry() {
  local attempts="$1"
  local delay_seconds="$2"
  shift 2

  local i
  for ((i=1; i<=attempts; i++)); do
    if "$@"; then
      return 0
    fi
    if (( i == attempts )); then
      return 1
    fi
    sleep "${delay_seconds}"
  done
}

helm_current_revision() {
  helm history "${FUGUE_RELEASE_NAME}" -n "${FUGUE_NAMESPACE}" --max 1 | awk 'NR==2 {print $1}'
}

rollout_status() {
  local deployment_name="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" rollout status "deploy/${deployment_name}" --timeout="${FUGUE_ROLLOUT_TIMEOUT}"
}

apply_chart_crds() {
  local crd_dir="${FUGUE_HELM_CHART_PATH}/crds"
  local attempt=0
  local max_attempts=3

  if [[ ! -d "${crd_dir}" ]]; then
    log "skip CRD apply because ${crd_dir} does not exist"
    return 0
  fi

  if ! find "${crd_dir}" -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) | grep -q .; then
    log "skip CRD apply because ${crd_dir} has no manifest files"
    return 0
  fi

  while (( attempt < max_attempts )); do
    attempt=$((attempt + 1))
    log "applying Helm CRDs from ${crd_dir} (attempt ${attempt}/${max_attempts})"
    ${KUBECTL} apply -f "${crd_dir}"
    ${KUBECTL} wait --for=condition=Established --timeout=60s -f "${crd_dir}"
    if verify_chart_crds_in_sync "${crd_dir}"; then
      return 0
    fi
    if (( attempt < max_attempts )); then
      log "Helm CRDs still drift after apply; retrying"
      sleep 2
    fi
  done

  fail "Helm CRDs still drift after apply; refusing to continue upgrade"
}

verify_chart_crds_in_sync() {
  local crd_dir="$1"
  local diff_output=""

  if diff_output="$(${KUBECTL} diff -f "${crd_dir}" 2>&1)"; then
    return 0
  fi

  local status=$?
  if (( status == 1 )); then
    log_stderr "Helm CRD drift detected after apply:"
    printf '%s\n' "${diff_output}" >&2
    return 1
  fi

  log_stderr "kubectl diff failed while verifying Helm CRDs (exit=${status}):"
  printf '%s\n' "${diff_output}" >&2
  return "${status}"
}

deployment_exists() {
  local deployment_name="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" >/dev/null 2>&1
}

smoke_test() {
  require_env FUGUE_SMOKE_URL
  curl -fsS --max-time 10 "${FUGUE_SMOKE_URL}" >/dev/null
}

cleanup_control_plane_automation_tmp() {
  if [[ -n "${CONTROL_PLANE_AUTOMATION_TMP_DIR}" && -d "${CONTROL_PLANE_AUTOMATION_TMP_DIR}" ]]; then
    rm -rf "${CONTROL_PLANE_AUTOMATION_TMP_DIR}"
  fi
}

cleanup_upgrade_override_values() {
  if [[ -n "${UPGRADE_OVERRIDE_VALUES_FILE}" && -f "${UPGRADE_OVERRIDE_VALUES_FILE}" ]]; then
    rm -f "${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${DNS_STATIC_RECORDS_FILE}" && -f "${DNS_STATIC_RECORDS_FILE}" ]]; then
    rm -f "${DNS_STATIC_RECORDS_FILE}"
  fi
  if [[ -n "${PLATFORM_ROUTES_FILE}" && -f "${PLATFORM_ROUTES_FILE}" ]]; then
    rm -f "${PLATFORM_ROUTES_FILE}"
  fi
}

cleanup_tmp_artifacts() {
  cleanup_control_plane_automation_tmp
  cleanup_upgrade_override_values
}

write_upgrade_override_values() {
  UPGRADE_OVERRIDE_VALUES_FILE="$(mktemp -t fugue-upgrade-values.XXXXXX.yaml)"
  cat >"${UPGRADE_OVERRIDE_VALUES_FILE}" <<'EOF'
tolerations:
  - key: node.kubernetes.io/disk-pressure
    operator: Exists
    effect: NoSchedule
api:
  # Keep the control-plane API off tenant-owned runtime nodes. When this
  # lands on a shared app node, tenant traffic can directly inflate page-load
  # latency for fugue-web and other callers.
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
  # Explicit non-empty tolerations prevent Helm's `default` fallback from
  # inheriting the global disk-pressure toleration onto stateless workloads.
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: "1"
      memory: 2Gi
  podDisruptionBudget:
    enabled: true
    minAvailable: 2
controller:
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
snapshotController:
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
topologyLabeler:
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Equal
      effect: NoSchedule
    - key: node-role.kubernetes.io/master
      operator: Equal
      effect: NoSchedule
    - key: fugue.io/dedicated
      operator: Equal
      value: internal
      effect: NoSchedule
edge:
  nodeSelector:
    fugue.io/role.edge: "true"
    fugue.io/schedulable: "true"
  tolerations:
    - key: fugue.io/dedicated
      operator: Equal
      value: edge
      effect: NoSchedule
    - key: fugue.io/tenant
      operator: Exists
      effect: NoSchedule
  caddy:
    enabled: ${FUGUE_EDGE_CADDY_ENABLED}
    listenAddr: $(yaml_quote "${FUGUE_EDGE_CADDY_LISTEN_ADDR}")
    tlsMode: $(yaml_quote "${FUGUE_EDGE_CADDY_TLS_MODE}")
    publicHostPorts:
      enabled: ${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}
      http: ${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP}
      https: ${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS}
cloudnative-pg:
  replicaCount: 2
  priorityClassName: system-cluster-critical
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi
  affinity:
    nodeAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          preference:
            matchExpressions:
              - key: fugue.install/role
                operator: NotIn
                values:
                  - primary
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            topologyKey: kubernetes.io/hostname
            labelSelector:
              matchLabels:
                app.kubernetes.io/name: cloudnative-pg
                app.kubernetes.io/instance: fugue
EOF
  append_upgrade_edge_dynamic_values
  append_upgrade_image_prepull_values
  append_upgrade_dns_values
  printf '%s' "${UPGRADE_OVERRIDE_VALUES_FILE}"
}

append_upgrade_image_prepull_values() {
  local image
  if [[ -z "${FUGUE_IMAGE_PREPULL_IMAGES:-}" ]]; then
    return 0
  fi
  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<'EOF'

imagePrePull:
  enabled: true
  images:
EOF
  printf '%s' "${FUGUE_IMAGE_PREPULL_IMAGES}" | tr ',' '\n' | while IFS= read -r image; do
    image="$(printf '%s' "${image}" | awk '{$1=$1; print}')"
    if [[ -z "${image}" ]]; then
      continue
    fi
    image="${image//\"/\\\"}"
    printf '    - "%s"\n' "${image}" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  done
}

yaml_quote() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "${value}"
}

trim_field() {
  printf '%s' "$1" | awk '{$1=$1; print}'
}

node_selector_country_yaml() {
  local country_code="$1"
  local indent="$2"

  country_code="$(trim_field "${country_code}")"
  if [[ -z "${country_code}" ]]; then
    return 0
  fi
  printf '%sfugue.io/location-country-code: %s\n' "${indent}" "$(yaml_quote "${country_code}")"
}

edge_extra_groups_yaml() {
  local raw="${FUGUE_EDGE_EXTRA_GROUPS:-}"
  local entry name edge_group country_code token_secret edge_region edge_country edge_public_ipv4 edge_public_ipv6 edge_mesh_ip

  raw="${raw//;/$'\n'}"
  if [[ -z "$(trim_field "${raw}")" ]]; then
    return 0
  fi

  printf '  groups:\n'
  while IFS= read -r entry; do
    entry="$(trim_field "${entry}")"
    if [[ -z "${entry}" ]]; then
      continue
    fi
    IFS='|' read -r name edge_group country_code token_secret edge_region edge_country edge_public_ipv4 edge_public_ipv6 edge_mesh_ip _ <<<"${entry}"
    name="$(trim_field "${name}")"
    edge_group="$(trim_field "${edge_group}")"
    country_code="$(trim_field "${country_code}")"
    token_secret="$(trim_field "${token_secret}")"
    edge_region="$(trim_field "${edge_region}")"
    edge_country="$(trim_field "${edge_country}")"
    edge_public_ipv4="$(trim_field "${edge_public_ipv4}")"
    edge_public_ipv6="$(trim_field "${edge_public_ipv6}")"
    edge_mesh_ip="$(trim_field "${edge_mesh_ip}")"
    if [[ -z "${name}" || -z "${edge_group}" || -z "${country_code}" || -z "${token_secret}" ]]; then
      fail "FUGUE_EDGE_EXTRA_GROUPS entries must be name|edge_group_id|country_code|token_secret_name[|region|country|public_ipv4|public_ipv6|mesh_ip]"
    fi
    if [[ -z "${edge_country}" ]]; then
      edge_country="${country_code}"
    fi
    printf '    - name: %s\n' "$(yaml_quote "${name}")"
    printf '      edgeGroupID: %s\n' "$(yaml_quote "${edge_group}")"
    printf '      tokenSecret:\n'
    printf '        name: %s\n' "$(yaml_quote "${token_secret}")"
    printf '        key: "FUGUE_EDGE_TOKEN"\n'
    printf '      nodeSelector:\n'
    printf '        fugue.io/role.edge: "true"\n'
    printf '        fugue.io/schedulable: "true"\n'
    printf '        fugue.io/location-country-code: %s\n' "$(yaml_quote "${country_code}")"
    if [[ -n "${edge_region}" || -n "${edge_country}" || -n "${edge_public_ipv4}" || -n "${edge_public_ipv6}" || -n "${edge_mesh_ip}" ]]; then
      printf '      extraEnv:\n'
      if [[ -n "${edge_region}" ]]; then
        printf '        - name: FUGUE_EDGE_REGION\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_region}")"
      fi
      if [[ -n "${edge_country}" ]]; then
        printf '        - name: FUGUE_EDGE_COUNTRY\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_country}")"
      fi
      if [[ -n "${edge_public_ipv4}" ]]; then
        printf '        - name: FUGUE_EDGE_PUBLIC_IPV4\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_public_ipv4}")"
      fi
      if [[ -n "${edge_public_ipv6}" ]]; then
        printf '        - name: FUGUE_EDGE_PUBLIC_IPV6\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_public_ipv6}")"
      fi
      if [[ -n "${edge_mesh_ip}" ]]; then
        printf '        - name: FUGUE_EDGE_MESH_IP\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_mesh_ip}")"
      fi
    fi
  done <<<"${raw}"
}

dns_extra_groups_yaml() {
  local raw="${FUGUE_DNS_EXTRA_GROUPS:-}"
  local entry name edge_group country_code answer_ips token_secret answer_ip

  raw="${raw//;/$'\n'}"
  if [[ -z "$(trim_field "${raw}")" ]]; then
    return 0
  fi

  printf '  groups:\n'
  while IFS= read -r entry; do
    entry="$(trim_field "${entry}")"
    if [[ -z "${entry}" ]]; then
      continue
    fi
    IFS='|' read -r name edge_group country_code answer_ips token_secret _ <<<"${entry}"
    name="$(trim_field "${name}")"
    edge_group="$(trim_field "${edge_group}")"
    country_code="$(trim_field "${country_code}")"
    answer_ips="$(trim_field "${answer_ips}")"
    token_secret="$(trim_field "${token_secret}")"
    if [[ -z "${name}" || -z "${edge_group}" || -z "${country_code}" || -z "${answer_ips}" || -z "${token_secret}" ]]; then
      fail "FUGUE_DNS_EXTRA_GROUPS entries must be name|edge_group_id|country_code|answer_ips|token_secret_name"
    fi
    if [[ "$(dns_answer_ip_count "${answer_ips}")" == "0" ]]; then
      fail "FUGUE_DNS_EXTRA_GROUPS entry ${name} must contain at least one answer IP"
    fi
    printf '    - name: %s\n' "$(yaml_quote "${name}")"
    printf '      edgeGroupID: %s\n' "$(yaml_quote "${edge_group}")"
    printf '      tokenSecret:\n'
    printf '        name: %s\n' "$(yaml_quote "${token_secret}")"
    printf '        key: "FUGUE_EDGE_TOKEN"\n'
    printf '      nodeSelector:\n'
    printf '        fugue.io/role.dns: "true"\n'
    printf '        fugue.io/schedulable: "true"\n'
    printf '        fugue.io/location-country-code: %s\n' "$(yaml_quote "${country_code}")"
    printf '      answerIPs:\n'
    while IFS= read -r answer_ip; do
      printf '        - %s\n' "$(yaml_quote "${answer_ip}")"
    done < <(dns_answer_ips_lines "${answer_ips}")
  done <<<"${raw}"
}

append_upgrade_edge_dynamic_values() {
  local edge_region edge_country edge_public_hostname edge_public_ipv4 edge_public_ipv6 edge_mesh_ip

  edge_region="$(trim_field "${FUGUE_EDGE_REGION:-}")"
  edge_country="$(trim_field "${FUGUE_EDGE_COUNTRY:-}")"
  edge_public_hostname="$(trim_field "${FUGUE_EDGE_PUBLIC_HOSTNAME:-}")"
  edge_public_ipv4="$(trim_field "${FUGUE_EDGE_PUBLIC_IPV4:-}")"
  edge_public_ipv6="$(trim_field "${FUGUE_EDGE_PUBLIC_IPV6:-}")"
  edge_mesh_ip="$(trim_field "${FUGUE_EDGE_MESH_IP:-}")"

  if [[ -z "$(trim_field "${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE:-}")" && -z "$(trim_field "${FUGUE_EDGE_EXTRA_GROUPS:-}")" && -z "${edge_region}${edge_country}${edge_public_hostname}${edge_public_ipv4}${edge_public_ipv6}${edge_mesh_ip}" ]]; then
    return 0
  fi

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF

edge:
EOF
  if [[ -n "${edge_region}" ]]; then
    printf '  region: %s\n' "$(yaml_quote "${edge_region}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_country}" ]]; then
    printf '  country: %s\n' "$(yaml_quote "${edge_country}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_public_hostname}" ]]; then
    printf '  publicHostname: %s\n' "$(yaml_quote "${edge_public_hostname}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_public_ipv4}" ]]; then
    printf '  publicIPv4: %s\n' "$(yaml_quote "${edge_public_ipv4}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_public_ipv6}" ]]; then
    printf '  publicIPv6: %s\n' "$(yaml_quote "${edge_public_ipv6}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_mesh_ip}" ]]; then
    printf '  meshIP: %s\n' "$(yaml_quote "${edge_mesh_ip}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF
  nodeSelector:
    fugue.io/role.edge: "true"
    fugue.io/schedulable: "true"
EOF
  node_selector_country_yaml "${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE}" "    " >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  edge_extra_groups_yaml >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
}

dns_answer_ips_lines() {
  local raw="$1"
  local answer_ip
  raw="${raw//;/,}"

  printf '%s\n' "${raw}" | tr ',' '\n' | while IFS= read -r answer_ip; do
    answer_ip="$(printf '%s' "${answer_ip}" | awk '{$1=$1; print}')"
    if [[ -n "${answer_ip}" ]]; then
      printf '%s\n' "${answer_ip}"
    fi
  done
}

dns_answer_ip_count() {
  dns_answer_ips_lines "$1" | awk 'NF > 0 {count++} END {print count + 0}'
}

csv_lines() {
  local raw="$1"
  raw="${raw//;/,}"
  printf '%s\n' "${raw}" | tr ',' '\n' | while IFS= read -r value; do
    value="$(trim_field "${value}")"
    if [[ -n "${value}" ]]; then
      printf '%s\n' "${value}"
    fi
  done
}

append_upgrade_dns_values() {
  local answer_ip
  local nameserver
  local rendered_answer_ips=0

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF

dns:
  enabled: ${FUGUE_DNS_ENABLED}
EOF

  if [[ "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    return 0
  fi

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<'EOF'
  answerIPs:
EOF
  while IFS= read -r answer_ip; do
    printf '    - %s\n' "$(yaml_quote "${answer_ip}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    rendered_answer_ips=$((rendered_answer_ips + 1))
  done < <(dns_answer_ips_lines "${FUGUE_DNS_ANSWER_IPS}")
  if (( rendered_answer_ips == 0 )); then
    fail "FUGUE_DNS_ANSWER_IPS must contain at least one non-empty IP when FUGUE_DNS_ENABLED=true"
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-}")" ]]; then
    printf '  routeAAnswerIPs:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    while IFS= read -r answer_ip; do
      printf '    - %s\n' "$(yaml_quote "${answer_ip}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    done < <(dns_answer_ips_lines "${FUGUE_DNS_ROUTE_A_ANSWER_IPS}")
  fi

  {
    printf '  nodeSelector:\n'
    printf '    fugue.io/role.dns: "true"\n'
    printf '    fugue.io/schedulable: "true"\n'
    node_selector_country_yaml "${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE}" "    "
    printf '  tolerations:\n'
    printf '    - key: fugue.io/dedicated\n'
    printf '      operator: Equal\n'
    printf '      value: dns\n'
    printf '      effect: NoSchedule\n'
    printf '    - key: fugue.io/dedicated\n'
    printf '      operator: Equal\n'
    printf '      value: edge\n'
    printf '      effect: NoSchedule\n'
    printf '    - key: fugue.io/tenant\n'
    printf '      operator: Exists\n'
    printf '      effect: NoSchedule\n'
    printf '  publicHostPorts:\n'
    printf '    enabled: %s\n' "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}"
    printf '  udpAddr: %s\n' "$(yaml_quote "${FUGUE_DNS_UDP_ADDR}")"
    printf '  tcpAddr: %s\n' "$(yaml_quote "${FUGUE_DNS_TCP_ADDR}")"
    printf '  zone: %s\n' "$(yaml_quote "${FUGUE_DNS_ZONE}")"
    printf '  ttl: %s\n' "${FUGUE_DNS_TTL}"
  } >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  if [[ -n "$(trim_field "${FUGUE_DNS_NAMESERVERS:-}")" ]]; then
    printf '  nameservers:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    while IFS= read -r nameserver; do
      printf '    - %s\n' "$(yaml_quote "${nameserver}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    done < <(csv_lines "${FUGUE_DNS_NAMESERVERS}")
  fi
  dns_extra_groups_yaml >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
}

build_dns_helm_set_args() {
  local answer_ip
  local nameserver
  local index=0

  DNS_HELM_SET_ARGS=(
    --set "dns.enabled=${FUGUE_DNS_ENABLED}"
  )
  if [[ -n "$(trim_field "${FUGUE_DNS_STATIC_RECORDS_JSON:-}")" ]]; then
    DNS_STATIC_RECORDS_FILE="$(mktemp -t fugue-dns-static-records.XXXXXX.json)"
    printf '%s' "${FUGUE_DNS_STATIC_RECORDS_JSON}" >"${DNS_STATIC_RECORDS_FILE}"
    DNS_HELM_SET_ARGS+=(--set-file "api.dnsStaticRecordsJSON=${DNS_STATIC_RECORDS_FILE}")
  fi
  if [[ -n "$(trim_field "${FUGUE_PLATFORM_ROUTES_JSON:-}")" ]]; then
    PLATFORM_ROUTES_FILE="$(mktemp -t fugue-platform-routes.XXXXXX.json)"
    printf '%s' "${FUGUE_PLATFORM_ROUTES_JSON}" >"${PLATFORM_ROUTES_FILE}"
    DNS_HELM_SET_ARGS+=(--set-file "api.platformRoutesJSON=${PLATFORM_ROUTES_FILE}")
  fi
  if [[ "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    return 0
  fi

  DNS_HELM_SET_ARGS+=(
    --set "dns.publicHostPorts.enabled=${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}"
    --set-string "dns.udpAddr=${FUGUE_DNS_UDP_ADDR}"
    --set-string "dns.tcpAddr=${FUGUE_DNS_TCP_ADDR}"
    --set-string "dns.zone=${FUGUE_DNS_ZONE}"
    --set "dns.ttl=${FUGUE_DNS_TTL}"
  )
  while IFS= read -r answer_ip; do
    DNS_HELM_SET_ARGS+=(--set-string "dns.answerIPs[${index}]=${answer_ip}")
    index=$((index + 1))
  done < <(dns_answer_ips_lines "${FUGUE_DNS_ANSWER_IPS}")
  if (( index == 0 )); then
    fail "FUGUE_DNS_ANSWER_IPS must contain at least one non-empty IP when FUGUE_DNS_ENABLED=true"
  fi
  index=0
  while IFS= read -r answer_ip; do
    DNS_HELM_SET_ARGS+=(--set-string "dns.routeAAnswerIPs[${index}]=${answer_ip}")
    index=$((index + 1))
  done < <(dns_answer_ips_lines "${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-}")
  index=0
  while IFS= read -r nameserver; do
    DNS_HELM_SET_ARGS+=(--set-string "dns.nameservers[${index}]=${nameserver}")
    index=$((index + 1))
  done < <(csv_lines "${FUGUE_DNS_NAMESERVERS:-}")
}

use_local_control_plane_automation_bundle_from_dir() {
  local bundle_dir="$1"

  [[ -r "${bundle_dir}/hosts.env" ]] || return 1
  [[ -r "${bundle_dir}/id_ed25519" ]] || return 1
  [[ -r "${bundle_dir}/known_hosts" ]] || return 1

  export FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE="${bundle_dir}/hosts.env"
  export FUGUE_CONTROL_PLANE_SSH_KEY_FILE="${bundle_dir}/id_ed25519"
  export FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE="${bundle_dir}/known_hosts"
  export FUGUE_USE_CONTROL_PLANE_AUTOMATION_SSH=true
  log_stderr "using local control-plane automation bundle from ${bundle_dir}"
  return 0
}

prepare_control_plane_automation_ssh() {
  if [[ -n "${FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE:-}" && -r "${FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE}" ]] && \
     [[ -n "${FUGUE_CONTROL_PLANE_SSH_KEY_FILE:-}" && -r "${FUGUE_CONTROL_PLANE_SSH_KEY_FILE}" ]] && \
     [[ -n "${FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE:-}" && -r "${FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}" ]]; then
    export FUGUE_USE_CONTROL_PLANE_AUTOMATION_SSH=true
    return
  fi

  if use_local_control_plane_automation_bundle_from_dir "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}"; then
    return
  fi
  if [[ "${LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR}" != "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}" ]] && \
     use_local_control_plane_automation_bundle_from_dir "${LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR}"; then
    return
  fi
  fail "missing local control-plane automation bundle on this server; run scripts/bootstrap_control_plane_automation.sh or scripts/install_fugue_ha.sh to install it"
}

load_control_plane_hosts_env() {
  if [[ "${CONTROL_PLANE_HOSTS_ENV_LOADED}" == "true" ]]; then
    return
  fi
  CONTROL_PLANE_HOSTS_ENV_LOADED="true"
  # shellcheck disable=SC1090
  source "${FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE}"
}

primary_control_plane_ssh_login() {
  if [[ -z "${PRIMARY_CONTROL_PLANE_SSH_HOST}" ]]; then
    resolve_primary_control_plane_ssh_target ""
  fi
  [[ -n "${PRIMARY_CONTROL_PLANE_SSH_HOST}" ]] || fail "primary control-plane SSH host is not configured"
  if [[ -n "${PRIMARY_CONTROL_PLANE_SSH_USER}" ]]; then
    printf '%s@%s' "${PRIMARY_CONTROL_PLANE_SSH_USER}" "${PRIMARY_CONTROL_PLANE_SSH_HOST}"
    return
  fi
  printf '%s' "${PRIMARY_CONTROL_PLANE_SSH_HOST}"
}

ssh_host_port_is_reachable() {
  local host="$1"
  local port="$2"

  [[ -n "${host}" && -n "${port}" ]] || return 1
  if command_exists nc; then
    nc -z -w 3 "${host}" "${port}" >/dev/null 2>&1 && return 0
    nc -z -G 3 "${host}" "${port}" >/dev/null 2>&1 && return 0
  fi
  if command_exists timeout; then
    timeout 3 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1 && return 0
  fi
  return 1
}

primary_node_address_candidates() {
  local primary_node_name="$1"

  [[ -n "${primary_node_name}" ]] || return 0
  ${KUBECTL} get node "${primary_node_name}" -o jsonpath='{range .status.addresses[?(@.type=="ExternalIP")]}{.address}{"\n"}{end}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{"\n"}{end}{range .status.addresses[?(@.type=="Hostname")]}{.address}{"\n"}{end}' 2>/dev/null | awk 'NF > 0 && !seen[$0]++'
}

resolve_primary_control_plane_ssh_target() {
  local primary_node_name="$1"
  local configured_host=""
  local candidate=""

  load_control_plane_hosts_env
  PRIMARY_CONTROL_PLANE_SSH_HOST="${FUGUE_NODE1_HOST:-${FUGUE_NODE1_ALIAS:-}}"
  PRIMARY_CONTROL_PLANE_SSH_USER="${FUGUE_NODE1_USER:-}"
  PRIMARY_CONTROL_PLANE_SSH_PORT="${FUGUE_NODE1_PORT:-22}"
  PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS=""
  configured_host="${PRIMARY_CONTROL_PLANE_SSH_HOST}"

  [[ -n "${PRIMARY_CONTROL_PLANE_SSH_HOST}" ]] || fail "primary control-plane SSH host is not configured"
  if [[ "${FUGUE_CONTROL_PLANE_SSH_NODE_ADDRESS_FALLBACK:-true}" != "true" ]]; then
    return
  fi
  if ssh_host_port_is_reachable "${PRIMARY_CONTROL_PLANE_SSH_HOST}" "${PRIMARY_CONTROL_PLANE_SSH_PORT}"; then
    return
  fi

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    [[ "${candidate}" != "${configured_host}" ]] || continue
    if ssh_host_port_is_reachable "${candidate}" "${PRIMARY_CONTROL_PLANE_SSH_PORT}"; then
      PRIMARY_CONTROL_PLANE_SSH_HOST="${candidate}"
      PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS="${configured_host}"
      log_stderr "configured primary SSH host ${configured_host}:${PRIMARY_CONTROL_PLANE_SSH_PORT} is not reachable; using Kubernetes node address ${candidate}:${PRIMARY_CONTROL_PLANE_SSH_PORT}"
      return
    fi
  done < <(primary_node_address_candidates "${primary_node_name}")
}

build_primary_control_plane_ssh_opts() {
  local primary_node_name="$1"

  resolve_primary_control_plane_ssh_target "${primary_node_name}"
  PRIMARY_CONTROL_PLANE_SSH_OPTS=(
    -o BatchMode=yes
    -o ConnectTimeout=15
    -o ServerAliveInterval=15
    -o ServerAliveCountMax=3
    -o IdentitiesOnly=yes
    -i "${FUGUE_CONTROL_PLANE_SSH_KEY_FILE}"
    -o StrictHostKeyChecking=yes
    -o UserKnownHostsFile="${FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}"
  )
  if [[ -n "${PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS}" ]]; then
    PRIMARY_CONTROL_PLANE_SSH_OPTS+=(-o "HostKeyAlias=${PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS}")
  fi
  if [[ -n "${PRIMARY_CONTROL_PLANE_SSH_PORT}" ]]; then
    PRIMARY_CONTROL_PLANE_SSH_OPTS+=(-p "${PRIMARY_CONTROL_PLANE_SSH_PORT}")
  fi
}

detect_primary_node_name() {
  ${KUBECTL} get nodes -l fugue.install/role=primary -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

primary_node_is_ready() {
  local node_name="$1"
  local status=""

  if command_exists timeout; then
    status="$(timeout 15s ${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' 2>/dev/null || true)"
  else
    status="$(${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' 2>/dev/null || true)"
  fi
  [[ "${status}" == "True" ]]
}

primary_node_has_disk_pressure() {
  local node_name="$1"
  local status=""
  if command_exists timeout; then
    status="$(timeout 15s ${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}' 2>/dev/null || true)"
  else
    status="$(${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}' 2>/dev/null || true)"
  fi
  [[ "${status}" == "True" ]]
}

run_primary_host_root_command() {
  local primary_node_name="$1"
  local cmd="$2"
  local local_hostname=""
  local local_hostname_short=""

  local_hostname="$(hostname 2>/dev/null || true)"
  local_hostname_short="$(hostname -s 2>/dev/null || true)"
  if [[ "${local_hostname}" == "${primary_node_name}" || "${local_hostname_short}" == "${primary_node_name}" ]]; then
    if sudo -n true >/dev/null 2>&1; then
      sudo -n bash -lc "${cmd}"
      return
    fi
    log_stderr "local primary host ${primary_node_name} requires interactive sudo; falling back to automation SSH"
  fi

  prepare_control_plane_automation_ssh
  build_primary_control_plane_ssh_opts "${primary_node_name}"
  ssh -n "${PRIMARY_CONTROL_PLANE_SSH_OPTS[@]}" "$(primary_control_plane_ssh_login)" \
    "sudo -n bash -lc $(printf '%q' "${cmd}")"
}

wait_for_primary_node_ready() {
  local primary_node_name="$1"
  local attempt
  local max_attempts

  if ! [[ "${PRIMARY_NODE_READY_POLL_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_NODE_READY_POLL_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_NODE_READY_POLL_SECONDS must be a positive integer"
  fi
  if ! [[ "${PRIMARY_NODE_READY_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_NODE_READY_TIMEOUT_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_NODE_READY_TIMEOUT_SECONDS must be a positive integer"
  fi

  max_attempts=$(( (PRIMARY_NODE_READY_TIMEOUT_SECONDS + PRIMARY_NODE_READY_POLL_SECONDS - 1) / PRIMARY_NODE_READY_POLL_SECONDS ))
  log "waiting up to ${PRIMARY_NODE_READY_TIMEOUT_SECONDS}s for primary node ${primary_node_name} to report Ready"

  for attempt in $(seq 1 "${max_attempts}"); do
    if primary_node_is_ready "${primary_node_name}"; then
      return 0
    fi
    sleep "${PRIMARY_NODE_READY_POLL_SECONDS}"
  done
  return 1
}

local_kube_api_is_ready() {
  local readyz=""

  if command_exists timeout; then
    readyz="$(timeout 10s ${KUBECTL} get --raw='/readyz' 2>/dev/null || true)"
  else
    readyz="$(${KUBECTL} get --raw='/readyz' 2>/dev/null || true)"
  fi
  [[ "${readyz}" == *"ok"* ]]
}

wait_for_local_kube_api_ready() {
  local attempt
  local max_attempts

  if ! [[ "${LOCAL_KUBE_API_READY_POLL_SECONDS}" =~ ^[0-9]+$ ]] || (( LOCAL_KUBE_API_READY_POLL_SECONDS <= 0 )); then
    fail "FUGUE_LOCAL_KUBE_API_READY_POLL_SECONDS must be a positive integer"
  fi
  if ! [[ "${LOCAL_KUBE_API_READY_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( LOCAL_KUBE_API_READY_TIMEOUT_SECONDS <= 0 )); then
    fail "FUGUE_LOCAL_KUBE_API_READY_TIMEOUT_SECONDS must be a positive integer"
  fi

  max_attempts=$(( (LOCAL_KUBE_API_READY_TIMEOUT_SECONDS + LOCAL_KUBE_API_READY_POLL_SECONDS - 1) / LOCAL_KUBE_API_READY_POLL_SECONDS ))
  log "waiting up to ${LOCAL_KUBE_API_READY_TIMEOUT_SECONDS}s for local kube-apiserver to answer /readyz"

  for attempt in $(seq 1 "${max_attempts}"); do
    if local_kube_api_is_ready; then
      return 0
    fi
    sleep "${LOCAL_KUBE_API_READY_POLL_SECONDS}"
  done
  return 1
}

wait_for_primary_disk_pressure_clear() {
  local primary_node_name="$1"
  local attempt
  local max_attempts

  if ! [[ "${PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS must be a positive integer"
  fi
  if ! [[ "${PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS must be a positive integer"
  fi

  max_attempts=$(( (PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS + PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS - 1) / PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS ))
  log "waiting up to ${PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS}s for primary node ${primary_node_name} to clear DiskPressure"

  for attempt in $(seq 1 "${max_attempts}"); do
    if ! primary_node_has_disk_pressure "${primary_node_name}"; then
      return 0
    fi
    sleep "${PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS}"
  done
  return 1
}

release_pod_selector() {
  printf 'app.kubernetes.io/instance=%s,app.kubernetes.io/name=fugue' "${FUGUE_RELEASE_NAME}"
}

release_pod_names_by_phase() {
  local phase="$1"
  local output=""

  if command_exists timeout; then
    output="$(timeout 30s ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(release_pod_selector)" \
      --field-selector "status.phase=${phase}" \
      -o name 2>/dev/null || true)"
  else
    output="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(release_pod_selector)" \
      --field-selector "status.phase=${phase}" \
      -o name 2>/dev/null || true)"
  fi

  printf '%s\n' "${output}" | awk 'NF > 0'
}

delete_release_pod_batch() {
  local phase="$1"
  shift
  [[ "$#" -gt 0 ]] || return 0

  if command_exists timeout; then
    if timeout 60s ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
      --ignore-not-found \
      --wait=false "$@" >/dev/null 2>&1; then
      return 0
    fi
    log "warning: failed to delete a batch of ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"
    return 0
  fi

  if ! ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
    --ignore-not-found \
    --wait=false "$@" >/dev/null 2>&1; then
    log "warning: failed to delete a batch of ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"
  fi
}

prune_release_pods_by_phase() {
  local phase="$1"
  local names=""
  local count=""
  local name=""
  local -a batch=()

  names="$(release_pod_names_by_phase "${phase}")"
  count="$(printf '%s\n' "${names}" | awk 'NF > 0 {count++} END {print count + 0}')"
  [[ "${count}" != "0" ]] || return 0
  log "deleting ${count} ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"

  while IFS= read -r name; do
    [[ -n "${name}" ]] || continue
    batch+=("${name}")
    if (( ${#batch[@]} == 50 )); then
      delete_release_pod_batch "${phase}" "${batch[@]}"
      batch=()
    fi
  done <<< "${names}"

  if (( ${#batch[@]} > 0 )); then
    delete_release_pod_batch "${phase}" "${batch[@]}"
  fi
}

prune_terminated_release_pods() {
  prune_release_pods_by_phase Failed
  prune_release_pods_by_phase Succeeded
  prune_release_pods_by_phase Unknown
}

unhealthy_node_names() {
  ${KUBECTL} get nodes --no-headers 2>/dev/null | awk '$2 !~ /^Ready/ {print $1}'
}

is_stateless_release_component() {
  local component="$1"
  case "${component}" in
    api|controller|node-janitor|topology-labeler|edge|dns|edge-*|dns-*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

stateless_release_pod_names_on_node() {
  local node_name="$1"
  local line=""
  local pod_name=""
  local component=""

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
    -l "$(release_pod_selector)" \
    --field-selector "spec.nodeName=${node_name}" \
    -o go-template='{{range .items}}{{.metadata.name}}{{"\t"}}{{index .metadata.labels "app.kubernetes.io/component"}}{{"\n"}}{{end}}' 2>/dev/null |
    while IFS=$'\t' read -r pod_name component; do
      [[ -n "${pod_name}" ]] || continue
      if is_stateless_release_component "${component}"; then
        printf 'pod/%s\n' "${pod_name}"
      fi
    done
}

force_delete_release_pods_on_unhealthy_nodes() {
  local node_name=""
  local names=""
  local count=""
  local pod=""
  local -a batch=()

  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    names="$(stateless_release_pod_names_on_node "${node_name}")"
    count="$(printf '%s\n' "${names}" | awk 'NF > 0 {count++} END {print count + 0}')"
    [[ "${count}" != "0" ]] || continue

    log "force deleting ${count} stateless Fugue release pods on unhealthy node ${node_name}"
    batch=()
    while IFS= read -r pod; do
      [[ -n "${pod}" ]] || continue
      batch+=("${pod}")
      if (( ${#batch[@]} == 50 )); then
        ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
          --ignore-not-found \
          --force \
          --grace-period=0 \
          --wait=false "${batch[@]}" >/dev/null 2>&1 || true
        batch=()
      fi
    done <<< "${names}"
    if (( ${#batch[@]} > 0 )); then
      ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
        --ignore-not-found \
        --force \
        --grace-period=0 \
        --wait=false "${batch[@]}" >/dev/null 2>&1 || true
    fi
  done < <(unhealthy_node_names)
}

recover_primary_node_if_needed() {
  local primary_node_name=""
  local restart_cmd=""
  local restarted_via_ssh="false"

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary node recovery because the primary node could not be identified"
    return 0
  fi

  prune_terminated_release_pods
  force_delete_release_pods_on_unhealthy_nodes

  if primary_node_is_ready "${primary_node_name}"; then
    return 0
  fi

  log "primary node ${primary_node_name} is NotReady; restarting k3s on the primary host"
  restart_cmd="$(cat <<'EOF'
set -euo pipefail

run_bounded_host_command() {
  local timeout_seconds="$1"
  local pid=""
  local state=""
  local deadline=""
  shift

  "$@" &
  pid="$!"
  deadline=$((SECONDS + timeout_seconds))

  while true; do
    state="$(awk '{print $3}' "/proc/${pid}/stat" 2>/dev/null || true)"
    if [[ -z "${state}" || "${state}" == "Z" ]]; then
      wait "${pid}"
      return $?
    fi
    if (( SECONDS >= deadline )); then
      printf '[fugue-upgrade][primary-recovery] timed out after %ss: %s\n' "${timeout_seconds}" "$*" >&2
      kill "${pid}" >/dev/null 2>&1 || true
      sleep 2
      kill -KILL "${pid}" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 1
  done
}

if command -v k3s >/dev/null 2>&1; then
  run_bounded_host_command 90 k3s crictl rmi --prune >/tmp/fugue-primary-node-image-prune.log 2>&1 || true
fi

run_bounded_host_command 120 systemctl restart k3s
systemctl is-active --quiet k3s
EOF
)"

  if run_primary_host_root_command "${primary_node_name}" "${restart_cmd}"; then
    restarted_via_ssh="true"
  else
    log "warning: failed to restart k3s on ${primary_node_name} over SSH; waiting to see if cleanup alone restores node readiness"
  fi

  if [[ "${restarted_via_ssh}" == "true" ]] && ! wait_for_local_kube_api_ready; then
    fail "local kube-apiserver did not recover after restarting k3s on primary node ${primary_node_name}"
  fi

  if ! wait_for_primary_node_ready "${primary_node_name}"; then
    if [[ "${restarted_via_ssh}" == "true" ]]; then
      fail "primary node ${primary_node_name} remained NotReady after restarting k3s"
    fi
    fail "primary node ${primary_node_name} remained NotReady after cleanup and SSH restart fallback"
  fi

  prune_terminated_release_pods
}

control_plane_postgres_selector() {
  printf 'app.kubernetes.io/component=postgres,app.kubernetes.io/instance=%s,app.kubernetes.io/name=fugue' "${FUGUE_RELEASE_NAME}"
}

control_plane_postgres_pod_status_lines() {
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
    -l "$(control_plane_postgres_selector)" \
    --sort-by=.metadata.creationTimestamp \
    -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready,PHASE:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount \
    --no-headers 2>/dev/null | tail -n 10
}

control_plane_postgres_pod_summary() {
  control_plane_postgres_pod_status_lines | awk '
    NF > 0 {
      printf "%s%s(ready=%s phase=%s restarts=%s)", sep, $1, $2, $3, $4
      sep = ", "
    }
    END {
      if (NR == 0) {
        printf "none"
      }
    }
  '
}

control_plane_postgres_pod_names() {
  control_plane_postgres_pod_status_lines | awk 'NF > 0 {lines[++count]=$1} END {for (i=count; i>=1; i--) print lines[i]}'
}

control_plane_postgres_logs() {
  local pod_name="$1"
  local logs=""

  [[ -n "${pod_name}" ]] || return 0
  if command_exists timeout; then
    logs="$(timeout 15s ${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --previous --tail=200 2>/dev/null || true)"
  else
    logs="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --previous --tail=200 2>/dev/null || true)"
  fi
  if [[ -z "${logs}" ]]; then
    if command_exists timeout; then
      logs="$(timeout 15s ${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --tail=200 2>/dev/null || true)"
    else
      logs="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --tail=200 2>/dev/null || true)"
    fi
  fi
  printf '%s' "${logs}"
}

control_plane_postgres_has_invalid_checkpoint() {
  local pod_name="$1"
  local logs=""

  logs="$(control_plane_postgres_logs "${pod_name}")"
  [[ "${logs}" == *"invalid resource manager ID in checkpoint record"* ]] || \
    [[ "${logs}" == *"could not locate a valid checkpoint record"* ]]
}

control_plane_postgres_has_ready_pod() {
  control_plane_postgres_pod_status_lines | awk 'NF > 0 && $2 == "true" && $3 == "Running" {found=1} END {exit found ? 0 : 1}'
}

invalid_checkpoint_control_plane_postgres_pod_name() {
  local pod_name=""
  local attempt=""
  local pod_summary=""

  for attempt in $(seq 1 6); do
    pod_summary="$(control_plane_postgres_pod_summary)"
    while IFS= read -r pod_name; do
      [[ -n "${pod_name}" ]] || continue
      if control_plane_postgres_has_invalid_checkpoint "${pod_name}"; then
        printf '%s' "${pod_name}"
        return 0
      fi
    done < <(control_plane_postgres_pod_names)

    if control_plane_postgres_has_ready_pod; then
      return 1
    fi

    if (( attempt == 6 )); then
      log "control-plane postgres still has no ready pods after ${attempt} checks; inspected ${pod_summary}"
      return 2
    fi

    log "control-plane postgres has no ready pods yet; inspected ${pod_summary}; waiting before checking WAL corruption again"
    sleep 5
  done

  return 1
}

wait_for_control_plane_postgres_pods_gone() {
  local attempt
  local names=""

  for attempt in $(seq 1 24); do
    names="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(control_plane_postgres_selector)" \
      -o go-template='{{range .items}}{{if eq .status.phase "Running"}}{{.metadata.name}} {{end}}{{end}}' 2>/dev/null || true)"
    if [[ -z "${names}" ]]; then
      return 0
    fi
    sleep 5
  done
  return 1
}

recover_primary_postgres_if_needed() {
  local primary_node_name=""
  local postgres_pod_name=""
  local original_replicas=""
  local repair_cmd=""
  local detect_status=0

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary postgres recovery because the primary node could not be identified"
    return 0
  fi

  postgres_pod_name="$(invalid_checkpoint_control_plane_postgres_pod_name)" || detect_status=$?
  case "${detect_status}" in
    0)
      ;;
    1)
      return 0
      ;;
    2)
      fail "control-plane postgres had no ready pods before upgrade, but no invalid-checkpoint signature was found in recent logs"
      ;;
    *)
      fail "control-plane postgres recovery pre-check failed with unexpected status ${detect_status}"
      ;;
  esac

  if [[ -z "${postgres_pod_name}" ]]; then
    fail "control-plane postgres recovery pre-check succeeded without returning a pod name"
  fi

  log "detected invalid checkpoint in control-plane postgres pod ${postgres_pod_name}; resetting WAL on the primary host"
  original_replicas="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  [[ -n "${original_replicas}" ]] || original_replicas="1"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas=0 >/dev/null
  if ! wait_for_control_plane_postgres_pods_gone; then
    ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas="${original_replicas}" >/dev/null || true
    fail "control-plane postgres pods did not terminate before WAL recovery"
  fi

  repair_cmd="$(cat <<EOF
set -euo pipefail

log() {
  printf '[fugue-upgrade][primary-postgres-repair] %s\n' "\$*"
}

postgres_root=$(printf '%q' "${PRIMARY_POSTGRES_DATA_ROOT}")
pgdata="\${postgres_root}/pgdata"
backup_dir="\${postgres_root}/pgdata.pre-resetwal-\$(date -u +%Y%m%dT%H%M%SZ)"
postgres_image=$(printf '%q' "${PRIMARY_POSTGRES_IMAGE}")
repair_id="fugue-postgres-repair-\$(date +%s)"

cleanup() {
  k3s ctr tasks kill "\${repair_id}" >/dev/null 2>&1 || true
  k3s ctr containers rm "\${repair_id}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

if [[ ! -d "\${pgdata}" ]]; then
  log "postgres data directory \${pgdata} does not exist; skipping WAL recovery"
  exit 0
fi

cp -a "\${pgdata}" "\${backup_dir}"
log "backed up \${pgdata} to \${backup_dir}"

rm -f "\${pgdata}/postmaster.pid"

if ! k3s ctr images ls | awk 'NR > 1 {print \$1}' | grep -Fxq "\${postgres_image}"; then
  log "pulling \${postgres_image}"
  k3s ctr images pull "\${postgres_image}"
fi

log "running pg_resetwal against \${pgdata}"
timeout 300s k3s ctr run --rm \
  --mount type=bind,src="\${postgres_root}",dst=/var/lib/postgresql/data,options=rbind:rw \
  "\${postgres_image}" "\${repair_id}" \
  sh -lc 'set -euo pipefail; chown -R 70:70 /var/lib/postgresql/data; su-exec postgres pg_resetwal -f /var/lib/postgresql/data/pgdata'

log "pg_resetwal completed"
EOF
)"
  if ! run_primary_host_root_command "${primary_node_name}" "${repair_cmd}"; then
    ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas="${original_replicas}" >/dev/null || true
    fail "control-plane postgres WAL recovery failed"
  fi

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas="${original_replicas}" >/dev/null
}

restore_primary_mesh_network_if_needed() {
  local primary_node_name=""
  local primary_config=""
  local primary_private_ip=""
  local primary_private_ip_cmd=""
  local primary_mesh_ip=""
  local current_node_ip=""
  local current_external_ip=""
  local current_flannel_iface=""
  local current_flannel_external_ip=""
  local filtered_config=""
  local patched_config=""
  local restore_cmd=""
  local backup_path=""

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary mesh restore because the primary node could not be identified"
    return 0
  fi

  primary_mesh_ip="$(run_primary_host_root_command "${primary_node_name}" "if command -v tailscale >/dev/null 2>&1; then tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}'; fi" | tr -d '\r')"
  if [[ -z "${primary_mesh_ip}" ]]; then
    log "skip primary mesh restore because tailscale has no IPv4 on ${primary_node_name}"
    return 0
  fi

  primary_private_ip_cmd="$(cat <<'EOF'
ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}'
EOF
)"
  primary_private_ip="$(run_primary_host_root_command "${primary_node_name}" "${primary_private_ip_cmd}" | tr -d '\r')"
  [[ -n "${primary_private_ip}" ]] || fail "failed to detect the primary private IP while restoring mesh networking"

  primary_config="$(run_primary_host_root_command "${primary_node_name}" "cat /etc/rancher/k3s/config.yaml 2>/dev/null || true")"
  if [[ -z "${primary_config}" ]]; then
    log "skip primary mesh restore because /etc/rancher/k3s/config.yaml is absent on ${primary_node_name}"
    return 0
  fi

  current_node_ip="$(printf '%s\n' "${primary_config}" | awk '$1 == "node-ip:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"
  current_external_ip="$(printf '%s\n' "${primary_config}" | awk '$1 == "node-external-ip:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"
  current_flannel_iface="$(printf '%s\n' "${primary_config}" | awk '$1 == "flannel-iface:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"
  current_flannel_external_ip="$(printf '%s\n' "${primary_config}" | awk '$1 == "flannel-external-ip:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"

  if [[ "${current_node_ip}" == "${primary_private_ip}" ]] && \
     [[ "${current_external_ip}" == "${primary_mesh_ip}" ]] && \
     [[ "${current_flannel_iface}" == "tailscale0" ]] && \
     [[ "${current_flannel_external_ip}" == "true" ]]; then
    return 0
  fi

  filtered_config="$(printf '%s\n' "${primary_config}" | awk '
    $1 == "node-ip:" {next}
    $1 == "node-external-ip:" {next}
    $1 == "flannel-external-ip:" {next}
    $1 == "flannel-iface:" {next}
    {print}
  ')"

  patched_config="$(printf '%s\n' "${filtered_config}" | awk -v node_ip="${primary_private_ip}" -v mesh_ip="${primary_mesh_ip}" '
    function print_mesh_block() {
      printf "node-ip: \"%s\"\n", node_ip
      printf "node-external-ip: \"%s\"\n", mesh_ip
      print "flannel-external-ip: true"
      print "flannel-iface: \"tailscale0\""
    }
    {
      print
      if (!inserted && $1 == "write-kubeconfig-mode:") {
        print_mesh_block()
        inserted = 1
      }
    }
    END {
      if (!inserted) {
        print_mesh_block()
      }
    }
  ')"

  log "restoring primary k3s server ${primary_node_name} to mesh networking (${primary_mesh_ip})"
  restore_cmd="$(cat <<EOF
set -euo pipefail

config=/etc/rancher/k3s/config.yaml
backup="\${config}.mesh-restore-\$(date +%Y%m%d%H%M%S)"
cp "\${config}" "\${backup}"
cat >"\${config}" <<'CFG'
${patched_config}
CFG
if command -v timeout >/dev/null 2>&1; then
  timeout --kill-after=15s 120s systemctl restart k3s
else
  systemctl restart k3s
fi
systemctl is-active --quiet k3s
printf '%s\n' "\${backup}"
EOF
)"
  backup_path="$(run_primary_host_root_command "${primary_node_name}" "${restore_cmd}" | tr -d '\r' | tail -n 1)"
  if [[ -n "${backup_path}" ]]; then
    log "backed up primary k3s config to ${backup_path} before restoring mesh networking"
  fi

  if ! wait_for_local_kube_api_ready; then
    fail "local kube-apiserver did not recover after restoring mesh networking on ${primary_node_name}"
  fi

  if ! wait_for_primary_node_ready "${primary_node_name}"; then
    fail "primary node ${primary_node_name} did not become Ready after restoring mesh networking"
  fi
}

ready_nodes_matching_selector() {
  local selector="$1"
  ${KUBECTL} get nodes -l "${selector}" --no-headers 2>/dev/null | \
    awk '$2 == "Ready" || $2 ~ /^Ready,/ {count++} END {print count + 0}'
}

ensure_coredns_multinode_scheduling() {
  local desired_replicas="${FUGUE_COREDNS_TARGET_REPLICAS}"
  local coredns_selector_key="fugue.install/profile"
  local coredns_selector_value="combined"
  local ready_coredns_nodes="0"
  local current_replicas=""
  local current_profile_selector=""
  local current_control_plane_selector=""
  local current_os_selector=""
  local patch_payload=""

  if ! [[ "${desired_replicas}" =~ ^[0-9]+$ ]] || (( desired_replicas <= 0 )); then
    fail "FUGUE_COREDNS_TARGET_REPLICAS must be a positive integer"
  fi

  if ! ${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" >/dev/null 2>&1; then
    log "skip CoreDNS HA normalization because deploy/${FUGUE_COREDNS_DEPLOYMENT_NAME} is absent from ${FUGUE_COREDNS_NAMESPACE}"
    return 0
  fi

  # Keep CoreDNS off worker nodes so pod DNS does not vary with worker host resolvers.
  ready_coredns_nodes="$(ready_nodes_matching_selector "kubernetes.io/os=linux,${coredns_selector_key}=${coredns_selector_value}")"
  if ! [[ "${ready_coredns_nodes}" =~ ^[0-9]+$ ]] || (( ready_coredns_nodes == 0 )); then
    coredns_selector_key="node-role.kubernetes.io/control-plane"
    coredns_selector_value="true"
    ready_coredns_nodes="$(ready_nodes_matching_selector "kubernetes.io/os=linux,${coredns_selector_key}=${coredns_selector_value}")"
  fi
  if ! [[ "${ready_coredns_nodes}" =~ ^[0-9]+$ ]] || (( ready_coredns_nodes == 0 )); then
    fail "no Ready linux control-plane nodes available for CoreDNS scheduling"
  fi
  if (( desired_replicas > ready_coredns_nodes )); then
    desired_replicas="${ready_coredns_nodes}"
  fi
  if (( desired_replicas < 1 )); then
    desired_replicas=1
  fi

  current_replicas="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  current_profile_selector="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.template.spec.nodeSelector.fugue\.install/profile}' 2>/dev/null || true)"
  current_control_plane_selector="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.template.spec.nodeSelector.node-role\.kubernetes\.io/control-plane}' 2>/dev/null || true)"
  current_os_selector="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.template.spec.nodeSelector.kubernetes\.io/os}' 2>/dev/null || true)"

  if [[ "${current_replicas}" == "${desired_replicas}" ]] && [[ "${current_os_selector}" == "linux" ]]; then
    if [[ "${coredns_selector_key}" == "fugue.install/profile" ]] && [[ "${current_profile_selector}" == "${coredns_selector_value}" ]]; then
      return 0
    fi
    if [[ "${coredns_selector_key}" == "node-role.kubernetes.io/control-plane" ]] && [[ "${current_control_plane_selector}" == "${coredns_selector_value}" ]]; then
      return 0
    fi
  fi

  log "ensuring CoreDNS stays on control-plane nodes (replicas=${desired_replicas})"
  patch_payload="$(cat <<EOF
[
  {"op":"add","path":"/spec/replicas","value":${desired_replicas}},
  {"op":"add","path":"/spec/template/spec/nodeSelector","value":{"kubernetes.io/os":"linux","${coredns_selector_key}":"${coredns_selector_value}"}}
]
EOF
)"
  ${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" patch deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" --type=json -p "${patch_payload}" >/dev/null
  ${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" rollout status "deploy/${FUGUE_COREDNS_DEPLOYMENT_NAME}" --timeout=180s
}

relieve_primary_disk_pressure() {
  local primary_node_name=""
  local cleanup_cmd=""

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary disk-pressure recovery because the primary node could not be identified"
    return 0
  fi
  if ! primary_node_has_disk_pressure "${primary_node_name}"; then
    return 0
  fi

  log "primary node ${primary_node_name} is under DiskPressure; running host-level registry cleanup before upgrade"
  cleanup_cmd="$(cat <<'EOF'
set -euo pipefail

log() {
  printf '[fugue-upgrade][primary-cleanup] %s\n' "$*"
}

run_bounded_host_command() {
  local timeout_seconds="$1"
  local pid=""
  local state=""
  local deadline=""
  shift

  "$@" &
  pid="$!"
  deadline=$((SECONDS + timeout_seconds))

  while true; do
    state="$(awk '{print $3}' "/proc/${pid}/stat" 2>/dev/null || true)"
    if [[ -z "${state}" || "${state}" == "Z" ]]; then
      wait "${pid}"
      return $?
    fi
    if (( SECONDS >= deadline )); then
      log "timed out after ${timeout_seconds}s: $*"
      kill "${pid}" >/dev/null 2>&1 || true
      sleep 2
      kill -KILL "${pid}" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 1
  done
}

registry_root="/var/lib/fugue/registry"
runner_update_root="/home/github-runner/actions-runner-work/_update"
registry_image="docker.io/library/registry:2.8.3"
stale_upload_minutes="${FUGUE_REGISTRY_UPLOAD_STALE_MINUTES:-1440}"
gc_id="fugue-registry-gc-$(date +%s)"

cleanup() {
  k3s ctr tasks kill "${gc_id}" >/dev/null 2>&1 || true
  k3s ctr containers rm "${gc_id}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

purge_stale_registry_uploads() {
  local repositories_root="${registry_root}/docker/registry/v2/repositories"
  local uploads_root=""
  local path=""

  [[ -d "${repositories_root}" ]] || return 0

  while IFS= read -r uploads_root; do
    while IFS= read -r path; do
      [[ -n "${path}" ]] || continue
      rm -rf -- "${path}"
      log "removed stale registry upload ${path}"
    done < <(find "${uploads_root}" -mindepth 1 -maxdepth 1 -type d -mmin "+${stale_upload_minutes}" -print)
  done < <(find "${repositories_root}" -type d -name '_uploads' -print)
}

log "filesystem usage before cleanup"
df -h /
du -sh "${registry_root}" 2>/dev/null || true

if command -v k3s >/dev/null 2>&1; then
  if run_bounded_host_command 90 k3s crictl rmi --prune >/tmp/fugue-primary-image-prune.log 2>&1; then
    log "unused k3s images pruned"
  else
    status=$?
    log "image prune returned ${status}; continuing"
  fi
fi

if [[ -d "${runner_update_root}" ]] && find "${runner_update_root}" -mindepth 0 -mmin "+1440" | grep -q .; then
  rm -rf -- "${runner_update_root}"
  mkdir -p "${runner_update_root}"
  chown -R github-runner:github-runner "${runner_update_root}" >/dev/null 2>&1 || true
  log "removed stale runner update cache ${runner_update_root}"
fi

if [[ ! -d "${registry_root}/docker/registry/v2" ]]; then
  log "registry data root ${registry_root} is absent; skipping offline registry GC"
  exit 0
fi

purge_stale_registry_uploads

if ! k3s ctr images ls | awk 'NR > 1 {print $1}' | grep -Fxq "${registry_image}"; then
  log "pulling ${registry_image} for offline registry GC"
  k3s ctr images pull "${registry_image}"
fi

log "running offline registry garbage-collect against ${registry_root}"
timeout 600s k3s ctr run --rm \
  --mount type=bind,src="${registry_root}",dst=/var/lib/registry,options=rbind:rw \
  "${registry_image}" "${gc_id}" \
  registry garbage-collect --delete-untagged /etc/docker/registry/config.yml

log "filesystem usage after cleanup"
du -sh "${registry_root}" 2>/dev/null || true
df -h /
EOF
)"
  run_primary_host_root_command "${primary_node_name}" "${cleanup_cmd}"

  if ! wait_for_primary_disk_pressure_clear "${primary_node_name}"; then
    fail "primary node ${primary_node_name} still reports DiskPressure after host-level registry cleanup"
  fi
}

sync_route_a_edge_proxy() {
  if [[ "${FUGUE_SYNC_EDGE_PROXY:-true}" != "true" ]]; then
    log "skip Route A edge proxy sync because FUGUE_SYNC_EDGE_PROXY=${FUGUE_SYNC_EDGE_PROXY}"
    return
  fi
  if [[ -z "${FUGUE_API_PUBLIC_DOMAIN:-}" ]]; then
    return
  fi

  prepare_control_plane_automation_ssh
  export FUGUE_DOMAIN="${FUGUE_API_PUBLIC_DOMAIN}"
  log "syncing Route A edge proxy through scripts/sync_fugue_edge_proxy.sh"
  bash ./scripts/sync_fugue_edge_proxy.sh
}

label_default_builder_nodes() {
  log "keeping primary control-plane node out of the shared runtime and builder pools"
  ${KUBECTL} label node -l fugue.install/role=primary \
    fugue.io/shared-pool- \
    fugue.io/build- \
    fugue.io/build-tier- \
    --overwrite >/dev/null || true

  log "labeling non-primary combined nodes as builder candidates"
  ${KUBECTL} label node -l 'fugue.install/profile=combined,fugue.install/role!=primary' \
    fugue.io/build=true \
    fugue.io/build-tier- \
    --overwrite >/dev/null
}

rollback_release() {
  local rollback_api_deployment="${FUGUE_API_DEPLOYMENT_NAME}"

  if [[ -z "${PREVIOUS_REVISION:-}" ]]; then
    log "skip rollback because no previous revision was captured"
    return 1
  fi

  log "rolling back release ${FUGUE_RELEASE_NAME} to revision ${PREVIOUS_REVISION}"
  helm rollback "${FUGUE_RELEASE_NAME}" "${PREVIOUS_REVISION}" \
    -n "${FUGUE_NAMESPACE}" \
    --timeout "${FUGUE_HELM_TIMEOUT}"

  if ! deployment_exists "${rollback_api_deployment}" && deployment_exists "${FUGUE_LEGACY_API_DEPLOYMENT_NAME}"; then
    rollback_api_deployment="${FUGUE_LEGACY_API_DEPLOYMENT_NAME}"
  fi

  rollout_status "${rollback_api_deployment}"
  if deployment_exists "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}"; then
    rollout_status "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}"
  else
    log "rollback target does not include ${FUGUE_CONTROLLER_DEPLOYMENT_NAME}; skipping controller rollout check"
  fi
  retry "${FUGUE_SMOKE_RETRIES}" "${FUGUE_SMOKE_DELAY_SECONDS}" smoke_test
}

main() {
  require_env FUGUE_API_IMAGE_REPOSITORY
  require_env FUGUE_API_IMAGE_TAG
  require_env FUGUE_CONTROLLER_IMAGE_REPOSITORY
  require_env FUGUE_CONTROLLER_IMAGE_TAG
  FUGUE_EDGE_ENABLED="${FUGUE_EDGE_ENABLED:-true}"
  if [[ "${FUGUE_EDGE_ENABLED}" == "true" ]]; then
    require_env FUGUE_EDGE_IMAGE_REPOSITORY
    require_env FUGUE_EDGE_IMAGE_TAG
  else
    FUGUE_EDGE_IMAGE_REPOSITORY="${FUGUE_EDGE_IMAGE_REPOSITORY:-fugue-edge}"
    FUGUE_EDGE_IMAGE_TAG="${FUGUE_EDGE_IMAGE_TAG:-latest}"
  fi

  export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
  KUBECTL="$(detect_kubectl)"
  export KUBECTL
  trap cleanup_tmp_artifacts EXIT

  FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
  FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
  FUGUE_HELM_CHART_PATH="${FUGUE_HELM_CHART_PATH:-deploy/helm/fugue}"
  FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-${FUGUE_RELEASE_NAME}-fugue}"
  FUGUE_API_DEPLOYMENT_NAME="${FUGUE_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-api}"
  FUGUE_LEGACY_API_DEPLOYMENT_NAME="${FUGUE_LEGACY_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}}"
  FUGUE_CONTROLLER_DEPLOYMENT_NAME="${FUGUE_CONTROLLER_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-controller}"
  FUGUE_POSTGRES_DEPLOYMENT_NAME="${FUGUE_POSTGRES_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-postgres}"
  FUGUE_HELM_TIMEOUT="${FUGUE_HELM_TIMEOUT:-10m0s}"
  FUGUE_ROLLOUT_TIMEOUT="${FUGUE_ROLLOUT_TIMEOUT:-600s}"
  FUGUE_SMOKE_RETRIES="${FUGUE_SMOKE_RETRIES:-12}"
  FUGUE_SMOKE_DELAY_SECONDS="${FUGUE_SMOKE_DELAY_SECONDS:-5}"
  FUGUE_API_REPLICA_COUNT="${FUGUE_API_REPLICA_COUNT:-3}"
  FUGUE_CONTROLLER_REPLICA_COUNT="${FUGUE_CONTROLLER_REPLICA_COUNT:-2}"
  FUGUE_API_DATABASE_URL="${FUGUE_API_DATABASE_URL:-}"
  FUGUE_POSTGRES_ENABLED="${FUGUE_POSTGRES_ENABLED:-true}"
  FUGUE_CONTROL_PLANE_POSTGRES_ENABLED="${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED:-false}"
  FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API="${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API:-false}"
  FUGUE_CONTROL_PLANE_POSTGRES_NAME="${FUGUE_CONTROL_PLANE_POSTGRES_NAME:-}"
  FUGUE_CONTROL_PLANE_POSTGRES_IMAGE_NAME="${FUGUE_CONTROL_PLANE_POSTGRES_IMAGE_NAME:-ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie}"
  FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES="${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES:-3}"
  FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_SIZE="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_SIZE:-10Gi}"
  FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_CLASS="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_CLASS:-}"
  FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME="${FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME:-}"
  FUGUE_REGISTRY_NODEPORT="${FUGUE_REGISTRY_NODEPORT:-30500}"
  FUGUE_REGISTRY_SERVICE_PORT="${FUGUE_REGISTRY_SERVICE_PORT:-5000}"
  FUGUE_API_PUBLIC_DOMAIN="${FUGUE_API_PUBLIC_DOMAIN:-}"
  FUGUE_APP_BASE_DOMAIN="${FUGUE_APP_BASE_DOMAIN:-fugue.pro}"
  FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME="${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME:-${FUGUE_RELEASE_FULLNAME}-control-plane-automation}"
  FUGUE_COREDNS_NAMESPACE="${FUGUE_COREDNS_NAMESPACE:-kube-system}"
  FUGUE_COREDNS_DEPLOYMENT_NAME="${FUGUE_COREDNS_DEPLOYMENT_NAME:-coredns}"
  FUGUE_COREDNS_TARGET_REPLICAS="${FUGUE_COREDNS_TARGET_REPLICAS:-2}"
  FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED="${FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED:-true}"
  FUGUE_SHARED_WORKSPACE_STORAGE_CLASS="${FUGUE_SHARED_WORKSPACE_STORAGE_CLASS:-fugue-rwx}"
  FUGUE_SHARED_WORKSPACE_NFS_CLUSTER_IP="${FUGUE_SHARED_WORKSPACE_NFS_CLUSTER_IP:-10.43.240.17}"
  FUGUE_EDGE_GROUP_ID="${FUGUE_EDGE_GROUP_ID:-}"
  FUGUE_EDGE_CADDY_ENABLED="${FUGUE_EDGE_CADDY_ENABLED:-true}"
  FUGUE_EDGE_CADDY_LISTEN_ADDR="${FUGUE_EDGE_CADDY_LISTEN_ADDR:-:18443}"
  FUGUE_EDGE_CADDY_TLS_MODE="${FUGUE_EDGE_CADDY_TLS_MODE:-internal}"
  FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED:-false}"
  FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP:-80}"
  FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS:-443}"
  FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED="${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED:-false}"
  FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME="${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME:-}"
  FUGUE_EDGE_CADDY_STATIC_TLS_MOUNT_PATH="${FUGUE_EDGE_CADDY_STATIC_TLS_MOUNT_PATH:-/etc/caddy/static-tls}"
  FUGUE_EDGE_CADDY_STATIC_TLS_CERTIFICATE_KEY="${FUGUE_EDGE_CADDY_STATIC_TLS_CERTIFICATE_KEY:-tls.crt}"
  FUGUE_EDGE_CADDY_STATIC_TLS_PRIVATE_KEY_KEY="${FUGUE_EDGE_CADDY_STATIC_TLS_PRIVATE_KEY_KEY:-tls.key}"
  FUGUE_EDGE_REGION="${FUGUE_EDGE_REGION:-}"
  FUGUE_EDGE_COUNTRY="${FUGUE_EDGE_COUNTRY:-}"
  FUGUE_EDGE_PUBLIC_HOSTNAME="${FUGUE_EDGE_PUBLIC_HOSTNAME:-}"
  FUGUE_EDGE_PUBLIC_IPV4="${FUGUE_EDGE_PUBLIC_IPV4:-}"
  FUGUE_EDGE_PUBLIC_IPV6="${FUGUE_EDGE_PUBLIC_IPV6:-}"
  FUGUE_EDGE_MESH_IP="${FUGUE_EDGE_MESH_IP:-}"
  FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE="${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE:-}"
  FUGUE_EDGE_EXTRA_GROUPS="${FUGUE_EDGE_EXTRA_GROUPS:-}"
  FUGUE_DNS_ENABLED="${FUGUE_DNS_ENABLED:-false}"
  FUGUE_DNS_ANSWER_IPS="${FUGUE_DNS_ANSWER_IPS:-}"
  FUGUE_DNS_ROUTE_A_ANSWER_IPS="${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-}"
  FUGUE_DNS_STATIC_RECORDS_JSON="${FUGUE_DNS_STATIC_RECORDS_JSON:-}"
  FUGUE_PLATFORM_ROUTES_JSON="${FUGUE_PLATFORM_ROUTES_JSON:-}"
  FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE="${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE:-}"
  FUGUE_DNS_EXTRA_GROUPS="${FUGUE_DNS_EXTRA_GROUPS:-}"
  FUGUE_DNS_NAMESERVERS="${FUGUE_DNS_NAMESERVERS:-}"
  FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED="${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED:-false}"
  if [[ "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" == "true" ]]; then
    FUGUE_DNS_UDP_ADDR="${FUGUE_DNS_UDP_ADDR:-:53}"
    FUGUE_DNS_TCP_ADDR="${FUGUE_DNS_TCP_ADDR:-:53}"
  else
    FUGUE_DNS_UDP_ADDR="${FUGUE_DNS_UDP_ADDR:-127.0.0.1:5353}"
    FUGUE_DNS_TCP_ADDR="${FUGUE_DNS_TCP_ADDR:-127.0.0.1:5353}"
  fi
  FUGUE_DNS_ZONE="${FUGUE_DNS_ZONE:-dns.${FUGUE_APP_BASE_DOMAIN}}"
  FUGUE_DNS_TTL="${FUGUE_DNS_TTL:-60}"

  case "${FUGUE_EDGE_CADDY_TLS_MODE}" in
    off|internal|public-on-demand) ;;
    *) fail "FUGUE_EDGE_CADDY_TLS_MODE must be off, internal, or public-on-demand" ;;
  esac
  if [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_ENABLED}" != "true" ]]; then
    fail "FUGUE_EDGE_CADDY_ENABLED must be true when FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_LISTEN_ADDR}" != ":443" ]]; then
    fail "FUGUE_EDGE_CADDY_LISTEN_ADDR must be :443 when FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_TLS_MODE}" != "public-on-demand" ]]; then
    fail "FUGUE_EDGE_CADDY_TLS_MODE must be public-on-demand when FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED=true"
  fi
  case "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED must be true or false" ;;
  esac
  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_ENABLED}" != "true" ]]; then
    fail "FUGUE_EDGE_CADDY_ENABLED must be true when FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_TLS_MODE}" == "off" ]]; then
    fail "FUGUE_EDGE_CADDY_TLS_MODE must not be off when FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" && -z "$(trim_field "${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME}")" ]]; then
    fail "FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME is required when FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true"
  fi
  if ! [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP}" =~ ^[0-9]+$ ]] || (( FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP <= 0 || FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP > 65535 )); then
    fail "FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP must be an integer between 1 and 65535"
  fi
  if ! [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS}" =~ ^[0-9]+$ ]] || (( FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS <= 0 || FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS > 65535 )); then
    fail "FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS must be an integer between 1 and 65535"
  fi
  case "${FUGUE_POSTGRES_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_POSTGRES_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_CONTROL_PLANE_POSTGRES_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" in
    true|false) ;;
    *) fail "FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API must be true or false" ;;
  esac
  if [[ "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" == "true" && "${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED}" != "true" ]]; then
    fail "FUGUE_CONTROL_PLANE_POSTGRES_ENABLED must be true when FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API=true"
  fi
  if [[ "${FUGUE_POSTGRES_ENABLED}" != "true" && "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" != "true" && -z "$(trim_field "${FUGUE_API_DATABASE_URL}")" ]]; then
    fail "FUGUE_API_DATABASE_URL or FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API=true is required when FUGUE_POSTGRES_ENABLED=false"
  fi
  if ! [[ "${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES}" =~ ^[0-9]+$ ]] || (( FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES < 2 )); then
    fail "FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES must be an integer >= 2"
  fi
  if [[ -n "$(trim_field "${FUGUE_EDGE_EXTRA_GROUPS}")" && -z "$(trim_field "${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE}")" ]]; then
    fail "FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE must be set when FUGUE_EDGE_EXTRA_GROUPS is set"
  fi

  if [[ "${FUGUE_DNS_ENABLED}" == "true" ]]; then
    require_env FUGUE_DNS_ANSWER_IPS
    if [[ "$(dns_answer_ip_count "${FUGUE_DNS_ANSWER_IPS}")" == "0" ]]; then
      fail "FUGUE_DNS_ANSWER_IPS must contain at least one non-empty IP when FUGUE_DNS_ENABLED=true"
    fi
    if [[ -n "$(trim_field "${FUGUE_DNS_ROUTE_A_ANSWER_IPS}")" && "$(dns_answer_ip_count "${FUGUE_DNS_ROUTE_A_ANSWER_IPS}")" == "0" ]]; then
      fail "FUGUE_DNS_ROUTE_A_ANSWER_IPS must contain only non-empty IP entries"
    fi
    if ! [[ "${FUGUE_DNS_TTL}" =~ ^[0-9]+$ ]] || (( FUGUE_DNS_TTL <= 0 || FUGUE_DNS_TTL > 3600 )); then
      fail "FUGUE_DNS_TTL must be an integer between 1 and 3600"
    fi
  fi
  edge_extra_groups_yaml >/dev/null
  if [[ -n "$(trim_field "${FUGUE_DNS_EXTRA_GROUPS}")" && "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    fail "FUGUE_DNS_ENABLED must be true when FUGUE_DNS_EXTRA_GROUPS is set"
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_EXTRA_GROUPS}")" && -z "$(trim_field "${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE}")" ]]; then
    fail "FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE must be set when FUGUE_DNS_EXTRA_GROUPS is set"
  fi
  dns_extra_groups_yaml >/dev/null

  if [[ -z "${FUGUE_REGISTRY_PUSH_BASE:-}" ]]; then
    FUGUE_REGISTRY_PUSH_BASE="${FUGUE_RELEASE_FULLNAME}-registry.${FUGUE_NAMESPACE}.svc.cluster.local:${FUGUE_REGISTRY_SERVICE_PORT}"
  fi
  if [[ -z "${FUGUE_REGISTRY_PULL_BASE:-}" ]]; then
    FUGUE_REGISTRY_PULL_BASE="$(detect_existing_registry_pull_base || true)"
  fi
  if [[ -z "${FUGUE_REGISTRY_PULL_BASE:-}" ]]; then
    FUGUE_REGISTRY_PULL_BASE="${FUGUE_DEFAULT_REGISTRY_PULL_BASE}"
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}" ]]; then
    FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT="${FUGUE_DEFAULT_CLUSTER_JOIN_REGISTRY_ENDPOINT:-127.0.0.1:${FUGUE_REGISTRY_NODEPORT}}"
  fi

  if [[ -z "${FUGUE_SMOKE_URL:-}" && -n "${FUGUE_API_PUBLIC_DOMAIN:-}" ]]; then
    FUGUE_SMOKE_URL="https://${FUGUE_API_PUBLIC_DOMAIN}/healthz"
  fi
  require_env FUGUE_SMOKE_URL

  command_exists helm || fail "helm is not installed"
  ensure_local_registry_mirror_config "${FUGUE_REGISTRY_PULL_BASE}" "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}"
  wait_for_local_kube_api_ready
  ${KUBECTL} version --client >/dev/null
  helm status "${FUGUE_RELEASE_NAME}" -n "${FUGUE_NAMESPACE}" >/dev/null

  PREVIOUS_REVISION="$(helm_current_revision)"
  [[ -n "${PREVIOUS_REVISION}" ]] || fail "failed to detect current Helm revision"

  log "upgrading ${FUGUE_RELEASE_NAME} in namespace ${FUGUE_NAMESPACE}"
  log "api image: ${FUGUE_API_IMAGE_REPOSITORY}:${FUGUE_API_IMAGE_TAG}"
  log "controller image: ${FUGUE_CONTROLLER_IMAGE_REPOSITORY}:${FUGUE_CONTROLLER_IMAGE_TAG}"
  log "edge image: ${FUGUE_EDGE_IMAGE_REPOSITORY}:${FUGUE_EDGE_IMAGE_TAG} enabled=${FUGUE_EDGE_ENABLED} edge_group_id=${FUGUE_EDGE_GROUP_ID:-<empty>}"
  log "edge caddy: enabled=${FUGUE_EDGE_CADDY_ENABLED} listen=${FUGUE_EDGE_CADDY_LISTEN_ADDR} tls_mode=${FUGUE_EDGE_CADDY_TLS_MODE} public_hostports=${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED} http=${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP} https=${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS} static_tls=${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED} static_tls_secret=${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME:-<none>}"
  log "control-plane postgres: legacy_enabled=${FUGUE_POSTGRES_ENABLED} cnpg_enabled=${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED} cnpg_use_for_api=${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API} cnpg_instances=${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES}"
  log "edge scheduling: primary_country=${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE:-<none>} public_ipv4=${FUGUE_EDGE_PUBLIC_IPV4:-<none>} extra_groups=${FUGUE_EDGE_EXTRA_GROUPS:-<none>}"
  log "previous Helm revision: ${PREVIOUS_REVISION}"
  log "registry push base: ${FUGUE_REGISTRY_PUSH_BASE}"
  log "registry pull base: ${FUGUE_REGISTRY_PULL_BASE}"
  log "cluster join registry endpoint: ${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}"
  log "app base domain: ${FUGUE_APP_BASE_DOMAIN}"
  log "custom domain base domain: dns.${FUGUE_APP_BASE_DOMAIN}"
  log "dns shadow: enabled=${FUGUE_DNS_ENABLED} zone=${FUGUE_DNS_ZONE} answer_ips=${FUGUE_DNS_ANSWER_IPS:-<none>} route_a_answer_ips=${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-<none>} nameservers=${FUGUE_DNS_NAMESERVERS:-<none>} static_records=$([[ -n "$(trim_field "${FUGUE_DNS_STATIC_RECORDS_JSON}")" ]] && printf enabled || printf disabled) platform_routes=$([[ -n "$(trim_field "${FUGUE_PLATFORM_ROUTES_JSON}")" ]] && printf enabled || printf disabled) public_hostports=${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED} udp=${FUGUE_DNS_UDP_ADDR} tcp=${FUGUE_DNS_TCP_ADDR}"
  log "dns scheduling: primary_country=${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE:-<none>} extra_groups=${FUGUE_DNS_EXTRA_GROUPS:-<none>}"
  log "shared workspace storage: enabled=${FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED} class=${FUGUE_SHARED_WORKSPACE_STORAGE_CLASS}"

  recover_primary_node_if_needed
  relieve_primary_disk_pressure
  recover_primary_postgres_if_needed
  restore_primary_mesh_network_if_needed
  ensure_coredns_multinode_scheduling

  apply_chart_crds

  upgrade_override_values_file="$(write_upgrade_override_values)"
  build_dns_helm_set_args
  log "injecting disk-pressure toleration for primary-pinned hostPath control-plane pods"

  # Do not use Helm's release-wide --wait here. It waits on every resource in
  # the chart, including DaemonSets scheduled onto stale/NotReady nodes. That
  # can deadlock control-plane upgrades exactly when the new API needs to clean
  # up those stale nodes. We gate success on targeted API/controller rollout
  # checks plus the smoke test below instead.
  if ! helm upgrade "${FUGUE_RELEASE_NAME}" "${FUGUE_HELM_CHART_PATH}" \
    -n "${FUGUE_NAMESPACE}" \
    --reset-then-reuse-values \
    --history-max 20 \
    --timeout "${FUGUE_HELM_TIMEOUT}" \
    -f "${upgrade_override_values_file}" \
    "${DNS_HELM_SET_ARGS[@]}" \
    --set-string api.image.repository="${FUGUE_API_IMAGE_REPOSITORY}" \
    --set-string api.image.tag="${FUGUE_API_IMAGE_TAG}" \
    --set-string controller.image.repository="${FUGUE_CONTROLLER_IMAGE_REPOSITORY}" \
    --set-string controller.image.tag="${FUGUE_CONTROLLER_IMAGE_TAG}" \
    --set edge.enabled="${FUGUE_EDGE_ENABLED}" \
    --set-string edge.image.repository="${FUGUE_EDGE_IMAGE_REPOSITORY}" \
    --set-string edge.image.tag="${FUGUE_EDGE_IMAGE_TAG}" \
    --set-string edge.edgeGroupID="${FUGUE_EDGE_GROUP_ID}" \
    --set-string edge.region="${FUGUE_EDGE_REGION}" \
    --set-string edge.country="${FUGUE_EDGE_COUNTRY}" \
    --set-string edge.publicHostname="${FUGUE_EDGE_PUBLIC_HOSTNAME}" \
    --set-string edge.publicIPv4="${FUGUE_EDGE_PUBLIC_IPV4}" \
    --set-string edge.publicIPv6="${FUGUE_EDGE_PUBLIC_IPV6}" \
    --set-string edge.meshIP="${FUGUE_EDGE_MESH_IP}" \
    --set edge.caddy.enabled="${FUGUE_EDGE_CADDY_ENABLED}" \
    --set-string edge.caddy.listenAddr="${FUGUE_EDGE_CADDY_LISTEN_ADDR}" \
    --set-string edge.caddy.tlsMode="${FUGUE_EDGE_CADDY_TLS_MODE}" \
    --set edge.caddy.publicHostPorts.enabled="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" \
    --set edge.caddy.publicHostPorts.http="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP}" \
    --set edge.caddy.publicHostPorts.https="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS}" \
    --set edge.caddy.staticTLS.enabled="${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" \
    --set-string edge.caddy.staticTLS.secretName="${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME}" \
    --set-string edge.caddy.staticTLS.mountPath="${FUGUE_EDGE_CADDY_STATIC_TLS_MOUNT_PATH}" \
    --set-string edge.caddy.staticTLS.certificateKey="${FUGUE_EDGE_CADDY_STATIC_TLS_CERTIFICATE_KEY}" \
    --set-string edge.caddy.staticTLS.privateKeyKey="${FUGUE_EDGE_CADDY_STATIC_TLS_PRIVATE_KEY_KEY}" \
    --set-string api.appBaseDomain="${FUGUE_APP_BASE_DOMAIN}" \
    --set-string api.apiPublicDomain="${FUGUE_API_PUBLIC_DOMAIN}" \
    --set-string api.databaseURL="${FUGUE_API_DATABASE_URL}" \
    --set-string api.registryPushBase="${FUGUE_REGISTRY_PUSH_BASE}" \
    --set-string api.registryPullBase="${FUGUE_REGISTRY_PULL_BASE}" \
    --set-string api.clusterJoinRegistryEndpoint="${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}" \
    --set api.replicaCount="${FUGUE_API_REPLICA_COUNT}" \
    --set api.hostNetwork=false \
    --set api.minReadySeconds=5 \
    --set api.terminationGracePeriodSeconds=40 \
    --set api.podDisruptionBudget.enabled=true \
    --set api.podDisruptionBudget.minAvailable=2 \
    --set-string api.shutdownDrainDelay=5s \
    --set-string api.shutdownTimeout=25s \
    --set controller.replicaCount="${FUGUE_CONTROLLER_REPLICA_COUNT}" \
    --set-string controller.fallbackPollInterval=30s \
    --set controller.terminationGracePeriodSeconds=30 \
    --set controller.podDisruptionBudget.enabled=true \
    --set controller.podDisruptionBudget.minAvailable=1 \
    --set controller.leaderElection.enabled=true \
    --set-string controller.leaderElection.leaseName="${FUGUE_CONTROLLER_DEPLOYMENT_NAME}" \
    --set-string controller.leaderElection.leaseNamespace="${FUGUE_NAMESPACE}" \
    --set-string controller.leaderElection.leaseDuration=15s \
    --set-string controller.leaderElection.renewDeadline=10s \
    --set-string controller.leaderElection.retryPeriod=2s \
    --set-string controller.migrationGuard.legacyControllerContainerName=controller \
    --set-string controller.migrationGuard.checkInterval=2s \
    --set postgres.enabled="${FUGUE_POSTGRES_ENABLED}" \
    --set controlPlanePostgres.enabled="${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED}" \
    --set controlPlanePostgres.useForAPI="${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" \
    --set-string controlPlanePostgres.name="${FUGUE_CONTROL_PLANE_POSTGRES_NAME}" \
    --set-string controlPlanePostgres.imageName="${FUGUE_CONTROL_PLANE_POSTGRES_IMAGE_NAME}" \
    --set controlPlanePostgres.instances="${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES}" \
    --set-string controlPlanePostgres.storage.size="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_SIZE}" \
    --set-string controlPlanePostgres.storage.storageClassName="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_CLASS}" \
    --set-string controlPlanePostgres.existingSecretName="${FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME}" \
    --set sharedWorkspaceStorage.enabled="${FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED}" \
    --set-string sharedWorkspaceStorage.storageClassName="${FUGUE_SHARED_WORKSPACE_STORAGE_CLASS}" \
    --set-string sharedWorkspaceStorage.server.clusterIP="${FUGUE_SHARED_WORKSPACE_NFS_CLUSTER_IP}"; then
    log "helm upgrade failed; attempting rollback"
    rollback_release || true
    fail "helm upgrade failed"
  fi

  force_delete_release_pods_on_unhealthy_nodes

  if ! rollout_status "${FUGUE_API_DEPLOYMENT_NAME}"; then
    log "api rollout check failed; attempting rollback"
    rollback_release || true
    fail "api rollout failed"
  fi

  if ! rollout_status "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}"; then
    log "controller rollout check failed; attempting rollback"
    rollback_release || true
    fail "controller rollout failed"
  fi

  label_default_builder_nodes

  sync_route_a_edge_proxy

  if ! retry "${FUGUE_SMOKE_RETRIES}" "${FUGUE_SMOKE_DELAY_SECONDS}" smoke_test; then
    log "smoke test failed; attempting rollback"
    rollback_release || true
    fail "smoke test failed"
  fi

  local current_revision
  current_revision="$(helm_current_revision)"
  log "upgrade complete; current Helm revision=${current_revision}"
}

main "$@"
