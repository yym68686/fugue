#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  issue_fugue_app_wildcard_tls.sh --domain <app-base-domain>

Issues/renews a wildcard app certificate with acme.sh DNS-01 through the
Fugue authoritative DNS challenge API and writes it to a Kubernetes TLS Secret
for fugue-edge Caddy static loading.

Options:
  --dns-provider <provider>     DNS hook provider: fugue or cloudflare. Default: fugue.
  --api-url <url>               Fugue API URL for --dns-provider fugue. Defaults to FUGUE_API_URL.
  --api-key <token>             Fugue API key for --dns-provider fugue. Defaults to FUGUE_API_KEY/FUGUE_TOKEN/FUGUE_BOOTSTRAP_KEY.
  --cloudflare-env-file <path>  Legacy env file containing a Cloudflare token.
  --namespace <namespace>       Kubernetes namespace. Default: fugue-system.
  --secret-name <name>          Kubernetes TLS Secret name. Default: fugue-app-wildcard-tls.
  --domain <domain>             App base domain. Required unless FUGUE_APP_BASE_DOMAIN is set.
  --server <server>             ACME server. Default: letsencrypt.
  --acme-home <dir>             Optional acme.sh home directory.
  --acme <path>                 acme.sh executable. Default: acme.sh.
  --kubectl <path>              kubectl executable. Default: kubectl.
  --renew-before-days <days>    Skip ACME if the current Secret cert is valid for at least this many days. Default: 0.
  --check-only                  Only verify the current Secret certificate; never invoke ACME or update the Secret.
  --dry-run                     Print the planned commands without issuing or writing.
  -h, --help                    Show this help.
EOF
}

fail() {
  printf 'issue_fugue_app_wildcard_tls.sh: %s\n' "$*" >&2
  exit 1
}

require_value() {
  local flag="$1"
  local value="${2:-}"
  [[ -n "${value}" ]] || fail "${flag} requires a non-empty value"
}

strip_env_quotes() {
  local value="$1"
  value="${value%$'\r'}"
  if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s' "${value}"
}

read_env_value() {
  local path="$1"
  local key="$2"
  awk -v key="${key}" '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    {
      line=$0
      sub(/^[[:space:]]*export[[:space:]]+/, "", line)
      if (index(line, key "=") == 1) {
        sub(/^[^=]*=/, "", line)
        print line
        exit
      }
    }
  ' "${path}"
}

current_secret_cert_is_fresh() {
  local days="$1"
  local seconds encoded cert_path check_output
  (( days > 0 )) || return 1
  seconds=$((days * 86400))
  encoded="$("${kubectl_cmd}" -n "${namespace}" get secret "${secret_name}" -o 'jsonpath={.data.tls\.crt}' 2>/dev/null || true)"
  encoded="$(printf '%s' "${encoded}" | tr -d '[:space:]')"
  [[ -n "${encoded}" ]] || return 1
  cert_path="${tmpdir}/current-secret-tls.crt"
  if ! printf '%s' "${encoded}" | openssl base64 -d -A >"${cert_path}" 2>/dev/null; then
    return 1
  fi
  if ! check_output="$(openssl x509 -in "${cert_path}" -noout -checkend "${seconds}" 2>&1)"; then
    return 1
  fi
  [[ "${check_output}" == *"Certificate will not expire"* ]]
}

namespace="${FUGUE_NAMESPACE:-fugue-system}"
secret_name="${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME:-fugue-app-wildcard-tls}"
domain="${FUGUE_APP_BASE_DOMAIN:-}"
server="${ACME_SERVER:-letsencrypt}"
dns_provider="${FUGUE_ACME_DNS_PROVIDER:-fugue}"
api_url="${FUGUE_API_URL:-}"
api_key="${FUGUE_API_KEY:-${FUGUE_TOKEN:-${FUGUE_BOOTSTRAP_KEY:-}}}"
cloudflare_env_file="${CLOUDFLARE_ENV_FILE:-}"
acme_home="${ACME_HOME:-}"
acme_cmd="${ACME_SH:-acme.sh}"
kubectl_cmd="${KUBECTL:-kubectl}"
challenge_ttl="${FUGUE_ACME_CHALLENGE_TTL:-60}"
challenge_expires_in_seconds="${FUGUE_ACME_CHALLENGE_EXPIRES_IN_SECONDS:-3600}"
renew_before_days="${FUGUE_APP_WILDCARD_TLS_RENEW_BEFORE_DAYS:-0}"
check_only="false"
dry_run="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dns-provider) require_value "$1" "${2:-}"; dns_provider="$2"; shift 2 ;;
    --api-url) require_value "$1" "${2:-}"; api_url="$2"; shift 2 ;;
    --api-key) require_value "$1" "${2:-}"; api_key="$2"; shift 2 ;;
    --cloudflare-env-file) require_value "$1" "${2:-}"; cloudflare_env_file="$2"; shift 2 ;;
    --namespace) require_value "$1" "${2:-}"; namespace="$2"; shift 2 ;;
    --secret-name) require_value "$1" "${2:-}"; secret_name="$2"; shift 2 ;;
    --domain) require_value "$1" "${2:-}"; domain="$2"; shift 2 ;;
    --server) require_value "$1" "${2:-}"; server="$2"; shift 2 ;;
    --acme-home) require_value "$1" "${2:-}"; acme_home="$2"; shift 2 ;;
    --acme) require_value "$1" "${2:-}"; acme_cmd="$2"; shift 2 ;;
    --kubectl) require_value "$1" "${2:-}"; kubectl_cmd="$2"; shift 2 ;;
    --renew-before-days) require_value "$1" "${2:-}"; renew_before_days="$2"; shift 2 ;;
    --check-only) check_only="true"; shift ;;
    --dry-run) dry_run="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "${namespace}" ]] || fail "--namespace is required"
[[ -n "${secret_name}" ]] || fail "--secret-name is required"
[[ -n "${domain}" ]] || fail "--domain is required"
[[ "${domain}" != "*."* ]] || fail "--domain must be the base domain, not a wildcard"
[[ "${renew_before_days}" =~ ^[0-9]+$ ]] || fail "--renew-before-days must be a non-negative integer"
if [[ "${check_only}" == "true" && "${dry_run}" == "true" ]]; then
  fail "--check-only and --dry-run cannot be used together"
fi
if [[ "${check_only}" == "true" && "${renew_before_days}" == "0" ]]; then
  fail "--check-only requires --renew-before-days greater than zero"
fi
case "${dns_provider}" in
  fugue|cloudflare) ;;
  *) fail "--dns-provider must be fugue or cloudflare" ;;
esac

cloudflare_token=""
if [[ "${check_only}" == "true" ]]; then
  :
elif [[ "${dns_provider}" == "cloudflare" ]]; then
  cloudflare_token="${CLOUDFLARE_DNS_API_TOKEN:-${CLOUDFLARE_API_TOKEN:-}}"
  if [[ -z "${cloudflare_token}" && -n "${cloudflare_env_file}" ]]; then
    [[ -f "${cloudflare_env_file}" ]] || fail "Cloudflare env file not found: ${cloudflare_env_file}"
    cloudflare_token="$(read_env_value "${cloudflare_env_file}" "CLOUDFLARE_DNS_API_TOKEN")"
    if [[ -z "${cloudflare_token}" ]]; then
      cloudflare_token="$(read_env_value "${cloudflare_env_file}" "CLOUDFLARE_API_TOKEN")"
    fi
    cloudflare_token="$(strip_env_quotes "${cloudflare_token}")"
  fi
  [[ -n "${cloudflare_token}" ]] || fail "Cloudflare token is required for --dns-provider cloudflare"
else
  [[ -n "${api_url}" ]] || fail "--api-url or FUGUE_API_URL is required for --dns-provider fugue"
  [[ -n "${api_key}" ]] || fail "--api-key or FUGUE_API_KEY/FUGUE_TOKEN/FUGUE_BOOTSTRAP_KEY is required for --dns-provider fugue"
fi

wildcard_domain="*.${domain}"
if [[ -z "${acme_home}" ]]; then
  acme_home="${HOME}/.acme.sh"
fi
acme_home_args=(--home "${acme_home}")

if [[ "${dry_run}" == "true" ]]; then
  printf 'dry_run=true\n'
  printf 'domain=%s\n' "${domain}"
  printf 'wildcard_domain=%s\n' "${wildcard_domain}"
  printf 'namespace=%s\n' "${namespace}"
  printf 'secret_name=%s\n' "${secret_name}"
  printf 'dns_provider=%s\n' "${dns_provider}"
  printf 'renew_before_days=%s\n' "${renew_before_days}"
  if [[ "${dns_provider}" == "cloudflare" ]]; then
    printf 'acme_issue=acme.sh --issue --dns dns_cf -d %s -d %s --server %s\n' "${wildcard_domain}" "${domain}" "${server}"
  else
    printf 'acme_issue=acme.sh --issue --dns dns_fugue -d %s -d %s --server %s\n' "${wildcard_domain}" "${domain}" "${server}"
  fi
  printf 'secret_apply=kubectl -n %s create secret tls %s --cert <fullchain> --key <key> --dry-run=client -o yaml | kubectl apply -f -\n' "${namespace}" "${secret_name}"
  exit 0
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
cert_file="${tmpdir}/tls.crt"
key_file="${tmpdir}/tls.key"

if (( renew_before_days > 0 )); then
  command -v openssl >/dev/null 2>&1 || fail "openssl is required when --renew-before-days is set"
fi
command -v "${kubectl_cmd}" >/dev/null 2>&1 || fail "kubectl executable not found: ${kubectl_cmd}"
if [[ "${check_only}" == "true" ]]; then
  if current_secret_cert_is_fresh "${renew_before_days}"; then
    printf 'certificate preflight passed; Kubernetes TLS Secret %s/%s is valid for at least %s days\n' "${namespace}" "${secret_name}" "${renew_before_days}"
    exit 0
  fi
  fail "certificate preflight failed; Kubernetes TLS Secret ${namespace}/${secret_name} is missing, invalid, or expires within ${renew_before_days} days"
fi
if current_secret_cert_is_fresh "${renew_before_days}"; then
  printf 'skipping renewal; Kubernetes TLS Secret %s/%s is valid for at least %s days\n' "${namespace}" "${secret_name}" "${renew_before_days}"
  exit 0
fi

command -v "${acme_cmd}" >/dev/null 2>&1 || fail "acme.sh executable not found: ${acme_cmd}"

if [[ "${dns_provider}" == "cloudflare" ]]; then
  export CF_Token="${cloudflare_token}"
  "${acme_cmd}" "${acme_home_args[@]}" --issue --dns dns_cf -d "${wildcard_domain}" -d "${domain}" --server "${server}"
else
  dnsapi_dir="${acme_home}/dnsapi"
  mkdir -p "${dnsapi_dir}" "${tmpdir}/state"
  cat >"${dnsapi_dir}/dns_fugue.sh" <<'HOOK'
#!/usr/bin/env sh

_fugue_api_request() {
  method="$1"
  path="$2"
  data="${3:-}"
  if [ -n "${data}" ]; then
    curl -fsS -X "${method}" \
      -H "Authorization: Bearer ${FUGUE_ACME_API_KEY}" \
      -H "Content-Type: application/json" \
      --data "${data}" \
      "${FUGUE_ACME_API_URL%/}${path}"
  else
    curl -fsS -X "${method}" \
      -H "Authorization: Bearer ${FUGUE_ACME_API_KEY}" \
      "${FUGUE_ACME_API_URL%/}${path}"
  fi
}

_fugue_state_key() {
  printf '%s' "$1:$2" | sed 's/[^A-Za-z0-9_.-]/_/g'
}

dns_fugue_add() {
  fulldomain="$1"
  txtvalue="$2"
  payload=$(printf '{"zone":"%s","name":"%s","value":"%s","ttl":%s,"owner":"%s","expires_in_seconds":%s}' "${FUGUE_ACME_ZONE}" "${fulldomain}" "${txtvalue}" "${FUGUE_ACME_CHALLENGE_TTL:-60}" "${FUGUE_ACME_OWNER:-wildcard-tls}" "${FUGUE_ACME_EXPIRES_IN_SECONDS:-3600}")
  response=$(_fugue_api_request POST "/v1/dns/acme-challenges" "${payload}") || return 1
  challenge_id=$(printf '%s' "${response}" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
  if [ -n "${challenge_id}" ] && [ -n "${FUGUE_ACME_CHALLENGE_STATE_DIR}" ]; then
    printf '%s' "${challenge_id}" >"${FUGUE_ACME_CHALLENGE_STATE_DIR}/$(_fugue_state_key "${fulldomain}" "${txtvalue}").id"
  fi
}

dns_fugue_rm() {
  fulldomain="$1"
  txtvalue="$2"
  state_file="${FUGUE_ACME_CHALLENGE_STATE_DIR}/$(_fugue_state_key "${fulldomain}" "${txtvalue}").id"
  if [ ! -r "${state_file}" ]; then
    return 0
  fi
  challenge_id=$(cat "${state_file}")
  if [ -z "${challenge_id}" ]; then
    return 0
  fi
  _fugue_api_request DELETE "/v1/dns/acme-challenges/${challenge_id}" >/dev/null || return 0
}
HOOK
  chmod 0755 "${dnsapi_dir}/dns_fugue.sh"
  export FUGUE_ACME_API_URL="${api_url}"
  export FUGUE_ACME_API_KEY="${api_key}"
  export FUGUE_ACME_ZONE="${domain}"
  export FUGUE_ACME_OWNER="wildcard:${domain}"
  export FUGUE_ACME_CHALLENGE_TTL="${challenge_ttl}"
  export FUGUE_ACME_EXPIRES_IN_SECONDS="${challenge_expires_in_seconds}"
  export FUGUE_ACME_CHALLENGE_STATE_DIR="${tmpdir}/state"
  "${acme_cmd}" "${acme_home_args[@]}" --issue --dns dns_fugue -d "${wildcard_domain}" -d "${domain}" --server "${server}"
fi
"${acme_cmd}" "${acme_home_args[@]}" --install-cert -d "${wildcard_domain}" --server "${server}" \
  --fullchain-file "${cert_file}" \
  --key-file "${key_file}"

"${kubectl_cmd}" -n "${namespace}" create secret tls "${secret_name}" \
  --cert "${cert_file}" \
  --key "${key_file}" \
  --dry-run=client \
  -o yaml | "${kubectl_cmd}" apply -f -

printf 'updated Kubernetes TLS Secret %s/%s\n' "${namespace}" "${secret_name}"
printf 'enable Helm static TLS with FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true and FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME=%s\n' "${secret_name}"
