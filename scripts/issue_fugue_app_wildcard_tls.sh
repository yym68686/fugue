#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  issue_fugue_app_wildcard_tls.sh --cloudflare-env-file <path>

Issues/renews a wildcard app certificate with acme.sh DNS-01 via Cloudflare
and writes it to a Kubernetes TLS Secret for fugue-edge Caddy static loading.

Options:
  --cloudflare-env-file <path>  Env file containing CLOUDFLARE_DNS_API_TOKEN.
  --namespace <namespace>       Kubernetes namespace. Default: fugue-system.
  --secret-name <name>          Kubernetes TLS Secret name. Default: fugue-app-wildcard-tls.
  --domain <domain>             App base domain. Default: fugue.pro.
  --server <server>             ACME server. Default: letsencrypt.
  --acme-home <dir>             Optional acme.sh home directory.
  --acme <path>                 acme.sh executable. Default: acme.sh.
  --kubectl <path>              kubectl executable. Default: kubectl.
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

namespace="${FUGUE_NAMESPACE:-fugue-system}"
secret_name="${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME:-fugue-app-wildcard-tls}"
domain="${FUGUE_APP_BASE_DOMAIN:-fugue.pro}"
server="${ACME_SERVER:-letsencrypt}"
cloudflare_env_file="${CLOUDFLARE_ENV_FILE:-}"
acme_home="${ACME_HOME:-}"
acme_cmd="${ACME_SH:-acme.sh}"
kubectl_cmd="${KUBECTL:-kubectl}"
dry_run="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cloudflare-env-file) require_value "$1" "${2:-}"; cloudflare_env_file="$2"; shift 2 ;;
    --namespace) require_value "$1" "${2:-}"; namespace="$2"; shift 2 ;;
    --secret-name) require_value "$1" "${2:-}"; secret_name="$2"; shift 2 ;;
    --domain) require_value "$1" "${2:-}"; domain="$2"; shift 2 ;;
    --server) require_value "$1" "${2:-}"; server="$2"; shift 2 ;;
    --acme-home) require_value "$1" "${2:-}"; acme_home="$2"; shift 2 ;;
    --acme) require_value "$1" "${2:-}"; acme_cmd="$2"; shift 2 ;;
    --kubectl) require_value "$1" "${2:-}"; kubectl_cmd="$2"; shift 2 ;;
    --dry-run) dry_run="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "${namespace}" ]] || fail "--namespace is required"
[[ -n "${secret_name}" ]] || fail "--secret-name is required"
[[ -n "${domain}" ]] || fail "--domain is required"
[[ "${domain}" != "*."* ]] || fail "--domain must be the base domain, not a wildcard"

cloudflare_token="${CLOUDFLARE_DNS_API_TOKEN:-${CLOUDFLARE_API_TOKEN:-}}"
if [[ -z "${cloudflare_token}" && -n "${cloudflare_env_file}" ]]; then
  [[ -f "${cloudflare_env_file}" ]] || fail "Cloudflare env file not found: ${cloudflare_env_file}"
  cloudflare_token="$(read_env_value "${cloudflare_env_file}" "CLOUDFLARE_DNS_API_TOKEN")"
  if [[ -z "${cloudflare_token}" ]]; then
    cloudflare_token="$(read_env_value "${cloudflare_env_file}" "CLOUDFLARE_API_TOKEN")"
  fi
  cloudflare_token="$(strip_env_quotes "${cloudflare_token}")"
fi
[[ -n "${cloudflare_token}" ]] || fail "CLOUDFLARE_DNS_API_TOKEN is required; pass --cloudflare-env-file or export it"

wildcard_domain="*.${domain}"
acme_home_args=()
if [[ -n "${acme_home}" ]]; then
  acme_home_args=(--home "${acme_home}")
fi

if [[ "${dry_run}" == "true" ]]; then
  printf 'dry_run=true\n'
  printf 'domain=%s\n' "${domain}"
  printf 'wildcard_domain=%s\n' "${wildcard_domain}"
  printf 'namespace=%s\n' "${namespace}"
  printf 'secret_name=%s\n' "${secret_name}"
  printf 'acme_issue=acme.sh --issue --dns dns_cf -d %s -d %s --server %s\n' "${wildcard_domain}" "${domain}" "${server}"
  printf 'secret_apply=kubectl -n %s create secret tls %s --cert <fullchain> --key <key> --dry-run=client -o yaml | kubectl apply -f -\n' "${namespace}" "${secret_name}"
  exit 0
fi

command -v "${acme_cmd}" >/dev/null 2>&1 || fail "acme.sh executable not found: ${acme_cmd}"
command -v "${kubectl_cmd}" >/dev/null 2>&1 || fail "kubectl executable not found: ${kubectl_cmd}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
cert_file="${tmpdir}/tls.crt"
key_file="${tmpdir}/tls.key"

export CF_Token="${cloudflare_token}"
"${acme_cmd}" "${acme_home_args[@]}" --issue --dns dns_cf -d "${wildcard_domain}" -d "${domain}" --server "${server}"
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
