#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

fail() {
  printf 'test_probe_fugue_public_dns.sh: %s\n' "$*" >&2
  exit 1
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
mkdir -p "${tmpdir}/bin"

cat >"${tmpdir}/bin/fugue" <<'EOF'
#!/usr/bin/env bash
[[ "$*" == "--json admin dns answer-check api.example.test" ]]
cat <<'JSON'
{"pass":true,"route_ready_edge_groups":["edge-group-country-us"],"route_explain":{"route":{"route_generation":"routegen_test"},"routes":[]}}
JSON
EOF

cat >"${tmpdir}/bin/dig" <<'EOF'
#!/usr/bin/env bash
if [[ " $* " == *" A "* ]]; then
  printf '%s\n' '203.0.113.10'
fi
EOF

cat >"${tmpdir}/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${PROBE_TEST_CURL_LOG}"
output_file=""
while (( $# > 0 )); do
  case "$1" in
    --output)
      output_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
[[ -n "${output_file}" ]]
printf '%s\n' 'ok' >"${output_file}"
printf '%s' '200'
EOF

chmod 0755 "${tmpdir}/bin/fugue" "${tmpdir}/bin/dig" "${tmpdir}/bin/curl"

curl_log="${tmpdir}/curl.log"
PATH="${tmpdir}/bin:${PATH}" \
  PROBE_TEST_CURL_LOG="${curl_log}" \
  FUGUE_PUBLIC_DNS_RESOLVERS="1.1.1.1,8.8.8.8" \
	FUGUE_PUBLIC_DNS_REQUIRED_IPS="203.0.113.10" \
	FUGUE_PUBLIC_DNS_FORBIDDEN_IPS="203.0.113.11" \
  bash "${REPO_ROOT}/scripts/probe_fugue_public_dns.sh" "api.example.test" >"${tmpdir}/probe.out"

grep -Fq -- 'control_plane hostname=api.example.test' "${tmpdir}/probe.out" || fail "missing control-plane evidence"
grep -Fq -- 'public_dns resolver=1.1.1.1' "${tmpdir}/probe.out" || fail "missing first public resolver evidence"
grep -Fq -- 'public_dns resolver=8.8.8.8' "${tmpdir}/probe.out" || fail "missing second public resolver evidence"
grep -Fq -- 'candidate host=api.example.test ip=203.0.113.10' "${tmpdir}/probe.out" || fail "missing exact candidate probe"
grep -Fq -- 'public_path hostname=api.example.test http_status=200 pass=true' "${tmpdir}/probe.out" || fail "missing public path proof"

[[ "$(wc -l <"${curl_log}" | awk '{$1=$1; print}')" == "2" ]] || fail "expected one exact-IP probe and one public-path probe"
grep -Fq -- '--connect-to api.example.test:443:203.0.113.10:443' "${curl_log}" || fail "exact-IP probe did not preserve TLS/Host identity"
if grep -Fq -- '--resolve' "${curl_log}"; then
  fail "public validation must not use forced resolver mappings"
fi
public_probe_args="$(sed -n '2p' "${curl_log}")"
[[ "${public_probe_args}" == *'https://api.example.test/'* ]] || fail "public-path probe is missing the target URL"
[[ "${public_probe_args}" != *'--connect-to'* ]] || fail "public-path probe must use normal DNS resolution"

rm "${tmpdir}/bin/dig"
cat >"${tmpdir}/bin/fugue-public-dns-query" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == "1.1.1.1" || "$1" == "8.8.8.8" ]]
[[ "$2" == "api.example.test" ]]
[[ "$3" == "A" || "$3" == "AAAA" ]]
if [[ "$3" == "A" ]]; then
  printf '%s\n' '203.0.113.10'
fi
EOF
chmod 0755 "${tmpdir}/bin/fugue-public-dns-query"
: >"${curl_log}"
PATH="${tmpdir}/bin:${PATH}" \
  PROBE_TEST_CURL_LOG="${curl_log}" \
  FUGUE_PUBLIC_DNS_QUERY_BIN="${tmpdir}/bin/fugue-public-dns-query" \
  FUGUE_PUBLIC_DNS_RESOLVERS="1.1.1.1,8.8.8.8" \
  FUGUE_PUBLIC_DNS_REQUIRED_IPS="203.0.113.10" \
  FUGUE_PUBLIC_DNS_FORBIDDEN_IPS="203.0.113.11" \
  bash "${REPO_ROOT}/scripts/probe_fugue_public_dns.sh" "api.example.test" >"${tmpdir}/fallback-probe.out"
grep -Fq -- 'public_dns resolver=1.1.1.1' "${tmpdir}/fallback-probe.out" || fail "self-contained DNS query path did not run"

if PATH="${tmpdir}/bin:${PATH}" PROBE_TEST_CURL_LOG="${curl_log}" \
  bash "${REPO_ROOT}/scripts/probe_fugue_public_dns.sh" '--resolve' >"${tmpdir}/bad.out" 2>"${tmpdir}/bad.err"; then
  fail "option-like hostnames must be rejected"
fi
