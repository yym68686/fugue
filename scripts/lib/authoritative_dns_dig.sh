#!/usr/bin/env bash

# Shared, wire-attested DiG execution for authoritative DNS release gates.
# This file is sourced by release scripts; it intentionally installs no traps
# and exposes no environment switch that can force modern or legacy mode.

if [[ "${AUTHORITATIVE_DNS_DIG_HELPER_LOADED:-false}" == "true" ]]; then
  return 0 2>/dev/null || exit 0
fi
AUTHORITATIVE_DNS_DIG_HELPER_LOADED=true

AUTHORITATIVE_DNS_DIG_WIRE_POLICY="authoritative-dns-dig-wire-v1"
AUTHORITATIVE_DNS_DIG_MAX_TIMEOUT_SECONDS="86400"
AUTHORITATIVE_DNS_DIG_QUERY_ARGV=()
AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR=""
AUTHORITATIVE_DNS_DIG_PROBE_RESULT=""
AUTHORITATIVE_DNS_DIG_PROBE_UDP_SHA256=""
AUTHORITATIVE_DNS_DIG_PROBE_TCP_SHA256=""
AUTHORITATIVE_DNS_DIG_PROBE_UDP_TRANSACTION_ID=""
AUTHORITATIVE_DNS_DIG_PROBE_TCP_TRANSACTION_ID=""

authoritative_dns_dig_error() {
  printf '[authoritative-dns-dig] ERROR: %s\n' "$*" >&2
  return 1
}

authoritative_dns_dig_hash_fields() {
  python3 - "$@" <<'PY'
import hashlib
import sys

digest = hashlib.sha256()
for value in sys.argv[1:]:
    encoded = value.encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
print(digest.hexdigest())
PY
}

authoritative_dns_dig_hash_stream() {
  python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())'
}

authoritative_dns_dig_resolve_binary() {
  local candidate=""

  candidate="$(type -P dig 2>/dev/null || true)"
  [[ -n "${candidate}" ]] || {
    authoritative_dns_dig_error "dig executable was not found on PATH"
    return 1
  }
  python3 - "${candidate}" <<'PY'
import os
import stat
import sys

path = os.path.realpath(sys.argv[1])
try:
    metadata = os.stat(path)
except OSError as exc:
    raise SystemExit(f"cannot stat dig executable: {exc}")
if not os.path.isabs(path) or not stat.S_ISREG(metadata.st_mode) or not os.access(path, os.X_OK):
    raise SystemExit("dig must resolve to an absolute executable regular file")
print(path)
PY
}

authoritative_dns_dig_file_identity() {
  local path="$1"
  local require_executable="${2:-true}"

  python3 - "${path}" "${require_executable}" <<'PY'
import hashlib
import json
import os
import stat
import sys

path = os.path.realpath(sys.argv[1])
require_executable = sys.argv[2] == "true"
try:
    metadata = os.stat(path)
except OSError as exc:
    raise SystemExit(f"cannot stat executable: {exc}")
if not os.path.isabs(path) or not stat.S_ISREG(metadata.st_mode):
    raise SystemExit("path is not an absolute regular file")
if require_executable and not os.access(path, os.X_OK):
    raise SystemExit("path is not executable")
if not require_executable and not os.access(path, os.R_OK):
    raise SystemExit("path is not readable")
digest = hashlib.sha256()
with open(path, "rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
print(json.dumps({
    "ctime_ns": metadata.st_ctime_ns,
    "dev": metadata.st_dev,
    "gid": metadata.st_gid,
    "ino": metadata.st_ino,
    "mode": stat.S_IMODE(metadata.st_mode),
    "mtime_ns": metadata.st_mtime_ns,
    "path": path,
    "sha256": digest.hexdigest(),
    "size": metadata.st_size,
    "uid": metadata.st_uid,
}, sort_keys=True, separators=(",", ":")))
PY
}

authoritative_dns_dig_helper_path() {
  python3 - "${BASH_SOURCE[0]}" <<'PY'
import os
import sys
print(os.path.realpath(sys.argv[1]))
PY
}

authoritative_dns_dig_builder_identity() {
  {
    printf '%s\n' "${AUTHORITATIVE_DNS_DIG_WIRE_POLICY}"
    printf '%s\n' "${AUTHORITATIVE_DNS_DIG_MAX_TIMEOUT_SECONDS}"
    declare -f authoritative_dns_dig_validate_timeout_seconds
    declare -f authoritative_dns_dig_build_query_argv
  } | authoritative_dns_dig_hash_stream
}

authoritative_dns_dig_validate_timeout_seconds() {
  local value="${1:-}"
  local maximum="${AUTHORITATIVE_DNS_DIG_MAX_TIMEOUT_SECONDS}"
  local value_length="${#value}"
  local maximum_length="${#maximum}"
  local LC_ALL=C

  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || {
    authoritative_dns_dig_error "query timeout must be a positive base-10 integer"
    return 1
  }
  if (( value_length > maximum_length )) ||
    { (( value_length == maximum_length )) && [[ "${value}" > "${maximum}" ]]; }; then
    authoritative_dns_dig_error "query timeout exceeds the maximum ${maximum} seconds"
    return 1
  fi
}

authoritative_dns_dig_controlled_capture() {
  local binary="$1"
  shift
  local empty_home=""
  local output=""
  local rc=0

  empty_home="$(mktemp -d "${TMPDIR:-/tmp}/fugue-dig-home.XXXXXX")" || return 1
  chmod 700 "${empty_home}" || {
    rm -rf "${empty_home}"
    return 1
  }
  output="$(python3 - "${empty_home}" "${binary}" "$@" <<'PY'
import os
import subprocess
import sys

home = sys.argv[1]
argv = sys.argv[2:]
environment = {
    "HOME": home,
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
}
try:
    completed = subprocess.run(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=environment,
        timeout=5,
        check=False,
    )
except subprocess.TimeoutExpired as exc:
    if exc.stdout:
        sys.stdout.buffer.write(exc.stdout)
    raise SystemExit(124)
sys.stdout.buffer.write(completed.stdout)
raise SystemExit(completed.returncode)
PY
)" || rc=$?
  if [[ -n "$(find "${empty_home}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    rc=125
    output="${output}${output:+$'\n'}controlled DiG HOME was modified"
  fi
  rm -rf "${empty_home}"
  printf '%s' "${output}"
  return "${rc}"
}

authoritative_dns_dig_build_query_argv() {
  local binary="$1"
  local mode="$2"
  local server="$3"
  local port="$4"
  local timeout_seconds="$5"
  local transport="$6"
  local query_name="$7"
  local query_type="$8"

  [[ "${binary}" == /* && -f "${binary}" && -x "${binary}" ]] || {
    authoritative_dns_dig_error "query binary is not an absolute executable regular file"
    return 1
  }
  case "${mode}" in
    modern|legacy) ;;
    *)
      authoritative_dns_dig_error "query mode is not attested"
      return 1
      ;;
  esac
  authoritative_dns_dig_validate_timeout_seconds "${timeout_seconds}" || return 1
  [[ -n "${server}" && "${server}" != *[[:space:]@+]* ]] || {
    authoritative_dns_dig_error "query server contains unsafe characters"
    return 1
  }
  [[ "${port}" =~ ^[1-9][0-9]*$ && "${port}" -le 65535 ]] || {
    authoritative_dns_dig_error "query port is invalid"
    return 1
  }
  case "${transport}" in
    udp|tcp) ;;
    *)
      authoritative_dns_dig_error "query transport must be udp or tcp"
      return 1
      ;;
  esac
  [[ "${query_name}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9.])?$ &&
      "${query_name}" != *[[:space:]@+]* ]] || {
    authoritative_dns_dig_error "query name contains unsafe characters"
    return 1
  }
  case "${query_type}" in
    A|AAAA|CNAME|SOA) ;;
    *)
      authoritative_dns_dig_error "query type is not allowed"
      return 1
      ;;
  esac

  AUTHORITATIVE_DNS_DIG_QUERY_ARGV=(
    "${binary}"
    "@${server}"
    -p "${port}"
    "+time=${timeout_seconds}"
    "+tries=1"
    "+norecurse"
    "+adflag"
    "+nocdflag"
    "+nosearch"
    "+edns=0"
    "+bufsize=1232"
    "+subnet=0.0.0.0/0"
    "+noall"
    "+comments"
    "+question"
    "+answer"
  )
  [[ "${transport}" != "tcp" ]] || AUTHORITATIVE_DNS_DIG_QUERY_ARGV+=(+tcp)
  AUTHORITATIVE_DNS_DIG_QUERY_ARGV+=("${query_name}" "${query_type}")
  if [[ "${mode}" == "modern" ]]; then
    AUTHORITATIVE_DNS_DIG_QUERY_ARGV+=(+nocookie)
  fi
}

authoritative_dns_dig_run_argv_bounded() {
  local empty_home="$1"
  local result_file="$2"
  shift 2

  python3 - "${empty_home}" "${result_file}" "$@" <<'PY'
import json
import os
import subprocess
import sys

home = sys.argv[1]
result_file = sys.argv[2]
argv = sys.argv[3:]
environment = {
    "HOME": home,
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
}
result = {"returncode": None, "timed_out": False}
try:
    completed = subprocess.run(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=environment,
        timeout=5,
        check=False,
    )
    result["returncode"] = completed.returncode
    sys.stdout.buffer.write(completed.stdout)
except subprocess.TimeoutExpired as exc:
    result["returncode"] = 124
    result["timed_out"] = True
    if exc.stdout:
        sys.stdout.buffer.write(exc.stdout)
with open(result_file, "w", encoding="utf-8") as handle:
    json.dump(result, handle, sort_keys=True, separators=(",", ":"))
PY
}

authoritative_dns_dig_wire_probe_one() {
  local binary="$1"
  local mode="$2"
  local transport="$3"
  local work_dir=""
  local home_dir=""
  local port_file=""
  local result_file=""
  local stop_file=""
  local exec_result_file=""
  local server_log=""
  local dig_log=""
  local server_pid=""
  local port=""
  local attempt=0
  local exec_infra_rc=0
  local server_rc=0
  local packet_count=""
  local wire_valid=""
  local wire_sha256=""
  local transaction_id=""
  local wire_error=""
  local command_rc=""
  local command_timed_out=""

  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/fugue-dig-probe.XXXXXX")" || return 1
  home_dir="${work_dir}/home"
  port_file="${work_dir}/port"
  result_file="${work_dir}/wire-result.json"
  stop_file="${work_dir}/stop"
  exec_result_file="${work_dir}/exec-result.json"
  server_log="${work_dir}/server.log"
  dig_log="${work_dir}/dig.log"
  mkdir -m 700 "${home_dir}" || {
    rm -rf "${work_dir}"
    return 1
  }

  python3 - "${port_file}" "${result_file}" "${stop_file}" "${transport}" >"${server_log}" 2>&1 <<'PY' &
import hashlib
import json
import os
import select
import socket
import struct
import sys
import time

port_file, result_file, stop_file, expected_transport = sys.argv[1:5]
expected_name = "fugue-dig-attest.invalid."
expected_type = 6
expected_class = 1
expected_flags = 0x0020
expected_ecs = b"\x00\x01\x00\x00"
packet_count = 0
valid_packets = 0
wire_sha256 = ""
transaction_id = None
errors = []

def wire_name(value):
    labels = value.rstrip(".").split(".")
    return b"".join(bytes([len(label)]) + label.encode("ascii") for label in labels) + b"\x00"

def read_exact(connection, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise ValueError("truncated DNS-over-TCP frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)

def parse_name(packet, offset):
    labels = []
    wire_length = 0
    while True:
        if offset >= len(packet):
            raise ValueError("truncated DNS name")
        length = packet[offset]
        offset += 1
        wire_length += 1
        if length == 0:
            break
        if length & 0xC0 or length > 63 or offset + length > len(packet):
            raise ValueError("compressed or malformed DNS question name")
        label = packet[offset:offset + length]
        try:
            labels.append(label.decode("ascii").lower())
        except UnicodeDecodeError as exc:
            raise ValueError("non-ASCII DNS question name") from exc
        offset += length
        wire_length += length
        if wire_length > 255:
            raise ValueError("DNS question name exceeds wire limit")
    return ".".join(labels) + ".", offset

def validate_query(packet):
    if len(packet) < 12:
        raise ValueError("short DNS header")
    identifier, flags, qdcount, ancount, nscount, arcount = struct.unpack("!HHHHHH", packet[:12])
    if flags != expected_flags:
        raise ValueError(f"unexpected DNS query flags 0x{flags:04x}")
    if (qdcount, ancount, nscount, arcount) != (1, 0, 0, 1):
        raise ValueError("DNS section counts are not exactly 1/0/0/1")
    name, offset = parse_name(packet, 12)
    if name != expected_name or offset + 4 > len(packet):
        raise ValueError("DNS question name is not the attestation name")
    qtype, qclass = struct.unpack("!HH", packet[offset:offset + 4])
    offset += 4
    if (qtype, qclass) != (expected_type, expected_class):
        raise ValueError("DNS question type or class drifted")
    if offset + 11 > len(packet) or packet[offset] != 0:
        raise ValueError("OPT owner is missing or not root")
    opt_type, udp_size, ttl, rdlength = struct.unpack("!HHIH", packet[offset + 1:offset + 11])
    offset += 11
    if opt_type != 41 or udp_size != 1232 or ttl != 0:
        raise ValueError("OPT type, UDP size, EDNS version, or flags drifted")
    if offset + rdlength != len(packet):
        raise ValueError("OPT RDLENGTH is malformed or trailing bytes exist")
    option_end = offset + rdlength
    options = []
    while offset < option_end:
        if offset + 4 > option_end:
            raise ValueError("truncated EDNS option header")
        code, length = struct.unpack("!HH", packet[offset:offset + 4])
        offset += 4
        if offset + length > option_end:
            raise ValueError("truncated EDNS option payload")
        options.append((code, packet[offset:offset + length]))
        offset += length
    if options != [(8, expected_ecs)]:
        raise ValueError("EDNS options are not exactly one unmapped IPv4 ECS option")
    return identifier

def response_for(identifier):
    question = wire_name(expected_name) + struct.pack("!HH", expected_type, expected_class)
    mname = wire_name("ns1.fugue-dig-attest.invalid.")
    rname = wire_name("hostmaster.fugue-dig-attest.invalid.")
    rdata = mname + rname + struct.pack("!IIIII", 1, 300, 60, 3600, 60)
    answer = b"\xc0\x0c" + struct.pack("!HHIH", expected_type, expected_class, 60, len(rdata)) + rdata
    ecs_option = struct.pack("!HH", 8, len(expected_ecs)) + expected_ecs
    opt = b"\x00" + struct.pack("!HHIH", 41, 1232, 0, len(ecs_option)) + ecs_option
    header = struct.pack("!HHHHHH", identifier, 0x8400, 1, 1, 0, 1)
    return header + question + answer + opt

udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp.bind(("127.0.0.1", 0))
port = udp.getsockname()[1]
tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcp.bind(("127.0.0.1", port))
tcp.listen(4)
with open(port_file, "w", encoding="ascii") as handle:
    handle.write(str(port))

deadline = time.monotonic() + 8
while time.monotonic() < deadline:
    stopping = os.path.exists(stop_file)
    timeout = 0.05 if stopping else min(0.1, deadline - time.monotonic())
    readable, _, _ = select.select([udp, tcp], [], [], max(0, timeout))
    if not readable:
        if stopping:
            break
        continue
    for listener in readable:
        observed_transport = "udp" if listener is udp else "tcp"
        connection = None
        try:
            if listener is udp:
                packet, address = udp.recvfrom(65535)
            else:
                connection, _ = tcp.accept()
                connection.settimeout(1)
                frame_length = struct.unpack("!H", read_exact(connection, 2))[0]
                packet = read_exact(connection, frame_length)
                connection.setblocking(False)
                try:
                    if connection.recv(1):
                        raise ValueError("extra DNS-over-TCP frame bytes")
                except BlockingIOError:
                    pass
            packet_count += 1
            if observed_transport != expected_transport:
                raise ValueError(f"query used {observed_transport}, expected {expected_transport}")
            identifier = validate_query(packet)
            if packet_count != 1:
                raise ValueError("DiG emitted more than one DNS request")
            # The DNS transaction ID is deliberately randomized by DiG.  It is
            # validated as a two-byte header field above, then zeroed only for
            # the equality digest; every other raw query byte remains bound.
            transaction_id = identifier
            wire_sha256 = hashlib.sha256(b"\x00\x00" + packet[2:]).hexdigest()
            valid_packets += 1
            response = response_for(identifier)
            if listener is udp:
                udp.sendto(response, address)
            else:
                connection.setblocking(True)
                connection.sendall(struct.pack("!H", len(response)) + response)
        except Exception as exc:
            errors.append(str(exc))
        finally:
            if connection is not None:
                connection.close()

udp.close()
tcp.close()
result = {
    "errors": errors,
    "packet_count": packet_count,
    "transaction_id": transaction_id,
    "valid": packet_count == 1 and valid_packets == 1 and not errors,
    "wire_sha256": wire_sha256,
}
with open(result_file, "w", encoding="utf-8") as handle:
    json.dump(result, handle, sort_keys=True, separators=(",", ":"))
PY
  server_pid=$!

  for attempt in {1..250}; do
    if [[ -s "${port_file}" ]]; then
      port="$(cat "${port_file}")"
      break
    fi
    if ! kill -0 "${server_pid}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.02
  done
  if ! [[ "${port}" =~ ^[1-9][0-9]*$ ]]; then
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
    AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR="wire probe server did not publish a port: $(tail -c 2000 "${server_log}" 2>/dev/null || true)"
    rm -rf "${work_dir}"
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=hard-failure
    return 0
  fi

  authoritative_dns_dig_build_query_argv "${binary}" "${mode}" 127.0.0.1 "${port}" 1 "${transport}" \
    fugue-dig-attest.invalid. SOA || {
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
    rm -rf "${work_dir}"
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=hard-failure
    return 0
  }
  authoritative_dns_dig_run_argv_bounded "${home_dir}" "${exec_result_file}" \
    "${AUTHORITATIVE_DNS_DIG_QUERY_ARGV[@]}" >"${dig_log}" 2>&1 || exec_infra_rc=$?
  : >"${stop_file}"
  wait "${server_pid}" || server_rc=$?

  if (( exec_infra_rc != 0 || server_rc != 0 )) || [[ ! -s "${exec_result_file}" || ! -s "${result_file}" ]]; then
    AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR="wire probe infrastructure failed: $(tail -c 2000 "${server_log}" 2>/dev/null || true) $(tail -c 2000 "${dig_log}" 2>/dev/null || true)"
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=hard-failure
    rm -rf "${work_dir}"
    return 0
  fi
  IFS=$'\t' read -r command_rc command_timed_out < <(python3 - "${exec_result_file}" <<'PY'
import json
import sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
print(f"{value.get('returncode')}\t{'true' if value.get('timed_out') else 'false'}")
PY
  )
  IFS=$'\t' read -r packet_count wire_valid wire_sha256 transaction_id wire_error < <(python3 - "${result_file}" <<'PY'
import json
import sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
errors = "; ".join(str(item).replace("\t", " ").replace("\n", " ") for item in value.get("errors", []))
print("\t".join([
    str(value.get("packet_count", "")),
    "true" if value.get("valid") else "false",
    str(value.get("wire_sha256") or ""),
    str(value.get("transaction_id") if value.get("transaction_id") is not None else ""),
    errors,
]))
PY
  )
  if [[ -n "$(find "${home_dir}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    wire_error="${wire_error}${wire_error:+; }controlled DiG HOME was modified"
    wire_valid=false
  fi

  if [[ "${command_timed_out}" == "false" && "${command_rc}" == "0" &&
        "${packet_count}" == "1" && "${wire_valid}" == "true" ]]; then
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=success
  elif [[ "${command_timed_out}" == "false" && "${command_rc}" != "0" &&
          "${packet_count}" == "0" ]]; then
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=unsupported
  else
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=hard-failure
  fi
  AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR="transport=${transport} rc=${command_rc} timeout=${command_timed_out} packets=${packet_count} valid=${wire_valid} error=${wire_error} output=$(tail -c 1000 "${dig_log}" 2>/dev/null || true)"
  if [[ "${transport}" == "udp" ]]; then
    AUTHORITATIVE_DNS_DIG_PROBE_UDP_SHA256="${wire_sha256}"
    AUTHORITATIVE_DNS_DIG_PROBE_UDP_TRANSACTION_ID="${transaction_id}"
  else
    AUTHORITATIVE_DNS_DIG_PROBE_TCP_SHA256="${wire_sha256}"
    AUTHORITATIVE_DNS_DIG_PROBE_TCP_TRANSACTION_ID="${transaction_id}"
  fi
  rm -rf "${work_dir}"
}

authoritative_dns_dig_probe_candidate() {
  local binary="$1"
  local mode="$2"
  local transport=""
  local transport_result=""
  local all_success=true
  local all_unsupported=true
  local errors=()

  AUTHORITATIVE_DNS_DIG_PROBE_UDP_SHA256=""
  AUTHORITATIVE_DNS_DIG_PROBE_TCP_SHA256=""
  AUTHORITATIVE_DNS_DIG_PROBE_UDP_TRANSACTION_ID=""
  AUTHORITATIVE_DNS_DIG_PROBE_TCP_TRANSACTION_ID=""
  for transport in udp tcp; do
    authoritative_dns_dig_wire_probe_one "${binary}" "${mode}" "${transport}" || return 1
    transport_result="${AUTHORITATIVE_DNS_DIG_PROBE_RESULT}"
    errors+=("${AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR}")
    [[ "${transport_result}" == "success" ]] || all_success=false
    [[ "${transport_result}" == "unsupported" ]] || all_unsupported=false
  done
  if [[ "${all_success}" == "true" ]]; then
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=success
  elif [[ "${all_unsupported}" == "true" ]]; then
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=unsupported
  else
    AUTHORITATIVE_DNS_DIG_PROBE_RESULT=hard-failure
  fi
  AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR="${errors[*]}"
}

authoritative_dns_dig_state_present() {
  [[ -n "${AUTHORITATIVE_DNS_DIG_ATTESTED:-}${AUTHORITATIVE_DNS_DIG_BIN:-}${AUTHORITATIVE_DNS_DIG_IDENTITY:-}${AUTHORITATIVE_DNS_DIG_HELPER_IDENTITY:-}${AUTHORITATIVE_DNS_DIG_BUILDER_IDENTITY:-}${AUTHORITATIVE_DNS_DIG_VERSION:-}${AUTHORITATIVE_DNS_DIG_HELP_SHA256:-}${AUTHORITATIVE_DNS_DIG_MODE:-}${AUTHORITATIVE_DNS_DIG_UDP_WIRE_SHA256:-}${AUTHORITATIVE_DNS_DIG_TCP_WIRE_SHA256:-}${AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256:-}" ]]
}

authoritative_dns_dig_state_complete() {
  [[ "${AUTHORITATIVE_DNS_DIG_ATTESTED:-}" == "${AUTHORITATIVE_DNS_DIG_WIRE_POLICY}" &&
      -n "${AUTHORITATIVE_DNS_DIG_BIN:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_IDENTITY:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_HELPER_IDENTITY:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_BUILDER_IDENTITY:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_VERSION:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_HELP_SHA256:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_MODE:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_UDP_WIRE_SHA256:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_TCP_WIRE_SHA256:-}" &&
      -n "${AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256:-}" ]]
}

authoritative_dns_dig_compute_attestation_sha256() {
  authoritative_dns_dig_hash_fields \
    "${AUTHORITATIVE_DNS_DIG_WIRE_POLICY}" \
    "${AUTHORITATIVE_DNS_DIG_BIN}" \
    "${AUTHORITATIVE_DNS_DIG_IDENTITY}" \
    "${AUTHORITATIVE_DNS_DIG_HELPER_IDENTITY}" \
    "${AUTHORITATIVE_DNS_DIG_BUILDER_IDENTITY}" \
    "${AUTHORITATIVE_DNS_DIG_VERSION}" \
    "${AUTHORITATIVE_DNS_DIG_HELP_SHA256}" \
    "${AUTHORITATIVE_DNS_DIG_MODE}" \
    "${AUTHORITATIVE_DNS_DIG_UDP_WIRE_SHA256}" \
    "${AUTHORITATIVE_DNS_DIG_TCP_WIRE_SHA256}"
}

authoritative_dns_dig_require_attested() {
  local current_identity=""
  local current_helper_identity=""
  local current_builder_identity=""
  local current_attestation=""

  authoritative_dns_dig_state_complete || {
    authoritative_dns_dig_error "wire attestation state is absent or incomplete"
    return 1
  }
  case "${AUTHORITATIVE_DNS_DIG_MODE}" in
    modern) ;;
    legacy)
      [[ "${AUTHORITATIVE_DNS_DIG_VERSION}" == "DiG 9.10.6" ]] || {
        authoritative_dns_dig_error "legacy mode is not bound to exact DiG 9.10.6"
        return 1
      }
      ;;
    *)
      authoritative_dns_dig_error "wire attestation mode is invalid"
      return 1
      ;;
  esac
  current_identity="$(authoritative_dns_dig_file_identity "${AUTHORITATIVE_DNS_DIG_BIN}")" || return 1
  [[ "${current_identity}" == "${AUTHORITATIVE_DNS_DIG_IDENTITY}" ]] || {
    authoritative_dns_dig_error "pinned dig binary identity drifted"
    return 1
  }
  current_helper_identity="$(authoritative_dns_dig_file_identity "$(authoritative_dns_dig_helper_path)" false)" || return 1
  [[ "${current_helper_identity}" == "${AUTHORITATIVE_DNS_DIG_HELPER_IDENTITY}" ]] || {
    authoritative_dns_dig_error "authoritative DiG helper identity drifted"
    return 1
  }
  current_builder_identity="$(authoritative_dns_dig_builder_identity)" || return 1
  [[ "${current_builder_identity}" == "${AUTHORITATIVE_DNS_DIG_BUILDER_IDENTITY}" ]] || {
    authoritative_dns_dig_error "authoritative DiG argv builder drifted"
    return 1
  }
  current_attestation="$(authoritative_dns_dig_compute_attestation_sha256)" || return 1
  [[ "${current_attestation}" == "${AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256}" ]] || {
    authoritative_dns_dig_error "wire attestation envelope drifted"
    return 1
  }
}

authoritative_dns_dig_preflight() {
  local previous_attestation=""
  local binary=""
  local identity=""
  local helper_identity=""
  local builder_identity=""
  local version=""
  local help_text=""
  local help_sha256=""
  local mode=""
  local modern_error=""

  if authoritative_dns_dig_state_present; then
    authoritative_dns_dig_state_complete || {
      authoritative_dns_dig_error "refusing partial inherited DiG attestation state"
      return 1
    }
    previous_attestation="${AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256}"
  fi

  binary="$(authoritative_dns_dig_resolve_binary)" || return 1
  identity="$(authoritative_dns_dig_file_identity "${binary}")" || return 1
  helper_identity="$(authoritative_dns_dig_file_identity "$(authoritative_dns_dig_helper_path)" false)" || return 1
  builder_identity="$(authoritative_dns_dig_builder_identity)" || return 1
  version="$(authoritative_dns_dig_controlled_capture "${binary}" -v)" || {
    authoritative_dns_dig_error "cannot read DiG version in the controlled environment"
    return 1
  }
  [[ -n "${version}" && "${version}" != *$'\n'* && "${version}" != *$'\r'* ]] || {
    authoritative_dns_dig_error "DiG version output must be one non-empty line"
    return 1
  }
  help_text="$(authoritative_dns_dig_controlled_capture "${binary}" -h)" || {
    authoritative_dns_dig_error "cannot read DiG help in the controlled environment"
    return 1
  }
  help_sha256="$(printf '%s' "${help_text}" | authoritative_dns_dig_hash_stream)" || return 1
  [[ "$(authoritative_dns_dig_file_identity "${binary}")" == "${identity}" ]] || {
    authoritative_dns_dig_error "dig binary changed while reading version/help"
    return 1
  }

  authoritative_dns_dig_probe_candidate "${binary}" modern || return 1
  case "${AUTHORITATIVE_DNS_DIG_PROBE_RESULT}" in
    success)
      mode=modern
      ;;
    unsupported)
      modern_error="${AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR}"
      [[ "${version}" == "DiG 9.10.6" ]] || {
        authoritative_dns_dig_error "modern no-cookie query is unsupported and legacy fallback is restricted to exact DiG 9.10.6: ${modern_error}"
        return 1
      }
      if printf '%s\n' "${help_text}" | grep -Eiq '(^|[^[:alnum:]_])(cookie|sit)([^[:alnum:]_]|$)'; then
        authoritative_dns_dig_error "legacy DiG help declares cookie or sit support"
        return 1
      fi
      [[ "$(authoritative_dns_dig_file_identity "${binary}")" == "${identity}" ]] || {
        authoritative_dns_dig_error "dig binary changed after the modern capability probe"
        return 1
      }
      authoritative_dns_dig_probe_candidate "${binary}" legacy || return 1
      [[ "${AUTHORITATIVE_DNS_DIG_PROBE_RESULT}" == "success" ]] || {
        authoritative_dns_dig_error "legacy DiG raw-wire probe failed closed: ${AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR}"
        return 1
      }
      mode=legacy
      ;;
    *)
      authoritative_dns_dig_error "modern DiG raw-wire probe failed closed without legacy downgrade: ${AUTHORITATIVE_DNS_DIG_LAST_PROBE_ERROR}"
      return 1
      ;;
  esac

  [[ "$(authoritative_dns_dig_file_identity "${binary}")" == "${identity}" ]] || {
    authoritative_dns_dig_error "dig binary changed during wire attestation"
    return 1
  }
  [[ "$(authoritative_dns_dig_builder_identity)" == "${builder_identity}" ]] || {
    authoritative_dns_dig_error "DiG argv builder changed during wire attestation"
    return 1
  }

  AUTHORITATIVE_DNS_DIG_ATTESTED="${AUTHORITATIVE_DNS_DIG_WIRE_POLICY}"
  AUTHORITATIVE_DNS_DIG_BIN="${binary}"
  AUTHORITATIVE_DNS_DIG_IDENTITY="${identity}"
  AUTHORITATIVE_DNS_DIG_HELPER_IDENTITY="${helper_identity}"
  AUTHORITATIVE_DNS_DIG_BUILDER_IDENTITY="${builder_identity}"
  AUTHORITATIVE_DNS_DIG_VERSION="${version}"
  AUTHORITATIVE_DNS_DIG_HELP_SHA256="${help_sha256}"
  AUTHORITATIVE_DNS_DIG_MODE="${mode}"
  AUTHORITATIVE_DNS_DIG_UDP_WIRE_SHA256="${AUTHORITATIVE_DNS_DIG_PROBE_UDP_SHA256}"
  AUTHORITATIVE_DNS_DIG_TCP_WIRE_SHA256="${AUTHORITATIVE_DNS_DIG_PROBE_TCP_SHA256}"
  AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256="$(authoritative_dns_dig_compute_attestation_sha256)" || return 1
  export AUTHORITATIVE_DNS_DIG_ATTESTED AUTHORITATIVE_DNS_DIG_BIN AUTHORITATIVE_DNS_DIG_IDENTITY
  export AUTHORITATIVE_DNS_DIG_HELPER_IDENTITY AUTHORITATIVE_DNS_DIG_BUILDER_IDENTITY
  export AUTHORITATIVE_DNS_DIG_VERSION AUTHORITATIVE_DNS_DIG_HELP_SHA256 AUTHORITATIVE_DNS_DIG_MODE
  export AUTHORITATIVE_DNS_DIG_UDP_WIRE_SHA256 AUTHORITATIVE_DNS_DIG_TCP_WIRE_SHA256
  export AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256

  authoritative_dns_dig_require_attested || return 1
  if [[ -n "${previous_attestation}" &&
        "${AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256}" != "${previous_attestation}" ]]; then
    AUTHORITATIVE_DNS_DIG_ATTESTED=""
    authoritative_dns_dig_error "fresh wire attestation differs from the inherited pre-mutation identity/mode"
    return 1
  fi
  printf '[authoritative-dns-dig] attested mode=%s binary=%s version=%s\n' \
    "${AUTHORITATIVE_DNS_DIG_MODE}" "${AUTHORITATIVE_DNS_DIG_BIN}" "${AUTHORITATIVE_DNS_DIG_VERSION}"
}

authoritative_dns_dig_attestation_json() {
  authoritative_dns_dig_require_attested || return 1
  python3 - \
    "${AUTHORITATIVE_DNS_DIG_WIRE_POLICY}" \
    "${AUTHORITATIVE_DNS_DIG_BIN}" \
    "${AUTHORITATIVE_DNS_DIG_IDENTITY}" \
    "${AUTHORITATIVE_DNS_DIG_VERSION}" \
    "${AUTHORITATIVE_DNS_DIG_MODE}" \
    "${AUTHORITATIVE_DNS_DIG_UDP_WIRE_SHA256}" \
    "${AUTHORITATIVE_DNS_DIG_TCP_WIRE_SHA256}" \
    "${AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256}" <<'PY'
import json
import sys
schema, binary, identity, version, mode, udp, tcp, digest = sys.argv[1:]
print(json.dumps({
    "attestation_sha256": digest,
    "binary": binary,
    "binary_identity": json.loads(identity),
    "mode": mode,
    "schema": schema,
    "tcp_wire_sha256": tcp,
    "udp_wire_sha256": udp,
    "version": version,
}, sort_keys=True, separators=(",", ":")))
PY
}

authoritative_dns_dig_execute_argv_to_file() {
  local output_file="$1"
  shift
  local empty_home=""
  local argument=""
  local query_timeout_seconds=""
  local query_timeout_seen="false"
  local process_timeout_seconds=""
  local rc=0

  for argument in "$@"; do
    case "${argument}" in
      +time=*)
        if [[ "${query_timeout_seen}" == "true" ]]; then
          authoritative_dns_dig_error "query argv must contain exactly one +time value"
          return 1
        fi
        query_timeout_seen="true"
        query_timeout_seconds="${argument#+time=}"
        ;;
    esac
  done
  [[ "${query_timeout_seen}" == "true" ]] || {
    authoritative_dns_dig_error "query argv must contain exactly one +time value"
    return 1
  }
  authoritative_dns_dig_validate_timeout_seconds "${query_timeout_seconds}" || return 1
  process_timeout_seconds=$((query_timeout_seconds + 2))
  if (( process_timeout_seconds < 5 )); then
    process_timeout_seconds=5
  fi

  empty_home="$(mktemp -d "${TMPDIR:-/tmp}/fugue-dig-home.XXXXXX")" || return 1
  chmod 700 "${empty_home}" || {
    rm -rf "${empty_home}"
    return 1
  }
  python3 - "${empty_home}" "${output_file}" "${process_timeout_seconds}" "$@" <<'PY' || rc=$?
import os
import subprocess
import sys

home = sys.argv[1]
output_file = sys.argv[2]
timeout_seconds = int(sys.argv[3])
argv = sys.argv[4:]
environment = {
    "HOME": home,
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
}
with open(output_file, "wb") as output:
    try:
        completed = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=environment,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(124)
raise SystemExit(completed.returncode)
PY
  if [[ -n "$(find "${empty_home}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    rc=125
  fi
  rm -rf "${empty_home}"
  return "${rc}"
}

authoritative_dns_dig_query() {
  local output_file="$1"
  local transport="$2"
  local server="$3"
  local port="$4"
  local timeout_seconds="$5"
  local query_name="$6"
  local query_type="$7"
  local rc=0

  authoritative_dns_dig_require_attested || return 1
  authoritative_dns_dig_build_query_argv \
    "${AUTHORITATIVE_DNS_DIG_BIN}" "${AUTHORITATIVE_DNS_DIG_MODE}" \
    "${server}" "${port}" "${timeout_seconds}" "${transport}" "${query_name}" "${query_type}" || return 1
  authoritative_dns_dig_execute_argv_to_file "${output_file}" \
    "${AUTHORITATIVE_DNS_DIG_QUERY_ARGV[@]}" || rc=$?
  authoritative_dns_dig_require_attested || return 1
  return "${rc}"
}
