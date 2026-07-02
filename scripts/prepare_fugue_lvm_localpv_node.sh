#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: prepare_fugue_lvm_localpv_node.sh --size-gib <gib> --node-role storage-agent [--vg-name fugue-vg] [--image-path /var/lib/fugue/lvm-localpv/fugue-vg.img] [--dry-run]

Creates an LVM volume group for Fugue managed Postgres LocalPV storage on the
current node. By default this uses a reserved loopback file, so it does not
modify existing disk partition tables or existing Kubernetes PVC data.

LocalPV preparation is opt-in. Edge-only, DNS-only, and control-plane-only
nodes are refused unless --allow-localpv is passed explicitly for a maintenance
exception.
EOF
}

SIZE_GIB=""
VG_NAME="fugue-vg"
IMAGE_PATH=""
ALLOW_LOCALPV="${FUGUE_ALLOW_LOCALPV_PREALLOCATION:-false}"
DRY_RUN="false"
NODE_ROLES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --size-gib)
      SIZE_GIB="${2:-}"
      shift 2
      ;;
    --vg-name)
      VG_NAME="${2:-}"
      shift 2
      ;;
    --image-path)
      IMAGE_PATH="${2:-}"
      shift 2
      ;;
    --node-role)
      NODE_ROLES+=("${2:-}")
      shift 2
      ;;
    --allow-localpv)
      ALLOW_LOCALPV="true"
      shift
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${SIZE_GIB}" || ! "${SIZE_GIB}" =~ ^[0-9]+$ || "${SIZE_GIB}" -le 0 ]]; then
  echo "--size-gib must be a positive integer" >&2
  exit 2
fi

if [[ -z "${VG_NAME}" ]]; then
  echo "--vg-name is required" >&2
  exit 2
fi

if [[ -n "${FUGUE_NODE_ROLES:-}" ]]; then
  IFS=',' read -r -a env_roles <<<"${FUGUE_NODE_ROLES}"
  NODE_ROLES+=("${env_roles[@]}")
fi

ROLE_TEXT=","
for role in "${NODE_ROLES[@]}"; do
  role="$(printf '%s' "${role}" | tr '[:upper:]' '[:lower:]' | xargs)"
  [[ -n "${role}" ]] || continue
  ROLE_TEXT+="${role},"
done

localpv_role_allowed=false
case "${ROLE_TEXT}" in
  *",storage,"*|*",storage-agent,"*|*",localpv,"*|*",localpv-storage,"*)
    localpv_role_allowed=true
    ;;
esac

localpv_role_blocked=false
case "${ROLE_TEXT}" in
  *",edge,"*|*",edge-only,"*|*",dns,"*|*",dns-only,"*|*",control-plane,"*|*",control-plane-only,"*)
    localpv_role_blocked=true
    ;;
esac

if [[ "${ALLOW_LOCALPV}" != "true" ]]; then
  if [[ "${localpv_role_blocked}" == "true" ]]; then
    echo "LocalPV preallocation is disabled for edge, DNS, and control-plane-only roles; pass --allow-localpv only for an explicit maintenance exception" >&2
    exit 2
  fi
  if [[ "${localpv_role_allowed}" != "true" ]]; then
    echo "LocalPV preallocation is opt-in; pass --node-role storage-agent or --allow-localpv" >&2
    exit 2
  fi
fi

if [[ -z "${IMAGE_PATH}" ]]; then
  IMAGE_PATH="/var/lib/fugue/lvm-localpv/${VG_NAME}.img"
fi

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "LocalPV preallocation dry-run: vg=${VG_NAME} image=${IMAGE_PATH} size_gib=${SIZE_GIB} roles=${ROLE_TEXT#,} allow_localpv=${ALLOW_LOCALPV}"
  exit 0
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

if ! command -v pvcreate >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y lvm2
  else
    echo "lvm2 is required and no supported package manager was found" >&2
    exit 1
  fi
fi

IMAGE_DIR="$(dirname "${IMAGE_PATH}")"
mkdir -p "${IMAGE_DIR}"

if ! vgs "${VG_NAME}" >/dev/null 2>&1; then
  if [[ ! -f "${IMAGE_PATH}" ]]; then
    fallocate -l "${SIZE_GIB}G" "${IMAGE_PATH}"
    chmod 600 "${IMAGE_PATH}"
  fi

  LOOP_DEVICE="$(losetup -j "${IMAGE_PATH}" | awk -F: 'NR==1{print $1}')"
  if [[ -z "${LOOP_DEVICE}" ]]; then
    LOOP_DEVICE="$(losetup --find --show "${IMAGE_PATH}")"
  fi

  if ! pvs "${LOOP_DEVICE}" >/dev/null 2>&1; then
    pvcreate -ff -y "${LOOP_DEVICE}"
  fi
  vgcreate "${VG_NAME}" "${LOOP_DEVICE}"
fi

SERVICE_PATH="/etc/systemd/system/fugue-lvm-localpv-loop.service"
cat > "${SERVICE_PATH}" <<EOF
[Unit]
Description=Attach Fugue LVM LocalPV loopback volume group
DefaultDependencies=no
After=local-fs.target
Before=k3s.service k3s-agent.service kubelet.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -ec 'losetup -j ${IMAGE_PATH} >/dev/null || losetup --find --show ${IMAGE_PATH} >/dev/null; vgchange -ay ${VG_NAME}'
ExecStop=/bin/sh -ec 'vgchange -an ${VG_NAME} || true'

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now fugue-lvm-localpv-loop.service
vgs --noheadings -o vg_name,vg_size,vg_free "${VG_NAME}"
