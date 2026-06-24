#!/usr/bin/env bash
set -euo pipefail

ssh_user="${FUGUE_SSH_USER:-root}"
workspace="${FUGUE_WORKSPACE:-/workspace}"
if [ -n "${FUGUE_SSH_AUTHORIZED_KEYS:-}" ]; then
  authorized_keys="${FUGUE_SSH_AUTHORIZED_KEYS}"
elif [ "${ssh_user}" = "root" ]; then
  authorized_keys="/root/.ssh/authorized_keys"
else
  authorized_keys="/home/${ssh_user}/.ssh/authorized_keys"
fi
host_keys_dir="${FUGUE_SSH_HOST_KEYS_DIR:-/etc/ssh/fugue-host-keys}"

if ! id "${ssh_user}" >/dev/null 2>&1; then
  echo "ssh user ${ssh_user} does not exist" >&2
  exit 1
fi

mkdir -p /run/sshd "${workspace}" "$(dirname "${authorized_keys}")" "${host_keys_dir}" /etc/ssh/sshd_config.d
chown "${ssh_user}:${ssh_user}" "${workspace}" "$(dirname "${authorized_keys}")"
chmod 700 "$(dirname "${authorized_keys}")"
if [ ! -f "${authorized_keys}" ]; then
  touch "${authorized_keys}"
fi
chown "${ssh_user}:${ssh_user}" "${authorized_keys}" || true
chmod 600 "${authorized_keys}" || true

for key_type in rsa ecdsa ed25519; do
  key_path="${host_keys_dir}/ssh_host_${key_type}_key"
  if [ ! -f "${key_path}" ]; then
    ssh-keygen -q -N "" -t "${key_type}" -f "${key_path}"
  fi
done
chmod 600 "${host_keys_dir}"/ssh_host_*_key

cat >/etc/ssh/sshd_config.d/fugue.conf <<EOF
Port 22
PasswordAuthentication no
PermitRootLogin prohibit-password
PubkeyAuthentication yes
AuthorizedKeysFile ${authorized_keys}
AllowTcpForwarding no
X11Forwarding no
PermitTunnel no
ChallengeResponseAuthentication no
UsePAM no
HostKey ${host_keys_dir}/ssh_host_rsa_key
HostKey ${host_keys_dir}/ssh_host_ecdsa_key
HostKey ${host_keys_dir}/ssh_host_ed25519_key
EOF

/usr/sbin/sshd -D -e &
sshd_pid="$!"

if [ "$#" -eq 0 ]; then
  wait "${sshd_pid}"
  exit "$?"
fi

"$@" &
app_pid="$!"

set +e
wait -n "${sshd_pid}" "${app_pid}"
status="$?"
kill "${sshd_pid}" "${app_pid}" >/dev/null 2>&1 || true
wait "${sshd_pid}" "${app_pid}" >/dev/null 2>&1 || true
exit "${status}"
