#!/bin/sh
# /controller is scratch storage shared with WAL helpers and online upgrades.
# A symlink exposes the image's manager without copying it or masking the image
# path with a volume. CNPG's verified atomic rename can later replace this link
# with an upgraded binary; container restarts must retain that upgraded file.
set -eu
manager=/controller/manager
baked=/usr/local/libexec/cnpg-manager
if [ ! -e "$manager" ] && [ ! -L "$manager" ]; then
    ln -s "$baked" "$manager"
fi
if [ -L "$manager" ] && [ "$(readlink "$manager")" != "$baked" ]; then
    echo 'refusing unexpected instance-manager symlink' >&2
    exit 1
fi
if [ ! -f "$manager" ] || [ ! -x "$manager" ]; then
    echo 'instance manager is missing or is not an executable regular file' >&2
    exit 1
fi
exec "$manager" "$@"
