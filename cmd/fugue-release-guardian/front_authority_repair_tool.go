package main

import (
	"bytes"
	"debug/elf"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	// The retained Worker has a separately bounded 4 MiB diagnostic scratch
	// EmptyDir. Leave room for its own files; never write the activation mount
	// or overwrite /usr/local/bin while delivering a temporary tool.
	frontRepairToolMaxBytes   = 3 << 20
	frontRepairScratchMinimum = 4 << 20
)

type frontRepairToolRecord struct {
	SourceCommit string `json:"sourceCommit"`
	Architecture string `json:"architecture"`
	BinaryDigest string `json:"binaryDigest"`
	BinaryBytes  int    `json:"binaryBytes"`
}

type frontRepairTool struct {
	record frontRepairToolRecord
	code   []byte
}

// Validating bytes against an authenticated artifact record is not approval.
// The protected launcher must separately verify the recorded build/run source
// and bind this artifact digest to the reviewed repair declaration.
func loadFrontRepairTool(record frontRepairToolRecord, code []byte) (frontRepairTool, error) {
	machine, supported := map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}[record.Architecture]
	if !supported || !exactSourceSHA(record.SourceCommit) || !exactSHA256Digest(record.BinaryDigest) ||
		record.BinaryBytes < 1 || record.BinaryBytes > frontRepairToolMaxBytes || len(code) != record.BinaryBytes ||
		shaDigest(code) != record.BinaryDigest {
		return frontRepairTool{}, errors.New("Front repair node-tool bytes do not match the bounded build record")
	}
	binary, err := elf.NewFile(bytes.NewReader(code))
	if err != nil {
		return frontRepairTool{}, errors.New("Front repair node tool is not an ELF executable")
	}
	defer binary.Close()
	if binary.Type != elf.ET_EXEC || binary.Machine != machine || binary.Class != elf.ELFCLASS64 ||
		binary.Data != elf.ELFDATA2LSB || binary.Entry == 0 {
		return frontRepairTool{}, errors.New("Front repair node tool architecture or executable format does not match")
	}
	for _, segment := range binary.Progs {
		if segment.Type == elf.PT_INTERP || segment.Type == elf.PT_DYNAMIC {
			return frontRepairTool{}, errors.New("Front repair node tool must be self-contained")
		}
	}
	return frontRepairTool{record: record, code: bytes.Clone(code)}, nil
}

func frontRepairToolScratchAvailable(worker corev1.Pod) bool {
	for _, c := range worker.Spec.Containers {
		if c.Name != "edge" || c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil ||
			!*c.SecurityContext.ReadOnlyRootFilesystem || c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
			continue
		}
		for _, mount := range c.VolumeMounts {
			if mount.MountPath != "/tmp" || mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" {
				continue
			}
			for _, volume := range worker.Spec.Volumes {
				if volume.Name == mount.Name && volume.EmptyDir != nil && volume.EmptyDir.Medium == corev1.StorageMediumDefault &&
					volume.EmptyDir.SizeLimit != nil && volume.EmptyDir.SizeLimit.Value() >= frontRepairScratchMinimum {
					return true
				}
			}
		}
	}
	return false
}

func (tool frontRepairTool) command(flag, request string) ([]string, error) {
	if tool.record.BinaryBytes == 0 || tool.record.BinaryBytes != len(tool.code) ||
		(flag != "--repair-request" && flag != "--repair-prepare-request") || len(request) == 0 || len(request) > 16<<10 ||
		strings.ContainsAny(request, "\r\n\t =") {
		return nil, errors.New("Front repair temporary node-tool command is invalid")
	}
	if _, err := base64.RawURLEncoding.Strict().DecodeString(request); err != nil {
		return nil, fmt.Errorf("Front repair temporary node-tool encoding: %w", err)
	}
	return []string{"/bin/sh", "-ec", frontRepairToolLoader, "fugue-front-repair-tool",
		strings.TrimPrefix(tool.record.BinaryDigest, "sha256:"), strconv.Itoa(tool.record.BinaryBytes), flag, request}, nil
}

// This fixed script receives no script text, path, environment or commands
// from the declaration. The tool is opened and unlinked BEFORE exec; an
// interrupted apply can't leave an installed command on a retained Worker.
const frontRepairToolLoader = `
want="$1"
size="$2"
mode="$3"
payload="$4"
case "$want" in ''|*[!0-9a-f]*) exit 65;; esac
[ "${#want}" -eq 64 ]
case "$size" in ''|*[!0-9]*) exit 65;; esac
[ "$size" -gt 0 ]
[ "$size" -le 3145728 ]
case "$mode" in --repair-request|--repair-prepare-request) :;; *) exit 65;; esac
umask 077
# Keep the existing diagnostic data well below the EmptyDir limit. Retain
# 256 KiB headroom for the live diagnostic socket and changes after sampling.
used="$(du -sk /tmp)"
set -- $used
case "$1" in ''|*[!0-9]*) exit 65;; esac
[ "$(( $1 * 1024 + size + 262144 ))" -le 4194304 ]
work="$(mktemp -d /tmp/.fugue-front-repair.XXXXXXXX)"
cleanup() { rm -f -- "$work/tool"; rmdir -- "$work"; }
trap cleanup EXIT
trap 'exit 75' HUP INT TERM
head -c "$((size+1))" > "$work/tool"
[ "$(wc -c < "$work/tool")" -eq "$size" ]
actual="$(sha256sum "$work/tool")"
[ "${actual%% *}" = "$want" ]
chmod 0500 "$work/tool"
exec 9< "$work/tool"
cleanup
trap - EXIT HUP INT TERM
exec /proc/self/fd/9 --state-file /var/lib/fugue-edge-front/activation.json "$mode" "$payload"
`
