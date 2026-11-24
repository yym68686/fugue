package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	st "fugue/internal/edgegroupfront"
)

var nativePodUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var nativeNode = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]$|^[a-z0-9]$`)
var nativeGroup = regexp.MustCompile(`^(edge-group-|cell-)[a-z0-9]+(-[a-z0-9]+)*$`)
var nativePIDNS = regexp.MustCompile(`^pid:\[[0-9]+\]$`)

// No host namespace or runtime socket. Pin the current container's own
// PID-1 birth and PID namespace so a restarted container in the same Pod
// cannot consume a request issued to the earlier incarnation.
func localWorkerRecipient() (st.NativeRecipient, error) {
	id := st.NativeRecipient{PodUID: os.Getenv("FUGUE_EDGE_FRONT_POD_UID"), Node: os.Getenv("FUGUE_EDGE_FRONT_EDGE_ID"), Group: os.Getenv("FUGUE_EDGE_FRONT_EDGE_GROUP_ID")}
	if !nativePodUID.MatchString(id.PodUID) || !nativeNode.MatchString(id.Node) || !nativeGroup.MatchString(id.Group) ||
		os.Getenv("FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE") != st.NativeStatePath {
		return st.NativeRecipient{}, errors.New("unbound ordinary Worker recipient")
	}
	ns, err := os.Readlink("/proc/self/ns/pid")
	if err != nil || !nativePIDNS.MatchString(ns) {
		return st.NativeRecipient{}, errors.New("unbound recipient namespace")
	}
	f, err := os.Open("/proc/1/stat")
	if err != nil {
		return st.NativeRecipient{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	// Field 22 follows PID and the parenthesized comm, which may contain spaces.
	i := bytes.LastIndex(raw, []byte(") "))
	if err != nil || len(raw) > 4096 || !bytes.HasPrefix(raw, []byte("1 (")) || i < 3 {
		return st.NativeRecipient{}, errors.New("unbound recipient PID")
	}
	fields := strings.Fields(string(raw[i+2:]))
	if len(fields) <= 19 {
		return st.NativeRecipient{}, errors.New("incomplete recipient PID")
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 {
		return st.NativeRecipient{}, errors.New("invalid recipient birth")
	}
	sum := sha256.Sum256([]byte(id.PodUID + "\n" + ns + "\n" + strconv.FormatUint(ticks, 10)))
	id.Incarnation = "sha256:" + hex.EncodeToString(sum[:])
	return id, nil
}

func runNativeWorker(args []string, out io.Writer, identify func() (st.NativeRecipient, error)) error {
	return runNativeWorkerAt(args, out, identify, st.NativeJournalPath, st.NativeStatePath)
}

func runNativeNodeWorker(args []string, out io.Writer, identify func() (st.NativeRecipient, error), cfg st.Config, path string) error {
	if len(args) != 2 || args[0] != "--node-worker-advance" || len(args[1]) > 16<<10 {
		return errors.New("invalid node Worker command")
	}
	id, err := identify()
	if err != nil {
		return err
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(args[1])
	var w st.NodeWorkerWrite
	if err != nil || json.Unmarshal(raw, &w) != nil {
		return errors.New("invalid node Worker envelope")
	}
	wire, err := json.Marshal(w)
	if err != nil || !bytes.Equal(wire, raw) || base64.RawURLEncoding.EncodeToString(raw) != args[1] || id != w.Recipient {
		return errors.New("node Worker recipient or envelope mismatch")
	}
	journal, err := readNativeProjection(path)
	if err != nil {
		return err
	}
	receipt, err := st.ExecuteNodeWorkerWrite(cfg, id, journal, w)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(receipt)
}

// File bindings are fixed by the compiled entrypoint, never supplied in the
// command or envelope. Isolated file-backed tests use their own directory.
func runNativeWorkerAt(args []string, out io.Writer, identify func() (st.NativeRecipient, error), journalPath, statePath string) error {
	if len(args) < 1 || out == nil || identify == nil {
		return errors.New("missing ordinary Worker command")
	}
	id, err := identify()
	if err != nil {
		return err
	}
	if len(args) == 1 && args[0] == "--worker-recipient" {
		return json.NewEncoder(out).Encode(id)
	}
	if len(args) == 1 && args[0] == "--worker-journal" {
		raw, err := readNativeProjection(journalPath)
		if err != nil {
			return err
		}
		digest, err := st.ProjectedJournalDigest(raw)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(st.NativeProjection{Recipient: id, Digest: digest})
	}
	if len(args) != 2 || args[0] != "--worker-convergence" || len(args[1]) > 16<<10 {
		return errors.New("invalid ordinary Worker command")
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(args[1])
	var w st.NativeWrite
	if err != nil || json.Unmarshal(raw, &w) != nil {
		return errors.New("invalid ordinary Worker envelope")
	}
	canonical, err := json.Marshal(w)
	if err != nil || !bytes.Equal(canonical, raw) || base64.RawURLEncoding.EncodeToString(raw) != args[1] ||
		w.Schema != st.NativeWriteSchema || w.Write.Schema != st.ActivationRepairWriteSchemaV1 ||
		id != w.Recipient || w.Write.Request.Expected.GroupID != id.Group {
		return errors.New("ordinary Worker recipient or journal envelope mismatch")
	}
	var receipt any
	// There is no command-line file/key override. Even an otherwise exact
	// envelope cannot perform a write until the actual ordinary Guardian
	// journal is projected read-only into this container.
	journal, err := readNativeProjection(journalPath)
	if err != nil {
		return err
	}
	if err := st.CheckNativeJournal(journal, w); err != nil {
		return err
	}
	switch w.Operation {
	case "prepare":
		receipt, err = st.PrepareActivationRepairPublication(statePath, w.Write.Request, w.Write.ExpiresAt)
	case "commit":
		receipt, err = st.ApplyActivationRepairCAS(statePath, w.Write.Request, w.Write.ExpiresAt)
	default:
		return errors.New("unknown ordinary Worker operation")
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(receipt)
}

func readNativeProjection(path string) ([]byte, error) {
	// Open the installed kubelet symlink, then inspect the actual opened file.
	// The mount and path are fixed at build time for the production entry.
	j, err := os.Open(path)
	if err != nil {
		return nil, errors.New("ordinary Worker journal is unavailable")
	}
	defer j.Close()
	stat, err := j.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < 1 || stat.Size() > 256<<10 {
		return nil, errors.New("ordinary Worker journal is not a bounded regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(j, (256<<10)+1))
	if err != nil || int64(len(raw)) != stat.Size() {
		return nil, errors.New("ordinary Worker journal is incomplete")
	}
	return raw, nil
}
