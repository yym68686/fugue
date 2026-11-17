package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Historical sources never recreate a PVC or PV to make old data look bound.
// A Released PV keeps its original claim identity; a directory without a PV
// gets a separate retirement identity and must remain unregistered throughout.
func planSelectedVolumeRetirement(ctx context.Context, kube physicalKubeFunc, host volumeHostFunc, opts volumeRetirementOptions, args []string) (retirementPreview, error) {
	var empty retirementPreview
	selections := len(args)
	if opts.releasedPV != "" {
		selections++
	}
	if opts.directory != "" {
		selections++
	}
	if selections != 1 || opts.directory == "" && (opts.formerPV != "" || opts.node != "") {
		return empty, fmt.Errorf("select exactly one PVC, --released-pv or --orphan-directory; --node and --former-pv apply only to orphan directories")
	}
	if len(args) == 1 {
		if opts.databaseCluster != "" {
			return empty, fmt.Errorf("bound PVC database ownership is read from its labels")
		}
		return planVolumeRetirement(ctx, kube, host, opts, args[0])
	}
	p := retirementPlan{Schema: volumeRetirementSchema, ObservedAt: time.Now().UTC(), Namespace: opts.namespace, StorageRoot: opts.storageRoot, DatabaseCluster: opts.databaseCluster, AllowTerminalJobs: opts.allowTerminalJobs}
	if opts.releasedPV != "" {
		pv, err := kube(ctx, nil, "get", "pv", opts.releasedPV, "-o", "json")
		if err != nil {
			return empty, err
		}
		spec := physicalMap(pv, "spec")
		claim := physicalMap(spec, "claimRef")
		p.Source, p.PV = "released-pv", opts.releasedPV
		p.PVUID = physicalString(physicalMap(pv, "metadata"), "uid")
		p.PVC, p.PVCUID = physicalString(claim, "name"), physicalString(claim, "uid")
		p.Path = physicalString(physicalMap(spec, "local"), "path")
		p.Node = retirementVolumeNode(pv)
		if p.PVUID == "" || p.PVC == "" || p.PVCUID == "" || p.Node == "" || physicalString(claim, "namespace") != p.Namespace || physicalString(spec, "persistentVolumeReclaimPolicy") != "Retain" || physicalString(physicalMap(pv, "status"), "phase") != "Released" {
			return empty, fmt.Errorf("an identified Released local Retain PV with an original claim in --namespace is required")
		}
	} else {
		if opts.formerPV == "" || opts.node == "" {
			return empty, fmt.Errorf("--orphan-directory requires an exact --former-pv and --node")
		}
		p.Source, p.Path, p.PV, p.Node = "orphan-directory", opts.directory, opts.formerPV, opts.node
	}
	return inspectVolumeRetirementPlan(ctx, kube, host, p)
}

func retirementIdentity(p retirementPlan) string {
	if p.Source == "orphan-directory" {
		return p.RetirementID
	}
	return p.PVUID
}

func retirementSourceValid(p retirementPlan) bool {
	if p.NodeUID == "" || p.KubeSystemUID == "" || p.PV == "" {
		return false
	}
	switch p.Source {
	case "", "released-pv":
		return p.PVUID != "" && p.PVCUID != "" && p.PVC != "" && p.RetirementID == ""
	case "orphan-directory":
		digest := sha256.Sum256([]byte(p.KubeSystemUID + "\x00" + p.NodeUID + "\x00" + p.Path))
		return p.PVUID == "" && p.PVCUID == "" && p.PVC == "" && p.RetirementID == "orphan-"+hex.EncodeToString(digest[:16])
	default:
		return false
	}
}
