package api

import (
	"context"
	"fugue/internal/model"
	corev1 "k8s.io/api/core/v1"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func (s *Server) enrichLocalPVBindings(ctx context.Context, in *model.LocalPVInventory) {
	in.BoundPVCount = -1
	in.BoundPVCountKnown = false
	in.BoundPVCRefs = nil
	client, err := s.requireClusterNodeClient()
	if err != nil {
		in.UnsafeReasons = uniqueNonEmptyStrings(append(in.UnsafeReasons, "bound_pv_count_unknown"))
		return
	}
	defer client.closeIdleConnections()
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var node corev1.Node
	var pvs corev1.PersistentVolumeList
	if strings.TrimSpace(in.ClusterNodeName) == "" {
		return
	}
	if err = client.doJSON(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(in.ClusterNodeName), &node); err != nil {
		return
	}
	if err = client.doJSON(ctx, http.MethodGet, "/api/v1/persistentvolumes", &pvs); err != nil {
		return
	}
	refs := []string{}
	count := 0
	for _, pv := range pvs.Items {
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != "local.csi.openebs.io" || pv.Status.Phase != corev1.VolumeBound {
			continue
		}
		attrs := pv.Spec.CSI.VolumeAttributes
		vg := firstNonEmptyImageAPIString(attrs["openebs.io/volgroup"], attrs["volgroup"], attrs["vgname"])
		if vg != "" && vg != in.VGName {
			continue
		}
		// An absent or incompletely understood affinity cannot prove exclusion.
		// Match host identity even if the corresponding LV is absent: otherwise a
		// failed LVM enumeration could turn a bound claim into a false zero.
		if !localPVPossiblyTargetsNode(pv, node) {
			continue
		}
		count++
		if pv.Spec.ClaimRef != nil {
			refs = append(refs, pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name)
		} else {
			refs = append(refs, pv.Name)
		}
	}
	in.BoundPVCount = count
	in.BoundPVCountKnown = true
	in.BoundPVCRefs = uniqueNonEmptyStrings(refs)
	reasons := in.UnsafeReasons[:0]
	for _, reason := range in.UnsafeReasons {
		switch reason {
		case "kubectl_pv_unavailable", "bound_pv_count_unknown", "bound_pvs_present":
		default:
			reasons = append(reasons, reason)
		}
	}
	in.UnsafeReasons = reasons
}

func localPVPossiblyTargetsNode(pv corev1.PersistentVolume, node corev1.Node) bool {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return true
	}
	terms := pv.Spec.NodeAffinity.Required.NodeSelectorTerms
	if len(terms) == 0 {
		return true
	}
	for _, term := range terms {
		excluded := false
		for _, expr := range append(append([]corev1.NodeSelectorRequirement{}, term.MatchExpressions...), term.MatchFields...) {
			value, known := node.Labels[expr.Key]
			if expr.Key == "metadata.name" {
				value = node.Name
				known = true
			}
			// Providers also use openebs.io/nodename without setting that node label.
			if expr.Key == "openebs.io/nodename" {
				value = node.Name
				known = true
			}
			present := false
			for _, v := range expr.Values {
				if v == value {
					present = true
				}
			}
			switch expr.Operator {
			case corev1.NodeSelectorOpIn:
				if known && !present {
					excluded = true
				}
			case corev1.NodeSelectorOpNotIn:
				if known && present {
					excluded = true
				}
				// Unknown labels/operators conservatively retain the claim.
			}
		}
		if !excluded {
			return true
		}
	}
	return false
}
