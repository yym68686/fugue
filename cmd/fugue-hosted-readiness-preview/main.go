// Offline, non-authorizing preview of the requirements shared by a path edit.
// No client credentials, remote fetch, signing or publication operation.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"fugue/internal/platformconfig"
)

type inputSide struct {
	Plan   *platformconfig.DNSReadinessPlan
	Policy *platformconfig.DNSReadinessPolicy
	Paths  []string
}
type input struct {
	Host          string
	Base, Desired inputSide
}

func preview(r io.Reader, w io.Writer) error {
	const limit = 32 << 20
	b, e := io.ReadAll(io.LimitReader(r, limit+1))
	if e != nil || len(b) > limit {
		return errors.New("incomplete or oversized input")
	}
	var in input
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil || d.Decode(new(any)) != io.EOF || in.Base.Policy == nil || in.Desired.Policy == nil || *in.Base.Policy != *in.Desired.Policy {
		return errors.New("invalid pair or changed readiness policy")
	}
	view, p, e := platformconfig.ComposeHostedReadinessRequirements(in.Host, in.Base.Plan, in.Desired.Plan, in.Base.Policy, in.Base.Paths, in.Desired.Paths)
	if e != nil {
		return e
	}
	restored, back, e := platformconfig.ComposeHostedReadinessRequirements(in.Host, view, in.Base.Plan, in.Base.Policy, in.Desired.Paths, in.Base.Paths)
	if e != nil {
		return e
	}
	viewDigest, e := platformconfig.Digest(view)
	if e != nil {
		return e
	}
	restoredDigest, e := platformconfig.Digest(restored)
	if e != nil {
		return e
	}
	if back.BasePlanDigest != viewDigest || back.DesiredPlanDigest != p.BasePlanDigest || restoredDigest != p.BasePlanDigest || len(back.Edits) != len(p.Edits) {
		return errors.New("inverse returned a different dependency set")
	}
	owners := map[string]bool{}
	for i, f := range p.Edits {
		v := back.Edits[i]
		if f.Record != v.Record || f.Edge != v.Edge || f.Address != v.Address || !slices.Equal(f.Before, v.After) || !slices.Equal(f.After, v.Before) {
			return errors.New("dependency inverse is not exact")
		}
		owners[f.Record] = true
	}
	return json.NewEncoder(w).Encode(map[string]any{"schema": "hosted-readiness-delta-preview/v2", "base_plan_digest": p.BasePlanDigest, "desired_plan_digest": p.DesiredPlanDigest, "composed_plan_digest": viewDigest, "restored_plan_digest": restoredDigest, "target_edits": len(p.Edits), "affected_records": len(owners), "inverse_exact": true, "authorizes_traffic": false})
}

func main() {
	if err := preview(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
