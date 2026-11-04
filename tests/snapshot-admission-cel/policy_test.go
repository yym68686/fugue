package snapshotadmission

import (
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	kcel "k8s.io/apiserver/pkg/cel"
)

// Compile the generated rule with Kubernetes' structural declarations and
// evaluate it with VAP's dynamic objects. These have different null presence
// semantics; Python state-machine tests cannot verify either CEL contract.
func TestGeneratedSnapshotPolicy(t *testing.T) {
	command := exec.Command("python3", "-c", `
import json
from scripts.recover_longhorn_snapshot import barrier_objects
c=dict(volume='data-volume',namespace='platform',primary='db-1',snapshotUID='snapshot-uid')
r=dict(snapshot_chain={'old-point':{}})
print(json.dumps({k:barrier_objects(c,r,'operation-example',**kw)[0]['spec']['validations'][0]['expression'] for k,kw in [('current',{}),('prior',{'typed_presence_snapshot_spec':True})]}))
`)
	command.Dir = "../.."
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var expressions map[string]string
	if err := json.Unmarshal(output, &expressions); err != nil {
		t.Fatal(err)
	}
	f := func(n string, d *kcel.DeclType) *kcel.DeclField { return kcel.NewDeclField(n, d, false, nil, nil) }
	spec := kcel.NewObjectType("SnapshotSpec", map[string]*kcel.DeclField{
		"volume": f("volume", kcel.StringType), "createSnapshot": f("createSnapshot", kcel.BoolType),
		"labels": f("labels", kcel.NewMapType(kcel.StringType, kcel.StringType, -1)),
	})
	meta := kcel.NewObjectType("SnapshotMetadata", map[string]*kcel.DeclField{"uid": f("uid", kcel.StringType)})
	obj := kcel.NewObjectType("Snapshot", map[string]*kcel.DeclField{"spec": f("spec", spec), "metadata": f("metadata", meta)})
	base, err := cel.NewEnv()
	if err != nil {
		t.Fatal(err)
	}
	opts, err := kcel.NewDeclTypeProvider(obj).EnvOptions(base.CELTypeProvider())
	if err != nil {
		t.Fatal(err)
	}
	typed, err := base.Extend(append(opts, cel.Variable("object", obj.CelType()), cel.Variable("oldObject", obj.CelType()), cel.Variable("request", cel.DynType))...)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := cel.NewEnv(cel.Variable("object", cel.DynType), cel.Variable("oldObject", cel.DynType), cel.Variable("request", cel.DynType))
	if err != nil {
		t.Fatal(err)
	}
	object := func(extra map[string]any, uid string) map[string]any {
		s := map[string]any{"volume": "data-volume", "createSnapshot": true}
		for k, v := range extra {
			s[k] = v
		}
		return map[string]any{"spec": s, "metadata": map[string]any{"uid": uid}}
	}
	for name, expr := range expressions {
		t.Run(name, func(t *testing.T) {
			if _, issues := typed.Compile(expr); issues.Err() != nil {
				t.Fatal(issues.Err())
			}
			ast, issues := dyn.Compile(expr)
			if issues.Err() != nil {
				t.Fatal(issues.Err())
			}
			program, err := dyn.Program(ast)
			if err != nil {
				t.Fatal(err)
			}
			cases := []struct {
				name, op          string
				current, previous any
				want              bool
			}{
				{"unchanged", "UPDATE", object(nil, "kept"), object(nil, "kept"), true},
				{"missing-to-null", "UPDATE", object(map[string]any{"labels": nil}, "kept"), object(nil, "kept"), name == "current"},
				{"null-to-missing", "UPDATE", object(nil, "kept"), object(map[string]any{"labels": nil}, "kept"), name == "current"},
				{"null-to-null", "UPDATE", object(map[string]any{"labels": nil}, "kept"), object(map[string]any{"labels": nil}, "kept"), true},
				{"new-label", "UPDATE", object(map[string]any{"labels": map[string]any{"a": "b"}}, "kept"), object(nil, "kept"), false},
				{"same-label", "UPDATE", object(map[string]any{"labels": map[string]any{"a": "b"}}, "kept"), object(map[string]any{"labels": map[string]any{"a": "b"}}, "kept"), true},
				{"changed-label", "UPDATE", object(map[string]any{"labels": map[string]any{"a": "b"}}, "kept"), object(map[string]any{"labels": map[string]any{"a": "c"}}, "kept"), false},
				{"changed-boolean", "UPDATE", object(map[string]any{"createSnapshot": false}, "kept"), object(nil, "kept"), false},
				{"create-fenced", "CREATE", object(nil, "new"), nil, false},
				{"other-delete-fenced", "DELETE", nil, object(nil, "kept"), false},
				{"declared-delete", "DELETE", nil, object(nil, "snapshot-uid"), true},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					v, _, err := program.Eval(map[string]any{"object": c.current, "oldObject": c.previous, "request": map[string]any{"operation": c.op}})
					if err != nil || v != types.Bool(c.want) {
						t.Fatalf("got %v err %v want %v", v, err, c.want)
					}
				})
			}
		})
	}
}
