package buildadmission

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildAdmissionDoesNotImportComposition(t *testing.T) {
	deps, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("list dependencies: %v\n%s", err, deps)
	}
	for _, name := range strings.Fields(string(deps)) {
		for _, root := range []string{"fugue/internal/api", "fugue/internal/controller", "fugue/internal/store"} {
			if name == root || strings.HasPrefix(name, root+"/") {
				t.Errorf("build admission imports composition package %s", name)
			}
		}
	}
}

func TestOnlyControllerAdapterAcquiresBuilderAdmissionCheckpoints(t *testing.T) {
	const method = "BuilderAdmissionCheckpoints"
	const allowed = "internal/controller/build_admission.go"
	root := filepath.Join("..", "..")
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil || filepath.ToSlash(relative) == allowed {
				return err
			}
			source, err := os.ReadFile(path)
			if err != nil || !bytes.Contains(source, []byte(method)) {
				return err
			}
			set := token.NewFileSet()
			parsed, err := parser.ParseFile(set, path, source, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == method {
					t.Errorf("%s: acquire builder admission checkpoints only in the Controller adapter", set.Position(selector.Pos()))
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
