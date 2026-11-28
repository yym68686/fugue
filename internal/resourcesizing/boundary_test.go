package resourcesizing

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

func TestResourceSizingDoesNotImportComposition(t *testing.T) {
	deps, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("list dependencies: %v\n%s", err, deps)
	}
	for _, dep := range strings.Fields(string(deps)) {
		for _, root := range []string{"fugue/internal/api", "fugue/internal/controller", "fugue/internal/store"} {
			if dep == root || strings.HasPrefix(dep, root+"/") {
				t.Errorf("resource sizing imports composition package %s", dep)
			}
		}
	}
}

func TestOnlyAPIResourceSizingAdapterAcquiresAutomaticSizingPorts(t *testing.T) {
	methods := []string{"CreateAutoscalingDeployOperation", "EnqueueAutomaticResourceSizingRequest", "RunAutomaticResourceSizing"}
	root := filepath.Join("..", "..")
	for _, tree := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil || filepath.ToSlash(relative) == "internal/api/right_sizing.go" {
				return err
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, method := range methods {
				if !bytes.Contains(content, []byte(method)) {
					continue
				}
				fset := token.NewFileSet()
				source, err := parser.ParseFile(fset, path, content, parser.SkipObjectResolution)
				if err != nil {
					return err
				}
				ast.Inspect(source, func(n ast.Node) bool {
					if selector, ok := n.(*ast.SelectorExpr); ok && selector.Sel.Name == method {
						t.Errorf("%s: acquire %s only in the API resource sizing adapter", fset.Position(selector.Pos()), method)
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
