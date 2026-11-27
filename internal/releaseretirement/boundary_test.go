package releaseretirement

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

func TestReleaseRetirementDoesNotImportApplicationComposition(t *testing.T) {
	deps, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("list dependencies: %v\n%s", err, deps)
	}
	for _, name := range strings.Fields(string(deps)) {
		for _, forbidden := range []string{"fugue/internal/api", "fugue/internal/controller", "fugue/internal/store"} {
			if name == forbidden || strings.HasPrefix(name, forbidden+"/") {
				t.Errorf("retirement domain depends on composition package %s", name)
			}
		}
	}
}

func TestOnlyControllerCompositionAcquiresReleaseRetirementWritePort(t *testing.T) {
	root := filepath.Join("..", "..")
	allowed := map[string]bool{
		"internal/controller/managed_app_reconciler.go":         true,
		"internal/controller/safe_rollout.go":                   true,
		"internal/controller/safe_rollout_historical_retire.go": true,
	}
	const method = "AppReleaseRetirement"
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
			rel, err := filepath.Rel(root, path)
			if err != nil || allowed[filepath.ToSlash(rel)] {
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
					t.Errorf("%s: acquire the retirement write port only in its Controller composition adapter", set.Position(selector.Pos()))
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
