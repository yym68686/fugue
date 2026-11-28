package inventory

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

func TestLocalPVInventoryDoesNotImportComposition(t *testing.T) {
	deps, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("list dependencies: %v\n%s", err, deps)
	}
	for _, dependency := range strings.Fields(string(deps)) {
		for _, root := range []string{"fugue/internal/api", "fugue/internal/controller", "fugue/internal/store"} {
			if dependency == root || strings.HasPrefix(dependency, root+"/") {
				t.Errorf("LocalPV inventory domain imports %s", dependency)
			}
		}
	}
}

func TestOnlyControllerAdapterUsesLocalPVInventoryQueuePorts(t *testing.T) {
	ports := map[string]bool{
		"EnqueueLocalPVInventoryProtocolUpgrade": true,
		"EnqueueLocalPVInventoryReport":          true,
	}
	const allowed = "internal/controller/localpv_inventory.go"
	root := filepath.Join("..", "..", "..")
	for _, sourceTree := range []string{"internal", "cmd"} {
		if err := filepath.WalkDir(filepath.Join(root, sourceTree), func(path string, info fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if info.Name() == "testdata" {
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
			content, err := os.ReadFile(path)
			if err != nil || !bytes.Contains(content, []byte("EnqueueLocalPVInventory")) {
				return err
			}
			set := token.NewFileSet()
			source, err := parser.ParseFile(set, path, content, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(source, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && ports[selector.Sel.Name] {
					t.Errorf("%s: only the Controller LocalPV adapter may acquire this write port", set.Position(selector.Pos()))
				}
				return true
			})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
