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

func TestLocalPVInventoryWritesStayOnTheirDomainPorts(t *testing.T) {
	ports := map[string]string{
		"EnqueueLocalPVInventoryProtocolUpgrade": "internal/controller/localpv_inventory.go",
		"EnqueueLocalPVInventoryReport":          "internal/controller/localpv_inventory.go",
		"LocalPVInventoryReporter":               "internal/api/image_cache_localpv_admin.go",
	}
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
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(bytes.ToLower(content), []byte("fugue_localpv_inventories")) &&
				relative != "internal/store/postgres.go" &&
				relative != "internal/schemamigrate/image_retirement.go" &&
				relative != "internal/store/internal/localpvinventory/postgres.go" {
				t.Errorf("%s: only the LocalPV repository and its existing schema migrations may use the snapshot table", relative)
			}
			if bytes.Contains(content, []byte(`"fugue/internal/store/internal/localpvinventory"`)) &&
				relative != "internal/store/localpv_inventory.go" {
				t.Errorf("%s: only the Store LocalPV adapter may open the inventory repository", relative)
			}
			if !bytes.Contains(content, []byte("EnqueueLocalPVInventory")) && !bytes.Contains(content, []byte("LocalPVInventoryReporter")) {
				return nil
			}
			set := token.NewFileSet()
			source, err := parser.ParseFile(set, path, content, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(source, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					if allowed, isWritePort := ports[selector.Sel.Name]; isWritePort && relative != allowed {
						t.Errorf("%s: only %s may acquire %s", set.Position(selector.Pos()), allowed, selector.Sel.Name)
					}
				}
				return true
			})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
