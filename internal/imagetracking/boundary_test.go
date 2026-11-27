package imagetracking

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

func TestImageTrackingDoesNotImportApplicationComposition(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	deps, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("list dependencies: %v\n%s", err, deps)
	}
	for _, name := range strings.Fields(string(deps)) {
		for _, forbidden := range []string{"fugue/internal/api", "fugue/internal/controller", "fugue/internal/store"} {
			if name == forbidden || strings.HasPrefix(name, forbidden+"/") {
				t.Errorf("tracking domain depends on composition package %s", name)
			}
		}
	}
}

func TestTrackingWriteCapabilityIsWiredOnlyAtComposition(t *testing.T) {
	root := filepath.Join("..", "..")
	allowed := map[string]bool{
		"internal/api/app_image_tracking.go":    true,
		"internal/controller/image_tracking.go": true,
		"internal/store/app_image_tracking.go":  true,
	}
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
			content, err := os.ReadFile(path)
			if err != nil || !bytes.Contains(content, []byte("ImageTracking")) {
				return err
			}
			set := token.NewFileSet()
			parsed, err := parser.ParseFile(set, path, content, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.CallExpr:
					if selector, ok := n.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "ImageTracking" {
						t.Errorf("%s: acquire image tracking write capability only in its composition adapter", set.Position(n.Pos()))
					}
				case *ast.SelectorExpr:
					if n.Sel.Name == "AppImageTrackings" || n.Sel.Name == "AppImageTrackingChecks" {
						t.Errorf("%s: use the image tracking repository, not raw serialized rows", set.Position(n.Pos()))
					}
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
