package backupusage

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOnlyBackupInventoryAssemblyAcquiresCheckpointWriter(t *testing.T) {
	root := filepath.Join("..", "..")
	adapter := filepath.Join(root, "internal", "api", "backup_inventory.go")
	const method = "BackupInventoryCheckpointWriter"
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
			if path == adapter || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil || !bytes.Contains(source, []byte(method)) {
				return err
			}
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, path, source, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == method {
					t.Errorf("%s: acquire backup inventory checkpoint writer only in its assembly", set.Position(selector.Pos()))
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
