package imagecache

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImageCacheSnapshotAndPlanSQLStayWithTheirRepository(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	owners := map[string]bool{
		"internal/store/internal/imagecache/postgres.go":       true,
		"internal/store/postgres.go":                           true,
		"internal/schemamigrate/image_orphans.go":              true,
		"internal/schemamigrate/image_cache_manifest_graph.go": true,
	}
	for _, tree := range []string{"internal", "cmd"} {
		if err := filepath.WalkDir(filepath.Join(root, tree), func(path string, entry fs.DirEntry, err error) error {
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
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(data, []byte(`"fugue/internal/store/internal/imagecache"`)) &&
				relative != "internal/store/image_cache.go" {
				t.Errorf("%s: only the Store image-cache adapter may open the private repository", relative)
			}
			if owners[relative] {
				return nil
			}
			data = bytes.ToLower(data)
			for _, table := range []string{"fugue_image_cache_nodes", "fugue_image_cache_manifests", "fugue_image_cache_prune_plans"} {
				if bytes.Contains(data, []byte(table)) {
					t.Errorf("%s: access to %s must stay with the image-cache repository or existing schema migrations", relative, table)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
