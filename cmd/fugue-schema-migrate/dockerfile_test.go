package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSchemaMigratorHasIndependentMinimalImage(t *testing.T) {
	dockerfilePath := filepath.Join("..", "..", "Dockerfile.schema-migrate")
	raw, err := os.ReadFile(dockerfilePath)
	if err != nil {
		t.Fatalf("read schema migrator Dockerfile: %v", err)
	}
	dockerfile := string(raw)
	for _, want := range []string{
		"CGO_ENABLED=0",
		"COPY internal/schemamigrate ./internal/schemamigrate",
		"go build -trimpath -ldflags='-s -w' -o /out/fugue-schema-migrate ./cmd/fugue-schema-migrate",
		"FROM scratch",
		"COPY --from=build /out/fugue-schema-migrate /fugue-schema-migrate",
		"USER 65532:65532",
		`ENTRYPOINT ["/fugue-schema-migrate"]`,
	} {
		if !strings.Contains(dockerfile, want) {
			t.Fatalf("schema migrator Dockerfile missing %q:\n%s", want, dockerfile)
		}
	}
	for _, forbidden := range []string{
		"fugue-api",
		"fugue-controller",
		"internal/store",
		"COPY internal ./internal",
		"kubectl",
		"helm",
		"ENTRYPOINT [\"/bin/",
		"CMD ",
	} {
		if strings.Contains(dockerfile, forbidden) {
			t.Fatalf("schema migrator Dockerfile contains forbidden capability %q:\n%s", forbidden, dockerfile)
		}
	}
	if got := strings.Count(dockerfile, "COPY --from=build"); got != 2 {
		t.Fatalf("runtime image copy count = %d, want binary plus CA bundle", got)
	}
}
