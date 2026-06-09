package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"fugue/internal/model"
	"fugue/internal/registrymaintenance"
	"fugue/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: fugue-registry-maintenance <scan|active-imports>")
	}
	switch os.Args[1] {
	case "scan":
		runScan(os.Args[2:])
	case "active-imports":
		runActiveImports(os.Args[2:])
	default:
		fatalf("unknown command %q", os.Args[1])
	}
}

func runScan(args []string) {
	flags := flag.NewFlagSet("scan", flag.ExitOnError)
	root := flags.String("root", "/var/lib/registry/docker/registry/v2", "Docker Distribution v2 storage root")
	keepFile := flags.String("keep-digest-file", "", "File containing workload digests to preserve")
	format := flags.String("format", "json", "Output format: json or env")
	_ = flags.Parse(args)

	keepDigests, err := registrymaintenance.ReadKeepDigests(*keepFile)
	if err != nil {
		fatalf("read keep digests: %v", err)
	}
	result, err := registrymaintenance.Scan(*root, keepDigests)
	if err != nil {
		fatalf("scan registry: %v", err)
	}

	if strings.EqualFold(strings.TrimSpace(*format), "env") {
		fmt.Printf("REGISTRY_STORAGE_USED_BYTES=%d\n", result.StorageUsedBytes)
		fmt.Printf("REGISTRY_STORAGE_CAPACITY_BYTES=%d\n", result.StorageCapacityBytes)
		fmt.Printf("REGISTRY_BLOB_COUNT=%d\n", result.BlobCount)
		fmt.Printf("REGISTRY_BLOB_BYTES=%d\n", result.BlobBytes)
		fmt.Printf("REGISTRY_REFERENCED_BLOB_COUNT=%d\n", result.ReferencedBlobCount)
		fmt.Printf("REGISTRY_REFERENCED_BLOB_BYTES=%d\n", result.ReferencedBlobBytes)
		fmt.Printf("REGISTRY_UNREFERENCED_BLOB_COUNT=%d\n", result.UnreferencedBlobCount)
		fmt.Printf("REGISTRY_UNREFERENCED_BLOB_BYTES=%d\n", result.UnreferencedBlobBytes)
		fmt.Printf("REGISTRY_KEEP_DIGEST_COUNT=%d\n", result.KeepDigestCount)
		fmt.Printf("REGISTRY_MISSING_KEEP_DIGEST_COUNT=%d\n", result.MissingKeepDigestCount)
		fmt.Printf("REGISTRY_MANIFEST_REVISION_COUNT=%d\n", result.ManifestRevisionCount)
		return
	}

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fatalf("encode scan result: %v", err)
	}
}

func runActiveImports(args []string) {
	flags := flag.NewFlagSet("active-imports", flag.ExitOnError)
	format := flags.String("format", "count", "Output format: count or json")
	_ = flags.Parse(args)

	stateStore := store.New("/tmp/fugue-registry-maintenance-store.json", strings.TrimSpace(os.Getenv("FUGUE_DATABASE_URL")))
	if err := stateStore.Init(); err != nil {
		fatalf("init store: %v", err)
	}
	operations, err := stateStore.ListActiveOperations()
	if err != nil {
		fatalf("list active operations: %v", err)
	}

	active := make([]model.Operation, 0)
	for _, operation := range operations {
		if operation.Type != model.OperationTypeImport {
			continue
		}
		if operation.Status != model.OperationStatusPending && operation.Status != model.OperationStatusRunning {
			continue
		}
		active = append(active, operation)
	}
	if strings.EqualFold(strings.TrimSpace(*format), "json") {
		if err := json.NewEncoder(os.Stdout).Encode(active); err != nil {
			fatalf("encode active imports: %v", err)
		}
		return
	}
	fmt.Println(len(active))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
