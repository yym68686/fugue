package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	dataConfigPath                            = ".fugue/data.yaml"
	dataHashCachePath                         = ".fugue/data-hash-cache.yaml"
	dataHashCachePerm                         = 0o600
	dataHashCacheVersion                      = 1
	dataTransferStateDir                      = ".fugue/transfers"
	dataTransferStatePerm                     = 0o600
	dataDownloadPartSize                      = 64 * 1024 * 1024
	defaultUntrackedLargeDirThreshold   int64 = 1 << 30
	dataControlPlaneRequestTimeout            = 10 * time.Minute
	defaultDataTransferBlobPageLimit          = 1024
	defaultDataTransferConcurrency            = 32
	defaultDataHashConcurrency                = 4
	dataCheckpointBlobThreshold               = 1024
	dataCheckpointByteThreshold         int64 = 1 << 30
	dataObjectTransferMaxAttempts             = 5
	dataControlPlaneTransferMaxAttempts       = 8
	dataObjectDialTimeout                     = 30 * time.Second
	dataObjectTLSHandshakeTimeout             = 30 * time.Second
	dataObjectResponseHeaderTimeout           = 2 * time.Minute
)

var dataTransferBlobPageLimit = configuredDataTransferBlobPageLimit()

var defaultDataIgnore = []string{
	".fugue",
	".git",
	".venv",
	"__pycache__",
	"*.tmp",
	"*.lock",
	".DS_Store",
}

func configuredDataTransferBlobPageLimit() int {
	raw := strings.TrimSpace(os.Getenv("FUGUE_DATA_BLOB_PAGE_LIMIT"))
	if raw == "" {
		return defaultDataTransferBlobPageLimit
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return defaultDataTransferBlobPageLimit
	}
	return limit
}

var dataHashCacheNow = time.Now
var dataControlPlaneRetrySleep = time.Sleep
var dataTransferStateMu sync.Mutex

func defaultDataHashWorkerCount() int {
	cpus := runtime.NumCPU()
	if cpus <= 0 {
		return defaultDataHashConcurrency
	}
	if cpus < defaultDataHashConcurrency {
		return cpus
	}
	return defaultDataHashConcurrency
}

func normalizeDataConcurrency(value, fallback, max int) int {
	if value <= 0 {
		value = fallback
	}
	if value <= 0 {
		value = 1
	}
	if max > 0 && value > max {
		value = max
	}
	return value
}

type dataConfig struct {
	Version   int               `json:"version" yaml:"version"`
	Workspace string            `json:"workspace" yaml:"workspace"`
	Project   string            `json:"project,omitempty" yaml:"project,omitempty"`
	Assets    []model.DataAsset `json:"assets" yaml:"assets"`
	Ignore    []string          `json:"ignore,omitempty" yaml:"ignore,omitempty"`
}

type dataBlobPlan = model.DataTransferPlanBlob

type dataUploadPlanResponse struct {
	Workspace       model.DataWorkspace `json:"workspace"`
	Transfer        model.DataTransfer  `json:"transfer"`
	Manifest        model.DataManifest  `json:"manifest"`
	Blobs           []dataBlobPlan      `json:"blobs"`
	BlobsTotal      int                 `json:"blobs_total"`
	BlobsOffset     int                 `json:"blobs_offset"`
	BlobsLimit      int                 `json:"blobs_limit"`
	BlobsNextOffset *int                `json:"blobs_next_offset"`
}

type dataDownloadPlanResponse struct {
	Workspace       model.DataWorkspace `json:"workspace"`
	Snapshot        model.DataSnapshot  `json:"snapshot"`
	Transfer        model.DataTransfer  `json:"transfer"`
	Manifest        model.DataManifest  `json:"manifest"`
	Blobs           []dataBlobPlan      `json:"blobs"`
	BlobsTotal      int                 `json:"blobs_total"`
	BlobsOffset     int                 `json:"blobs_offset"`
	BlobsLimit      int                 `json:"blobs_limit"`
	BlobsNextOffset *int                `json:"blobs_next_offset"`
}

type dataWorkspaceEnvelope struct {
	Workspace      model.DataWorkspace `json:"workspace"`
	LatestSnapshot model.DataSnapshot  `json:"latest_snapshot"`
}

type dataSnapshotEnvelope struct {
	Workspace model.DataWorkspace `json:"workspace"`
	Snapshot  model.DataSnapshot  `json:"snapshot"`
}

type dataTransferCompleteResponse struct {
	Workspace model.DataWorkspace `json:"workspace"`
	Transfer  model.DataTransfer  `json:"transfer"`
	Snapshot  model.DataSnapshot  `json:"snapshot"`
}

type dataTransferAuthorizationResponse struct {
	Workspace       model.DataWorkspace `json:"workspace"`
	Transfer        model.DataTransfer  `json:"transfer"`
	Manifest        model.DataManifest  `json:"manifest"`
	Blobs           []dataBlobPlan      `json:"blobs"`
	BlobsTotal      int                 `json:"blobs_total"`
	BlobsOffset     int                 `json:"blobs_offset"`
	BlobsLimit      int                 `json:"blobs_limit"`
	BlobsNextOffset *int                `json:"blobs_next_offset"`
}

type dataGCSweepResponse struct {
	Workspace model.DataWorkspace     `json:"workspace"`
	GC        model.DataGCSweepResult `json:"gc"`
}

type dataBackendMigrationResponse struct {
	Workspace model.DataWorkspace `json:"workspace"`
	Transfer  model.DataTransfer  `json:"transfer"`
}

type dataProgressRenderer struct {
	w          io.Writer
	enabled    bool
	label      string
	total      int64
	done       int64
	start      time.Time
	last       time.Time
	lastWidth  int
	lineActive bool
	finished   bool
	mu         sync.Mutex
}

type dataTransferProgress func(delta int64)

type dataTableAlign string

const (
	dataTableAlignLeft  dataTableAlign = "left"
	dataTableAlignRight dataTableAlign = "right"
)

type dataTableColumn struct {
	Title string
	Align dataTableAlign
}

type dataScanEstimate struct {
	Files int64 `json:"files"`
	Bytes int64 `json:"bytes"`
}

type dataLocalFileIdentity struct {
	Device uint64 `json:"device" yaml:"device"`
	Inode  uint64 `json:"inode" yaml:"inode"`
}

type dataHashCache struct {
	Version int                  `json:"version" yaml:"version"`
	Entries []dataHashCacheEntry `json:"entries,omitempty" yaml:"entries,omitempty"`
}

type dataHashCacheEntry struct {
	Path          string `json:"path" yaml:"path"`
	Size          int64  `json:"size" yaml:"size"`
	MTimeUnixNano int64  `json:"mtime_unix_nano" yaml:"mtime_unix_nano"`
	Device        uint64 `json:"device" yaml:"device"`
	Inode         uint64 `json:"inode" yaml:"inode"`
	SHA256        string `json:"sha256" yaml:"sha256"`
	ComputedAt    string `json:"computed_at" yaml:"computed_at"`
}

type dataHashCacheLookup struct {
	byPath     map[string]dataHashCacheEntry
	byIdentity map[string]dataHashCacheEntry
}

type dataTransferState struct {
	TransferID     string         `json:"transfer_id"`
	Direction      string         `json:"direction"`
	WorkspaceID    string         `json:"workspace_id"`
	SnapshotID     string         `json:"snapshot_id,omitempty"`
	Version        string         `json:"version,omitempty"`
	ManifestDigest string         `json:"manifest_digest,omitempty"`
	Blobs          []dataBlobPlan `json:"blobs,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type pullConflict struct {
	Kind   string `json:"kind"`
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type untrackedDataDirectory struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type pullPlan struct {
	Download  []model.DataManifestEntry `json:"download"`
	Skip      []model.DataManifestEntry `json:"skip"`
	Conflicts []pullConflict            `json:"conflicts"`
	Warnings  []pullConflict            `json:"warnings"`
	Prune     []string                  `json:"prune"`
}

type dataEvictPlan struct {
	Evict   []dataEvictPlanAsset `json:"evict"`
	Skip    []dataEvictPlanAsset `json:"skip"`
	Blocked []dataEvictPlanAsset `json:"blocked"`
}

type dataEvictPlanAsset struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	Files  int    `json:"files"`
	Bytes  int64  `json:"bytes"`
}

func (c *CLI) newDataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "data",
		Short: "Manage Fugue Data Workspaces for GPU training datasets and artifacts",
		Long: strings.TrimSpace(`
Manage project-level data workspaces. A data workspace tracks datasets, checkpoints,
outputs, and other training artifacts as versioned manifests so they can be pushed
from one GPU server and pulled on another without using the user's laptop as a
storage relay.
`),
		Example: strings.TrimSpace(`
  # Minimal provider-to-provider migration flow
  fugue data track ./data ./checkpoints ./outputs
  fugue data push --version before-provider-move

  # On the target GPU server, attach the project and pull the latest version
  fugue data workspace use my-training-project
  fugue data pull

  # Resume an interrupted upload or download
  fugue data transfer resume data_transfer_123

  # Configure a Cloudflare R2 backend
  fugue data backend create prod-r2 \
    --provider cloudflare-r2 \
    --bucket fugue-data-prod \
    --account-id "$FUGUE_DATA_R2_ACCOUNT_ID" \
    --access-key-id "$FUGUE_DATA_BACKEND_ACCESS_KEY_ID" \
    --secret-access-key "$FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY"
`),
	}
	cmd.AddCommand(
		c.newDataInitCommand(),
		c.newDataTrackCommand(),
		c.newDataUntrackCommand(),
		c.newDataStatusCommand(),
		c.newDataPushCommand(),
		c.newDataPullCommand(),
		c.newDataEvictCommand(),
		c.newDataCloneCommand(),
		c.newDataWorkspaceCommand(),
		c.newDataSnapshotCommand(),
		c.newDataGrantCommand(),
		c.newDataTransferCommand(),
		c.newDataBackendCommand(),
		c.newDataGCCommand(),
		c.newDataDoctorCommand(),
	)
	return cmd
}

func (c *CLI) newDataInitCommand() *cobra.Command {
	var workspaceName, projectName, region string
	var noDetect bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize .fugue/data.yaml in the current project",
		Example: strings.TrimSpace(`
  fugue data init
  fugue data init --workspace my-training-project --project default
  fugue data init --no-detect
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, created, err := ensureDataConfig(".", workspaceName, projectName, !noDetect)
			if err != nil {
				return err
			}
			if region != "" {
				_ = region
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"config": cfg, "created": created})
			}
			if created {
				fmt.Fprintln(c.stdout, "Created .fugue/data.yaml")
			} else {
				fmt.Fprintln(c.stdout, ".fugue/data.yaml already exists")
			}
			if len(cfg.Assets) > 0 {
				fmt.Fprintln(c.stdout, "\nTracked assets:")
				renderDataAssetsTable(c.stdout, cfg.Assets)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&workspaceName, "workspace", "", "Data workspace name. Defaults to the current directory name")
	cmd.Flags().StringVar(&projectName, "project", "", "Fugue project name to record in .fugue/data.yaml")
	cmd.Flags().StringVar(&region, "region", "", "Default data region hint")
	cmd.Flags().BoolVar(&noDetect, "no-detect", false, "Do not auto-detect common data directories")
	return cmd
}

func (c *CLI) newDataTrackCommand() *cobra.Command {
	var asName, mode string
	var optional bool
	cmd := &cobra.Command{
		Use:   "track <path> [path...]",
		Short: "Track one or more local data assets",
		Example: strings.TrimSpace(`
  fugue data track ./data ./checkpoints ./outputs
  fugue data track ./dataset.zip --as dataset
  fugue data track ./outputs --mode append --optional
`),
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, created, err := ensureDataConfig(".", "", "", false)
			if err != nil {
				return err
			}
			for _, rawPath := range args {
				clean, err := cleanConfigPath(rawPath)
				if err != nil {
					return err
				}
				name := strings.TrimSpace(asName)
				if name == "" {
					name = assetNameFromPath(clean)
				}
				if len(args) > 1 && asName != "" {
					return fmt.Errorf("--as can only be used when tracking one path")
				}
				if dataConfigAssetIndex(cfg, name) >= 0 {
					return fmt.Errorf("asset %q already exists; use --as to choose another name", name)
				}
				cfg.Assets = append(cfg.Assets, model.NormalizeDataAsset(model.DataAsset{
					Name:     name,
					Path:     clean,
					Required: !optional,
					Mode:     mode,
				}))
			}
			sortDataConfigAssets(&cfg)
			if err := writeDataConfig(".", cfg); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"config": cfg, "created": created})
			}
			if created {
				fmt.Fprintln(c.stdout, "Created .fugue/data.yaml")
			}
			renderDataKeyValueTable(c.stdout, [][]string{{"Data workspace", cfg.Workspace}})
			fmt.Fprintln(c.stdout)
			fmt.Fprintln(c.stdout, "Tracked assets:")
			renderDataAssetsTable(c.stdout, cfg.Assets)
			return nil
		},
	}
	cmd.Flags().StringVar(&asName, "as", "", "Asset name")
	cmd.Flags().StringVar(&mode, "mode", model.DataAssetModeReadMostly, "Asset mode: read-mostly or append")
	cmd.Flags().BoolVar(&optional, "optional", false, "Mark the asset as optional")
	return cmd
}

func (c *CLI) newDataUntrackCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "untrack <asset-or-path>",
		Short: "Stop tracking a data asset without deleting local or remote data",
		Example: strings.TrimSpace(`
  fugue data untrack outputs
  fugue data untrack ./outputs
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			value := strings.TrimSpace(args[0])
			next := cfg.Assets[:0]
			removed := false
			for _, asset := range cfg.Assets {
				if asset.Name == value || asset.Path == value {
					removed = true
					continue
				}
				next = append(next, asset)
			}
			if !removed {
				return fmt.Errorf("asset %q is not tracked", value)
			}
			cfg.Assets = next
			if err := writeDataConfig(".", cfg); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"config": cfg, "removed": value})
			}
			fmt.Fprintf(c.stdout, "Untracked %s\n", value)
			return nil
		},
	}
	return cmd
}

func (c *CLI) newDataStatusCommand() *cobra.Command {
	var noProgress bool
	hashConcurrency := defaultDataHashWorkerCount()
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show local or account-level data workspace status",
		Example: strings.TrimSpace(`
  fugue data status
  fugue data status --json
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					client, clientErr := c.newClient()
					if clientErr != nil {
						return clientErr
					}
					return c.renderAccountDataStatus(client)
				}
				return err
			}
			estimate, err := estimateDataManifestScanAllowMissing(".", cfg, "")
			if err != nil {
				return err
			}
			scanProgress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Hashing local data", estimate.Bytes)
			manifest, pathsByDigest, _, err := scanDataManifestWithConcurrencyAllowMissing(".", cfg, "", scanProgress.advance, hashConcurrency)
			if err != nil {
				return err
			}
			scanProgress.finish()
			_ = pathsByDigest
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspace, err := c.ensureRemoteDataWorkspace(client, cfg)
			if err != nil {
				return err
			}
			var latest model.DataSnapshot
			latestResp, err := client.GetDataWorkspace(workspace.ID)
			if err == nil {
				latest = latestResp.LatestSnapshot
				if latest.ID != "" {
					if fullLatest, fullErr := client.GetDataSnapshot(workspace.ID, latest.ID); fullErr == nil {
						latest = fullLatest.Snapshot
					}
				}
			}
			untrackedLargeDirs, err := findUntrackedLargeDataDirectories(".", cfg, dataUntrackedLargeDirThreshold())
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": workspace, "local_manifest": manifest, "latest_snapshot": latest, "untracked_large_directories": untrackedLargeDirs})
			}
			summaryRows := [][]string{{"Data workspace", workspace.Name}}
			if cfg.Project != "" {
				summaryRows = append(summaryRows, []string{"Project", cfg.Project})
			}
			if latest.ID != "" {
				summaryRows = append(summaryRows, []string{"Latest version", latest.Version})
			} else {
				summaryRows = append(summaryRows, []string{"Latest version", "none"})
			}
			renderDataKeyValueTable(c.stdout, summaryRows)
			fmt.Fprintln(c.stdout)
			localByAsset := manifestStatsByAsset(manifest)
			remoteByAsset := manifestStatsByAsset(latest.Manifest)
			rows := make([][]string, 0, len(cfg.Assets))
			for _, asset := range cfg.Assets {
				stats := localByAsset[asset.Name]
				status := "new"
				if _, err := os.Stat(asset.Path); os.IsNotExist(err) {
					status = "missing"
					if latest.ID != "" {
						if remoteStats, ok := remoteByAsset[asset.Name]; ok {
							status = "evicted"
							stats = remoteStats
						}
					}
				} else if latest.ID != "" {
					if manifestAssetDigest(manifest, asset.Name) == manifestAssetDigest(latest.Manifest, asset.Name) {
						status = "unchanged"
					} else if _, ok := remoteByAsset[asset.Name]; ok {
						status = "changed"
					}
				}
				rows = append(rows, []string{asset.Name, asset.Path, status, strconv.Itoa(stats.Files), formatBytes(stats.Bytes)})
			}
			renderDataTable(c.stdout, []dataTableColumn{
				{Title: "Asset"},
				{Title: "Local path"},
				{Title: "Status"},
				{Title: "Files", Align: dataTableAlignRight},
				{Title: "Size", Align: dataTableAlignRight},
			}, rows)
			if len(untrackedLargeDirs) > 0 {
				fmt.Fprintln(c.stdout, "\nUntracked large directories:")
				untrackedRows := make([][]string, 0, len(untrackedLargeDirs))
				for _, dir := range untrackedLargeDirs {
					untrackedRows = append(untrackedRows, []string{dir.Path, formatBytes(dir.Bytes)})
				}
				renderDataTable(c.stdout, []dataTableColumn{
					{Title: "Path"},
					{Title: "Size", Align: dataTableAlignRight},
				}, untrackedRows)
				fmt.Fprintln(c.stdout, "Run `fugue data track <path>` if this directory is part of the training workspace.")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress rendering")
	cmd.Flags().IntVar(&hashConcurrency, "hash-concurrency", hashConcurrency, "Local file hashing concurrency")
	return cmd
}

func (c *CLI) renderAccountDataStatus(client *Client) error {
	workspaces, err := client.ListDataWorkspaces()
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{"local_bound": false, "workspaces": workspaces})
	}
	fmt.Fprintln(c.stdout, "No local data workspace is bound in this directory.")
	fmt.Fprintln(c.stdout)
	if len(workspaces) == 0 {
		fmt.Fprintln(c.stdout, "No data workspaces found.")
		fmt.Fprintln(c.stdout)
		fmt.Fprintln(c.stdout, "Use:")
		fmt.Fprintln(c.stdout, "  fugue data track <path>")
		return nil
	}
	fmt.Fprintln(c.stdout, "Your data workspaces:")
	rows := make([][]string, 0, len(workspaces))
	for _, workspace := range workspaces {
		backend := strings.TrimSpace(workspace.StorageBackendID)
		if backend == "" {
			backend = "default"
		}
		rows = append(rows, []string{
			workspace.Name,
			backend,
			strconv.Itoa(len(workspace.Assets)),
			formatBytes(workspace.UsedBytes),
			formatTime(workspace.UpdatedAt),
		})
	}
	renderDataTable(c.stdout, []dataTableColumn{
		{Title: "Workspace"},
		{Title: "Backend"},
		{Title: "Assets", Align: dataTableAlignRight},
		{Title: "Used", Align: dataTableAlignRight},
		{Title: "Updated"},
	}, rows)
	fmt.Fprintln(c.stdout)
	fmt.Fprintln(c.stdout, "Use:")
	fmt.Fprintln(c.stdout, "  fugue data workspace use <workspace>")
	fmt.Fprintln(c.stdout, "  fugue data clone <workspace>")
	fmt.Fprintln(c.stdout, "  fugue data track <path>")
	return nil
}

func (c *CLI) newDataPushCommand() *cobra.Command {
	var version, message, assetName string
	var dryRun, noResume, noProgress bool
	var concurrency int
	hashConcurrency := defaultDataHashWorkerCount()
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Upload changed data assets and create a new data version",
		Example: strings.TrimSpace(`
  fugue data push
  fugue data push --version before-provider-move
  fugue data push --asset checkpoints --version checkpoint-step-8000
  fugue data push --dry-run
  fugue data push --no-resume
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspace, err := c.ensureRemoteDataWorkspace(client, cfg)
			if err != nil {
				return err
			}
			estimate, err := estimateDataManifestScan(".", cfg, assetName)
			if err != nil {
				return err
			}
			scanProgress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Hashing local data", estimate.Bytes)
			manifest, pathsByDigest, err := scanDataManifestWithConcurrency(".", cfg, assetName, scanProgress.advance, hashConcurrency)
			if err != nil {
				return err
			}
			scanProgress.finish()
			if dryRun {
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"workspace": workspace, "manifest": manifest, "dry_run": true})
				}
				renderDataKeyValueTable(c.stdout, [][]string{
					{"Data workspace", workspace.Name},
					{"Plan", "upload"},
					{"Assets", strconv.Itoa(len(cfg.Assets))},
					{"Files", strconv.Itoa(manifest.FileCount)},
					{"Upload size", formatBytes(manifest.TotalBytes)},
				})
				return nil
			}
			manifestDigest := digestDataManifest(manifest)
			var plan dataUploadPlanResponse
			var resumed bool
			if !noResume {
				if state, ok, err := findDataTransferState(".", model.DataTransferDirectionUpload, workspace.ID, "", manifestDigest); err != nil {
					return err
				} else if ok {
					refresh, err := client.RefreshDataTransferAuthorizationPage(state.TransferID, 0, dataTransferBlobPageLimit)
					if err == nil {
						plan = dataUploadPlanResponse{
							Workspace:       refresh.Workspace,
							Transfer:        refresh.Transfer,
							Manifest:        manifest,
							Blobs:           mergeDataPlanBlobsWithState(refresh.Blobs, state),
							BlobsTotal:      refresh.BlobsTotal,
							BlobsOffset:     refresh.BlobsOffset,
							BlobsLimit:      refresh.BlobsLimit,
							BlobsNextOffset: refresh.BlobsNextOffset,
						}
						resumed = true
					}
				}
			}
			if !resumed {
				plan, err = client.PlanDataUpload(workspace.ID, version, message, manifest)
				if err != nil {
					return err
				}
			}
			if manifest.FileCount > 0 && len(plan.Blobs) == 0 {
				return fmt.Errorf("upload plan returned no blob entries for %d files", manifest.FileCount)
			}
			if !c.wantsJSON() {
				action := "Planning upload"
				if resumed {
					action = "Resuming upload"
				}
				renderDataKeyValueTable(c.stdout, [][]string{
					{"Data workspace", workspace.Name},
					{"Action", action},
					{"Transfer", plan.Transfer.ID},
					{"Files", strconv.Itoa(manifest.FileCount)},
					{"Upload size", formatBytes(manifest.TotalBytes)},
				})
				fmt.Fprintln(c.stdout)
			}
			state := dataTransferState{
				TransferID:     plan.Transfer.ID,
				Direction:      model.DataTransferDirectionUpload,
				WorkspaceID:    workspace.ID,
				Version:        strings.TrimSpace(version),
				ManifestDigest: manifestDigest,
				Blobs:          plan.Blobs,
			}
			if !noResume {
				if err := saveDataTransferState(".", state); err != nil {
					return err
				}
			}
			if err := c.uploadDataPlanBlobs(client, workspace.ID, plan.Transfer.ID, manifestDigest, plan.Blobs, pathsByDigest, !noResume, noProgress, concurrency, manifest.TotalBytes, plan.BlobsOffset, plan.BlobsLimit, plan.BlobsNextOffset); err != nil {
				_, _ = client.CompleteDataTransfer(plan.Transfer.ID, map[string]any{"error_code": "upload_failed", "error_message": err.Error()})
				return err
			}
			complete, err := client.CompleteDataTransfer(plan.Transfer.ID, map[string]any{
				"version":    strings.TrimSpace(version),
				"message":    message,
				"manifest":   manifest,
				"bytes_done": manifest.TotalBytes,
				"files_done": manifest.FileCount,
			})
			if err != nil {
				return err
			}
			_ = removeDataTransferState(".", plan.Transfer.ID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, complete)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Created version", complete.Snapshot.Version},
				{"ID", complete.Snapshot.ID},
				{"Files", strconv.Itoa(complete.Snapshot.FileCount)},
				{"Size", formatBytes(complete.Snapshot.TotalBytes)},
			})
			return nil
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "Version label. Defaults to latest")
	cmd.Flags().StringVar(&message, "message", "", "Version message")
	cmd.Flags().StringVar(&assetName, "asset", "", "Only push one asset")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show upload plan without uploading")
	cmd.Flags().IntVar(&concurrency, "concurrency", defaultDataTransferConcurrency, "Transfer concurrency")
	cmd.Flags().IntVar(&hashConcurrency, "hash-concurrency", hashConcurrency, "Local file hashing concurrency")
	cmd.Flags().BoolVar(&noResume, "no-resume", false, "Ignore local transfer resume state")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress rendering")
	return cmd
}

func (c *CLI) newDataPullCommand() *cobra.Command {
	var version, assetName, toPath string
	var verify, dryRun, keepLocal, overwrite, prune, confirm, noResume, noProgress bool
	var concurrency int
	hashConcurrency := defaultDataHashWorkerCount()
	cmd := &cobra.Command{
		Use:   "pull [asset-or-path...]",
		Short: "Download a data workspace version into the current project",
		Example: strings.TrimSpace(`
  fugue data pull
  fugue data pull kernel.pt
  fugue data pull kernel-pt
  fugue data pull ./models/kernel.pt
  fugue data pull --version before-provider-move
  fugue data pull --asset dataset
  fugue data pull --asset checkpoints --to /mnt/nvme/checkpoints
  fugue data pull --dry-run
  fugue data pull --overwrite
  fugue data pull --prune --confirm
`),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			selectedAssets, err := selectDataPullAssets(cfg, assetName, args)
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspace, err := c.ensureRemoteDataWorkspace(client, cfg)
			if err != nil {
				return err
			}
			assets := []string{}
			for _, asset := range selectedAssets {
				assets = append(assets, asset.Name)
			}
			planResp, err := client.PlanDataDownload(workspace.ID, version, assets)
			if err != nil {
				return err
			}
			targetCfg := cfg
			if toPath != "" {
				if len(selectedAssets) != 1 {
					return fmt.Errorf("--to requires exactly one asset selector")
				}
				for idx := range targetCfg.Assets {
					if targetCfg.Assets[idx].Name == selectedAssets[0].Name {
						targetCfg.Assets[idx].Path = toPath
					}
				}
			}
			preflightBytes, err := estimatePullPreflightHashBytes(".", targetCfg, planResp.Manifest)
			if err != nil {
				return err
			}
			preflightProgress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Checking local files", preflightBytes)
			pullPlan, err := buildPullPlanWithConcurrency(".", targetCfg, planResp.Manifest, overwrite, keepLocal, prune, preflightProgress.advance, hashConcurrency)
			if err != nil {
				return err
			}
			preflightProgress.finish()
			if dryRun || len(pullPlan.Conflicts) > 0 {
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"workspace": workspace, "snapshot": planResp.Snapshot, "plan": pullPlan, "dry_run": dryRun})
				}
				renderPullPreflight(c.stdout, workspace, planResp.Snapshot, pullPlan)
				if len(pullPlan.Conflicts) > 0 && !dryRun {
					return fmt.Errorf("pull preflight found conflicts")
				}
				return nil
			}
			if prune && !confirm && len(pullPlan.Prune) > 0 {
				return fmt.Errorf("--prune requires --confirm before deleting %d local files", len(pullPlan.Prune))
			}
			progress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Download", totalManifestFileBytes(pullPlan.Download))
			for _, removePath := range pullPlan.Prune {
				if prune && confirm {
					if err := os.Remove(removePath); err != nil && !os.IsNotExist(err) {
						return err
					}
				}
			}
			if !noResume {
				if err := saveDataTransferState(".", dataTransferState{
					TransferID:     planResp.Transfer.ID,
					Direction:      model.DataTransferDirectionDownload,
					WorkspaceID:    workspace.ID,
					SnapshotID:     planResp.Snapshot.ID,
					Version:        planResp.Snapshot.Version,
					ManifestDigest: planResp.Snapshot.ManifestDigest,
					Blobs:          planResp.Blobs,
				}); err != nil {
					return err
				}
			}
			if _, _, err := c.downloadDataManifestEntriesPaged(client, planResp.Transfer.ID, ".", targetCfg, pullPlan.Download, planResp.Blobs, planResp.BlobsOffset, planResp.BlobsLimit, planResp.BlobsNextOffset, !noResume, overwrite, noProgress, concurrency, progress); err != nil {
				return err
			}
			progress.finish()
			if verify {
				verifyProgress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Verifying local data", totalManifestFileBytes(planResp.Manifest.Entries))
				if err := verifyPulledManifestWithConcurrency(".", targetCfg, planResp.Manifest, verifyProgress.advance, hashConcurrency); err != nil {
					return err
				}
				verifyProgress.finish()
			}
			complete, err := client.CompleteDataTransfer(planResp.Transfer.ID, map[string]any{
				"snapshot_id": planResp.Snapshot.ID,
				"bytes_done":  planResp.Manifest.TotalBytes,
				"files_done":  planResp.Manifest.FileCount,
			})
			if err != nil {
				return err
			}
			_ = removeDataTransferState(".", planResp.Transfer.ID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, complete)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Restored version", planResp.Snapshot.Version},
				{"Files", strconv.Itoa(planResp.Snapshot.FileCount)},
				{"Size", formatBytes(planResp.Snapshot.TotalBytes)},
			})
			return nil
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "Version to pull. Defaults to latest")
	cmd.Flags().StringVar(&assetName, "asset", "", "Only pull one asset selector")
	cmd.Flags().StringVar(&toPath, "to", "", "Override target path for a single asset")
	cmd.Flags().BoolVar(&verify, "verify", false, "Verify restored files after download")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show pull plan without downloading")
	cmd.Flags().BoolVar(&keepLocal, "keep-local", false, "Skip conflicting local files")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite conflicting local files")
	cmd.Flags().BoolVar(&prune, "prune", false, "Delete local files absent from the version manifest")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Required with --prune to delete extra local files")
	cmd.Flags().BoolVar(&noResume, "no-resume", false, "Ignore local transfer resume state")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress rendering")
	cmd.Flags().IntVar(&concurrency, "concurrency", 8, "Transfer concurrency")
	cmd.Flags().IntVar(&hashConcurrency, "hash-concurrency", hashConcurrency, "Local file hashing concurrency")
	return cmd
}

func (c *CLI) newDataEvictCommand() *cobra.Command {
	var confirm, dryRun, noProgress bool
	hashConcurrency := defaultDataHashWorkerCount()
	cmd := &cobra.Command{
		Use:   "evict [asset...]",
		Short: "Free local space for data assets already saved in the latest version",
		Example: strings.TrimSpace(`
  fugue data evict
  fugue data evict checkpoints
  fugue data evict checkpoints outputs --confirm
  fugue data pull
`),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspace, err := c.ensureRemoteDataWorkspace(client, cfg)
			if err != nil {
				return err
			}
			workspaceResp, err := client.GetDataWorkspace(workspace.ID)
			if err != nil {
				return err
			}
			latest := workspaceResp.LatestSnapshot
			if latest.ID == "" {
				return fmt.Errorf("no remote data version exists for workspace %s; run fugue data push first", workspace.Name)
			}
			fullLatest, err := client.GetDataSnapshot(workspace.ID, latest.ID)
			if err != nil {
				return err
			}
			latest = fullLatest.Snapshot
			selected, err := selectDataEvictAssets(cfg, args)
			if err != nil {
				return err
			}
			selectedCfg := cfg
			selectedCfg.Assets = selected
			estimate, err := estimateDataManifestScanAllowMissing(".", selectedCfg, "")
			if err != nil {
				return err
			}
			scanProgress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Checking local data", estimate.Bytes)
			localManifest, _, _, err := scanDataManifestWithConcurrencyAllowMissing(".", selectedCfg, "", scanProgress.advance, hashConcurrency)
			if err != nil {
				return err
			}
			scanProgress.finish()
			plan, err := buildDataEvictPlan(".", selected, localManifest, latest.Manifest)
			if err != nil {
				return err
			}
			if c.wantsJSON() && (dryRun || !confirm || len(plan.Blocked) > 0) {
				if err := writeJSON(c.stdout, map[string]any{"workspace": workspace, "latest_snapshot": latest, "plan": plan, "dry_run": dryRun || !confirm}); err != nil {
					return err
				}
			} else if !c.wantsJSON() {
				renderDataEvictPlan(c.stdout, workspace, latest, plan, !confirm || dryRun)
			}
			if len(plan.Blocked) > 0 {
				return fmt.Errorf("evict blocked for %d asset(s)", len(plan.Blocked))
			}
			if dryRun || !confirm {
				return nil
			}
			selectedByName := map[string]model.DataAsset{}
			for _, asset := range selected {
				selectedByName[asset.Name] = asset
			}
			for _, item := range plan.Evict {
				asset, ok := selectedByName[item.Name]
				if !ok {
					return fmt.Errorf("asset %q is not tracked", item.Name)
				}
				if err := removeDataAssetPath(".", asset); err != nil {
					return err
				}
			}
			if len(plan.Evict) == 0 {
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"workspace": workspace, "latest_snapshot": latest, "plan": plan, "evicted": plan.Evict, "dry_run": false})
				}
				return nil
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": workspace, "latest_snapshot": latest, "plan": plan, "evicted": plan.Evict, "dry_run": false})
			}
			fmt.Fprintln(c.stdout)
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Evicted assets", strconv.Itoa(len(plan.Evict))},
				{"Freed", formatBytes(totalDataEvictPlanBytes(plan.Evict))},
			})
			return nil
		},
	}
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Delete safe local asset paths after verification")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show evict plan without deleting local files")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress rendering")
	cmd.Flags().IntVar(&hashConcurrency, "hash-concurrency", hashConcurrency, "Local file hashing concurrency")
	return cmd
}

func (c *CLI) newDataCloneCommand() *cobra.Command {
	var toPath, version, assetName, grant string
	var noProgress bool
	var concurrency int
	hashConcurrency := defaultDataHashWorkerCount()
	cmd := &cobra.Command{
		Use:   "clone <workspace>",
		Short: "Clone a data workspace into a target directory",
		Example: strings.TrimSpace(`
  fugue data clone my-training-project
  fugue data clone my-training-project --to /workspace/my-training-project
  fugue data clone my-training-project --version before-provider-move --asset dataset
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspaceName := strings.TrimSpace(args[0])
			workspaceResp, err := client.GetDataWorkspace(workspaceName)
			if err != nil {
				return err
			}
			target := strings.TrimSpace(toPath)
			if target == "" {
				target = workspaceResp.Workspace.Slug
			}
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			cfg := dataConfig{
				Version:   1,
				Workspace: workspaceResp.Workspace.Name,
				Assets:    workspaceResp.Workspace.Assets,
				Ignore:    defaultDataIgnore,
			}
			if err := writeDataConfig(target, cfg); err != nil {
				return err
			}
			_ = grant
			planResp, err := client.PlanDataDownload(workspaceResp.Workspace.ID, version, compactStrings([]string{assetName}))
			if err == nil {
				preflightBytes, err := estimatePullPreflightHashBytes(target, cfg, planResp.Manifest)
				if err != nil {
					return err
				}
				preflightProgress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Checking local files", preflightBytes)
				pullPlan, err := buildPullPlanWithConcurrency(target, cfg, planResp.Manifest, false, false, false, preflightProgress.advance, hashConcurrency)
				if err != nil {
					return err
				}
				preflightProgress.finish()
				if len(pullPlan.Conflicts) > 0 {
					renderPullPreflight(c.stdout, workspaceResp.Workspace, planResp.Snapshot, pullPlan)
					return fmt.Errorf("pull preflight found conflicts")
				}
				progress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Download", totalManifestFileBytes(pullPlan.Download))
				if _, _, err := c.downloadDataManifestEntriesPaged(client, planResp.Transfer.ID, target, cfg, pullPlan.Download, planResp.Blobs, planResp.BlobsOffset, planResp.BlobsLimit, planResp.BlobsNextOffset, true, false, noProgress, concurrency, progress); err != nil {
					return err
				}
				progress.finish()
				if _, err := client.CompleteDataTransfer(planResp.Transfer.ID, map[string]any{"snapshot_id": planResp.Snapshot.ID, "bytes_done": planResp.Manifest.TotalBytes, "files_done": planResp.Manifest.FileCount}); err != nil {
					return err
				}
			} else if version != "" || assetName != "" {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": workspaceResp.Workspace, "path": target})
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Data workspace", workspaceResp.Workspace.Name},
				{"Path", target},
			})
			return nil
		},
	}
	cmd.Flags().StringVar(&toPath, "to", "", "Target directory")
	cmd.Flags().StringVar(&version, "version", "", "Version to pull after cloning")
	cmd.Flags().StringVar(&assetName, "asset", "", "Only pull one asset after cloning")
	cmd.Flags().StringVar(&grant, "grant", "", "Data grant secret")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress rendering")
	cmd.Flags().IntVar(&concurrency, "concurrency", 8, "Transfer concurrency")
	cmd.Flags().IntVar(&hashConcurrency, "hash-concurrency", hashConcurrency, "Local file hashing concurrency")
	return cmd
}

func (c *CLI) newDataWorkspaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workspace",
		Short: "List, show, and bind data workspaces",
		Example: strings.TrimSpace(`
  fugue data workspace ls
  fugue data workspace show
  fugue data workspace use my-training-project
  fugue data workspace set-backend my-training-project prod-r2
`),
	}
	cmd.AddCommand(
		c.newDataWorkspaceListCommand(),
		c.newDataWorkspaceShowCommand(),
		c.newDataWorkspaceUseCommand(),
		c.newDataWorkspaceSetBackendCommand(),
		c.newDataWorkspaceAccessCommand(),
		c.newDataWorkspaceShareCommand(),
		c.newDataWorkspaceUnshareCommand(),
	)
	return cmd
}

func (c *CLI) newDataWorkspaceListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List data workspaces",
		Example: strings.TrimSpace(`
  fugue data workspace ls
  fugue data workspace ls --json
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspaces, err := client.ListDataWorkspaces()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspaces": workspaces})
			}
			rows := make([][]string, 0, len(workspaces))
			for _, workspace := range workspaces {
				rows = append(rows, []string{workspace.Name, workspace.StorageBackendID, formatBytes(workspace.UsedBytes)})
			}
			renderDataTable(c.stdout, []dataTableColumn{
				{Title: "Workspace"},
				{Title: "Backend"},
				{Title: "Used", Align: dataTableAlignRight},
			}, rows)
			return nil
		},
	}
}

func (c *CLI) newDataWorkspaceShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show [workspace]",
		Short: "Show a data workspace",
		Example: strings.TrimSpace(`
  fugue data workspace show
  fugue data workspace show my-training-project
`),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := ""
			if len(args) > 0 {
				name = args[0]
			} else {
				cfg, err := readDataConfig(".")
				if err != nil {
					return err
				}
				name = cfg.Workspace
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.GetDataWorkspace(name)
			if err != nil {
				return err
			}
			latest := resp.LatestSnapshot
			if !c.wantsJSON() && latest.ID != "" && len(latest.Manifest.Entries) == 0 {
				if fullLatest, fullErr := client.GetDataSnapshot(resp.Workspace.ID, latest.ID); fullErr == nil {
					latest = fullLatest.Snapshot
				}
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			summaryRows := [][]string{
				{"Data workspace", resp.Workspace.Name},
				{"Backend", resp.Workspace.StorageBackendID},
				{"Used", formatBytes(resp.Workspace.UsedBytes)},
			}
			if latest.ID != "" {
				summaryRows = append(summaryRows, []string{"Latest version", latest.Version})
				summaryRows = append(summaryRows, []string{"Latest files", fmt.Sprintf("%d", dataSnapshotFileCount(latest))})
				summaryRows = append(summaryRows, []string{"Latest size", formatBytes(dataSnapshotTotalBytes(latest))})
			}
			renderDataKeyValueTable(c.stdout, summaryRows)
			if len(resp.Workspace.Assets) > 0 {
				fmt.Fprintln(c.stdout, "\nAssets:")
				if latest.ID != "" {
					renderDataWorkspaceAssetsTable(c.stdout, resp.Workspace.Assets, latest.Manifest)
				} else {
					renderDataAssetsTable(c.stdout, resp.Workspace.Assets)
				}
			}
			return nil
		},
	}
}

func (c *CLI) newDataWorkspaceUseCommand() *cobra.Command {
	var replace bool
	cmd := &cobra.Command{
		Use:   "use <workspace>",
		Short: "Bind the current directory to an existing data workspace",
		Example: strings.TrimSpace(`
  fugue data workspace use my-training-project
  fugue data workspace use my-training-project --replace
  fugue data pull
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.GetDataWorkspace(args[0])
			if err != nil {
				return err
			}
			if _, err := os.Stat(dataConfigPath); err == nil && !replace {
				return fmt.Errorf(".fugue/data.yaml already exists; pass --replace to overwrite the binding")
			}
			cfg := dataConfig{Version: 1, Workspace: resp.Workspace.Name, Assets: resp.Workspace.Assets, Ignore: defaultDataIgnore}
			if err := writeDataConfig(".", cfg); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": resp.Workspace, "config": cfg})
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Data workspace", resp.Workspace.Name},
				{"Binding", ".fugue/data.yaml"},
			})
			fmt.Fprintln(c.stdout, "Run: fugue data pull")
			return nil
		},
	}
	cmd.Flags().BoolVar(&replace, "replace", false, "Replace an existing .fugue/data.yaml binding")
	return cmd
}

func (c *CLI) newDataWorkspaceSetBackendCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "set-backend <workspace> <backend>",
		Short: "Set a workspace storage backend",
		Example: strings.TrimSpace(`
  fugue data workspace set-backend my-training-project prod-r2
`),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspace, err := client.PatchDataWorkspace(args[0], map[string]any{"storage_backend_id": args[1]})
			if err != nil {
				return err
			}
			return renderDataWorkspace(c, workspace.Workspace)
		},
	}
}

type dataWorkspaceAccessTargetOptions struct {
	User   string
	Tenant string
	APIKey string
	Role   string
}

func (c *CLI) newDataWorkspaceAccessCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "access",
		Short: "List persistent data workspace access",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "ls <workspace>",
		Short: "List persistent access grants for a data workspace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.GetDataWorkspace(args[0])
			if err != nil {
				return err
			}
			grants, err := client.ListDataWorkspaceAccessGrants(resp.Workspace.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": resp.Workspace, "grants": grants})
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Data workspace", resp.Workspace.Name},
				{"Access grants", strconv.Itoa(len(grants))},
			})
			if len(grants) == 0 {
				return nil
			}
			rows := make([][]string, 0, len(grants))
			for _, grant := range grants {
				rows = append(rows, []string{grant.SubjectType, grant.SubjectID, grant.Role, formatTime(grant.UpdatedAt)})
			}
			renderDataTable(c.stdout, []dataTableColumn{
				{Title: "Subject"},
				{Title: "ID"},
				{Title: "Role"},
				{Title: "Updated"},
			}, rows)
			return nil
		},
	})
	return cmd
}

func (c *CLI) newDataWorkspaceShareCommand() *cobra.Command {
	var opts dataWorkspaceAccessTargetOptions
	cmd := &cobra.Command{
		Use:   "share <workspace>",
		Short: "Grant persistent access to a data workspace",
		Example: strings.TrimSpace(`
  fugue data workspace share saebench-eval-weights --user user@example.com --role reader
  fugue data workspace share saebench-eval-weights --tenant my-workspace --role reader
  fugue data workspace share saebench-eval-weights --api-key apikey_123 --role reader
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.GetDataWorkspace(args[0])
			if err != nil {
				return err
			}
			subjectType, subjectID, label, err := c.resolveDataWorkspaceAccessTarget(client, opts)
			if err != nil {
				return err
			}
			role := normalizeCLIDataWorkspaceAccessRole(opts.Role)
			if role == "" {
				return fmt.Errorf("invalid role %q; use reader, writer, or admin", opts.Role)
			}
			grant, err := client.GrantDataWorkspaceAccess(resp.Workspace.ID, subjectType, subjectID, role)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": resp.Workspace, "grant": grant})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "data_workspace", Value: resp.Workspace.Name},
				kvPair{Key: "subject", Value: label},
				kvPair{Key: "role", Value: grant.Role},
				kvPair{Key: "updated_at", Value: formatTime(grant.UpdatedAt)},
			)
		},
	}
	addDataWorkspaceAccessTargetFlags(cmd, &opts, true)
	return cmd
}

func (c *CLI) newDataWorkspaceUnshareCommand() *cobra.Command {
	var opts dataWorkspaceAccessTargetOptions
	cmd := &cobra.Command{
		Use:     "unshare <workspace>",
		Aliases: []string{"revoke-access"},
		Short:   "Revoke persistent access to a data workspace",
		Example: strings.TrimSpace(`
  fugue data workspace unshare saebench-eval-weights --user user@example.com
  fugue data workspace unshare saebench-eval-weights --tenant my-workspace
  fugue data workspace unshare saebench-eval-weights --api-key apikey_123
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.GetDataWorkspace(args[0])
			if err != nil {
				return err
			}
			subjectType, subjectID, label, err := c.resolveDataWorkspaceAccessTarget(client, opts)
			if err != nil {
				return err
			}
			removed, err := client.RevokeDataWorkspaceAccess(resp.Workspace.ID, subjectType, subjectID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": resp.Workspace, "removed": removed})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "data_workspace", Value: resp.Workspace.Name},
				kvPair{Key: "subject", Value: label},
				kvPair{Key: "removed", Value: fmt.Sprintf("%t", removed)},
			)
		},
	}
	addDataWorkspaceAccessTargetFlags(cmd, &opts, false)
	return cmd
}

func addDataWorkspaceAccessTargetFlags(cmd *cobra.Command, opts *dataWorkspaceAccessTargetOptions, includeRole bool) {
	cmd.Flags().StringVar(&opts.User, "user", "", "Fugue user email for a tenant-level subject")
	cmd.Flags().StringVar(&opts.Tenant, "tenant", "", "Tenant/workspace name or id for a tenant-level subject")
	cmd.Flags().StringVar(&opts.APIKey, "api-key", "", "API key id or label for a token-level subject")
	if includeRole {
		cmd.Flags().StringVar(&opts.Role, "role", model.DataWorkspaceAccessRoleReader, "Access role: reader, writer, or admin")
	}
}

func (c *CLI) resolveDataWorkspaceAccessTarget(client *Client, opts dataWorkspaceAccessTargetOptions) (string, string, string, error) {
	count := 0
	if strings.TrimSpace(opts.User) != "" {
		count++
	}
	if strings.TrimSpace(opts.Tenant) != "" {
		count++
	}
	if strings.TrimSpace(opts.APIKey) != "" {
		count++
	}
	if count != 1 {
		return "", "", "", fmt.Errorf("pass exactly one of --user, --tenant, or --api-key")
	}
	if email := strings.TrimSpace(opts.User); email != "" {
		context, err := client.GetAuthContext()
		if err != nil {
			return "", "", "", fmt.Errorf("verify admin user targeting: %w", err)
		}
		if !context.Principal.PlatformAdmin {
			return "", "", "", fmt.Errorf("--user requires a platform-admin or bootstrap key; use --tenant when you already know the tenant id")
		}
		webClient, err := c.newWebClient("")
		if err != nil {
			return "", "", "", err
		}
		resolved, err := webClient.ResolveAdminWorkspace(email)
		if err != nil {
			return "", "", "", fmt.Errorf("resolve account workspace: %w", err)
		}
		tenantID := strings.TrimSpace(resolved.Workspace.TenantID)
		if tenantID == "" {
			return "", "", "", fmt.Errorf("account %s resolved without a tenant id", email)
		}
		return model.DataWorkspaceAccessSubjectTenant, tenantID, email, nil
	}
	if tenantRef := strings.TrimSpace(opts.Tenant); tenantRef != "" {
		tenant, err := c.resolveNamedTenant(client, tenantRef)
		if err != nil {
			return "", "", "", err
		}
		label := formatDisplayName(firstNonEmptyTrimmed(tenant.Name, tenant.Slug, tenant.ID), tenant.ID, c.showIDs())
		return model.DataWorkspaceAccessSubjectTenant, tenant.ID, label, nil
	}
	key, err := c.resolveNamedAPIKey(client, strings.TrimSpace(opts.APIKey))
	if err != nil {
		return "", "", "", err
	}
	label := formatDisplayName(firstNonEmptyTrimmed(key.Label, key.ID), key.ID, c.showIDs())
	return model.DataWorkspaceAccessSubjectAPIKey, key.ID, label, nil
}

func normalizeCLIDataWorkspaceAccessRole(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.DataWorkspaceAccessRoleReader, "read", "read-only", "readonly":
		return model.DataWorkspaceAccessRoleReader
	case model.DataWorkspaceAccessRoleWriter, "write", "read-write", "readwrite":
		return model.DataWorkspaceAccessRoleWriter
	case model.DataWorkspaceAccessRoleAdmin:
		return model.DataWorkspaceAccessRoleAdmin
	default:
		return ""
	}
}

func (c *CLI) newDataSnapshotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "List and inspect data versions",
		Example: strings.TrimSpace(`
  fugue data snapshot ls
  fugue data snapshot show before-provider-move
  fugue data snapshot diff before-provider-move latest
`),
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "ls",
			Short: "List data versions",
			Example: strings.TrimSpace(`
  fugue data snapshot ls
`),
			Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				cfg, err := readDataConfig(".")
				if err != nil {
					return err
				}
				client, err := c.newClient()
				if err != nil {
					return err
				}
				workspace, err := c.ensureRemoteDataWorkspace(client, cfg)
				if err != nil {
					return err
				}
				snapshots, err := client.ListDataSnapshots(workspace.ID)
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"snapshots": snapshots})
				}
				rows := make([][]string, 0, len(snapshots))
				for _, snapshot := range snapshots {
					rows = append(rows, []string{snapshot.Version, strconv.Itoa(snapshot.FileCount), formatBytes(snapshot.TotalBytes), snapshot.Message})
				}
				renderDataTable(c.stdout, []dataTableColumn{
					{Title: "Version"},
					{Title: "Files", Align: dataTableAlignRight},
					{Title: "Size", Align: dataTableAlignRight},
					{Title: "Message"},
				}, rows)
				return nil
			},
		},
		&cobra.Command{
			Use:   "show <version>",
			Short: "Show a data version",
			Example: strings.TrimSpace(`
  fugue data snapshot show before-provider-move
  fugue data snapshot show latest
`),
			Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				cfg, err := readDataConfig(".")
				if err != nil {
					return err
				}
				client, err := c.newClient()
				if err != nil {
					return err
				}
				workspace, err := c.ensureRemoteDataWorkspace(client, cfg)
				if err != nil {
					return err
				}
				snapshot, err := client.GetDataSnapshot(workspace.ID, args[0])
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, snapshot)
				}
				renderDataKeyValueTable(c.stdout, [][]string{
					{"Version", snapshot.Snapshot.Version},
					{"Files", strconv.Itoa(snapshot.Snapshot.FileCount)},
					{"Size", formatBytes(snapshot.Snapshot.TotalBytes)},
					{"Digest", snapshot.Snapshot.ManifestDigest},
				})
				return nil
			},
		},
	)
	cmd.AddCommand(&cobra.Command{
		Use:   "diff <from-version> <to-version>",
		Short: "Diff two data versions",
		Example: strings.TrimSpace(`
  fugue data snapshot diff before-provider-move latest
`),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workspace, err := c.ensureRemoteDataWorkspace(client, cfg)
			if err != nil {
				return err
			}
			from, err := client.GetDataSnapshot(workspace.ID, args[0])
			if err != nil {
				return err
			}
			to, err := client.GetDataSnapshot(workspace.ID, args[1])
			if err != nil {
				return err
			}
			diff := diffDataManifests(from.Snapshot.Manifest, to.Snapshot.Manifest)
			if c.wantsJSON() {
				return writeJSON(c.stdout, diff)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Added", strconv.Itoa(len(diff["added"]))},
				{"Removed", strconv.Itoa(len(diff["removed"]))},
				{"Changed", strconv.Itoa(len(diff["changed"]))},
			})
			return nil
		},
	})
	return cmd
}

func (c *CLI) newDataGrantCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "grant",
		Short: "Create and revoke data grants",
		Example: strings.TrimSpace(`
  fugue data grant create --version dataset-v3 --read-only --ttl 24h
  fugue data grant create my-training-project --version before-provider-move --read-only --ttl 6h
  fugue data grant revoke grant_123
`),
	}
	var version string
	var ttl time.Duration
	var readOnly bool
	create := &cobra.Command{
		Use:   "create [workspace]",
		Short: "Create a data grant",
		Example: strings.TrimSpace(`
  fugue data grant create --version dataset-v3 --read-only --ttl 24h
  fugue data grant create my-training-project --version before-provider-move --read-only --ttl 6h
`),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil && len(args) == 0 {
				return err
			}
			workspaceName := cfg.Workspace
			if len(args) > 0 {
				workspaceName = args[0]
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.GetDataWorkspace(workspaceName)
			if err != nil {
				return err
			}
			mode := "read-write"
			if readOnly {
				mode = "read-only"
			}
			grant, err := client.CreateDataGrant(resp.Workspace.ID, version, mode, int(ttl.Minutes()))
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, grant)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Grant", "created"},
				{"Secret", fmt.Sprint(grant["secret"])},
			})
			return nil
		},
	}
	create.Flags().StringVar(&version, "version", "", "Version to grant")
	create.Flags().DurationVar(&ttl, "ttl", 24*time.Hour, "Grant TTL")
	create.Flags().BoolVar(&readOnly, "read-only", false, "Create a read-only grant")
	cmd.AddCommand(create)
	cmd.AddCommand(&cobra.Command{
		Use:   "revoke <grant-id>",
		Short: "Revoke a data grant",
		Example: strings.TrimSpace(`
  fugue data grant revoke grant_123
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.RevokeDataGrant(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Grant", args[0]},
				{"Status", "revoked"},
			})
			return nil
		},
	})
	return cmd
}

func (c *CLI) newDataTransferCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "transfer",
		Short: "Inspect and control data transfers",
		Example: strings.TrimSpace(`
  fugue data transfer ls
  fugue data transfer show data_transfer_123
  fugue data transfer watch data_transfer_123
  fugue data transfer resume data_transfer_123
  fugue data transfer cancel data_transfer_123
`),
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "ls",
			Short: "List data transfers",
			Example: strings.TrimSpace(`
  fugue data transfer ls
`),
			Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				client, err := c.newClient()
				if err != nil {
					return err
				}
				transfers, err := client.ListDataTransfers()
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"transfers": transfers})
				}
				rows := make([][]string, 0, len(transfers))
				for _, transfer := range transfers {
					rows = append(rows, []string{transfer.ID, transfer.Direction, transfer.Status, transfer.WorkspaceID})
				}
				renderDataTable(c.stdout, []dataTableColumn{
					{Title: "Transfer"},
					{Title: "Direction"},
					{Title: "Status"},
					{Title: "Workspace"},
				}, rows)
				return nil
			},
		},
		&cobra.Command{
			Use:   "show <transfer-id>",
			Short: "Show a data transfer",
			Example: strings.TrimSpace(`
  fugue data transfer show data_transfer_123
`),
			Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				client, err := c.newClient()
				if err != nil {
					return err
				}
				transfer, err := client.GetDataTransfer(args[0])
				if err != nil {
					return err
				}
				return writeJSON(c.stdout, transfer)
			},
		},
		&cobra.Command{
			Use:   "cancel <transfer-id>",
			Short: "Cancel a data transfer",
			Example: strings.TrimSpace(`
  fugue data transfer cancel data_transfer_123
`),
			Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				client, err := c.newClient()
				if err != nil {
					return err
				}
				resp, err := client.CancelDataTransfer(args[0])
				if err != nil {
					return err
				}
				return writeJSON(c.stdout, resp)
			},
		},
	)
	watch := &cobra.Command{Use: "watch <transfer-id>", Short: "Watch a data transfer", Example: strings.TrimSpace(`
  fugue data transfer watch data_transfer_123
`), Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		lastWidth := 0
		for {
			transfer, err := client.GetDataTransferModel(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, map[string]any{"transfer": transfer}); err != nil {
					return err
				}
			} else {
				percent := 0.0
				if transfer.BytesTotal > 0 {
					percent = float64(transfer.BytesDone) / float64(transfer.BytesTotal) * 100
				}
				line := fmt.Sprintf("%s  %s  %5.1f%%  %s/%s  files %d/%d", transfer.ID, dataProgressBar(percent, 20), percent, formatBytes(transfer.BytesDone), formatBytes(transfer.BytesTotal), transfer.FilesDone, transfer.FilesTotal)
				width := dataDisplayWidth(line)
				padding := ""
				if lastWidth > width {
					padding = strings.Repeat(" ", lastWidth-width)
				}
				fmt.Fprintf(c.stdout, "\r%s%s", line, padding)
				lastWidth = width
			}
			switch transfer.Status {
			case model.DataTransferStatusCompleted, model.DataTransferStatusFailed, model.DataTransferStatusCanceled:
				if !c.wantsJSON() {
					fmt.Fprintln(c.stdout)
				}
				return nil
			}
			<-ticker.C
		}
	}}
	cmd.AddCommand(watch)
	var resumeConcurrency int
	resumeHashConcurrency := defaultDataHashWorkerCount()
	resume := &cobra.Command{Use: "resume <transfer-id>", Short: "Resume a data transfer", Example: strings.TrimSpace(`
  fugue data transfer resume data_transfer_123
`), Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := readDataConfig(".")
		if err != nil {
			return err
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		transfer, err := client.GetDataTransferModel(args[0])
		if err != nil {
			return err
		}
		switch transfer.Direction {
		case model.DataTransferDirectionUpload:
			refresh, err := client.RefreshDataTransferAuthorizationPage(args[0], 0, dataTransferBlobPageLimit)
			if err != nil {
				return err
			}
			estimate, err := estimateDataManifestScan(".", cfg, "")
			if err != nil {
				return err
			}
			scanProgress := newDataProgressRenderer(c.stdout, !c.wantsJSON(), "Hashing local data", estimate.Bytes)
			manifest, pathsByDigest, err := scanDataManifestWithConcurrency(".", cfg, "", scanProgress.advance, resumeHashConcurrency)
			if err != nil {
				return err
			}
			scanProgress.finish()
			manifestDigest := digestDataManifest(manifest)
			if len(refresh.Manifest.Entries) > 0 {
				manifestDigest = digestDataManifest(refresh.Manifest)
			} else if len(refresh.Transfer.Manifest.Entries) > 0 {
				manifestDigest = digestDataManifest(refresh.Transfer.Manifest)
			}
			loadedState, loadedOK, err := loadDataTransferState(".", refresh.Transfer.ID)
			if err != nil {
				return err
			}
			state := uploadResumeStateFromRefresh(refresh, manifestDigest, loadedState, loadedOK)
			if err := saveDataTransferState(".", state); err != nil {
				return err
			}
			if err := c.uploadDataPlanBlobs(client, refresh.Transfer.WorkspaceID, refresh.Transfer.ID, manifestDigest, refresh.Blobs, pathsByDigest, true, false, resumeConcurrency, manifest.TotalBytes, refresh.BlobsOffset, refresh.BlobsLimit, refresh.BlobsNextOffset); err != nil {
				return err
			}
			manifestToComplete := refresh.Manifest
			if len(manifestToComplete.Entries) == 0 {
				manifestToComplete = refresh.Transfer.Manifest
			}
			if len(manifestToComplete.Entries) == 0 {
				manifestToComplete = manifest
			}
			complete, err := client.CompleteDataTransfer(refresh.Transfer.ID, map[string]any{
				"version":    refresh.Transfer.Version,
				"message":    refresh.Transfer.Message,
				"manifest":   manifestToComplete,
				"bytes_done": manifestToComplete.TotalBytes,
				"files_done": manifestToComplete.FileCount,
			})
			if err != nil {
				return err
			}
			_ = removeDataTransferState(".", refresh.Transfer.ID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, complete)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Transfer", refresh.Transfer.ID},
				{"Direction", refresh.Transfer.Direction},
				{"Status", "completed"},
			})
			return nil
		case model.DataTransferDirectionDownload:
			refresh, err := client.RefreshDataTransferAuthorization(args[0])
			if err != nil {
				return err
			}
			manifest := refresh.Manifest
			if len(manifest.Entries) == 0 {
				manifest = refresh.Transfer.Manifest
			}
			if len(manifest.Entries) == 0 {
				return fmt.Errorf("transfer %s does not include a manifest to resume", refresh.Transfer.ID)
			}
			preflightBytes, err := estimatePullPreflightHashBytes(".", cfg, manifest)
			if err != nil {
				return err
			}
			preflightProgress := newDataProgressRenderer(c.stdout, !c.wantsJSON(), "Checking local files", preflightBytes)
			pullPlan, err := buildPullPlanWithConcurrency(".", cfg, manifest, false, false, false, preflightProgress.advance, resumeHashConcurrency)
			if err != nil {
				return err
			}
			preflightProgress.finish()
			if len(pullPlan.Conflicts) > 0 {
				renderPullPreflight(c.stdout, refresh.Workspace, model.DataSnapshot{Version: refresh.Transfer.Version, Manifest: manifest}, pullPlan)
				return fmt.Errorf("pull preflight found conflicts")
			}
			progress := newDataProgressRenderer(c.stdout, !c.wantsJSON(), "Download", totalManifestFileBytes(pullPlan.Download))
			if _, _, err := c.downloadDataManifestEntriesPaged(client, refresh.Transfer.ID, ".", cfg, pullPlan.Download, refresh.Blobs, refresh.BlobsOffset, refresh.BlobsLimit, refresh.BlobsNextOffset, true, false, false, resumeConcurrency, progress); err != nil {
				return err
			}
			progress.finish()
			complete, err := client.CompleteDataTransfer(refresh.Transfer.ID, map[string]any{
				"snapshot_id": refresh.Transfer.SnapshotID,
				"bytes_done":  manifest.TotalBytes,
				"files_done":  manifest.FileCount,
			})
			if err != nil {
				return err
			}
			_ = removeDataTransferState(".", refresh.Transfer.ID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, complete)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Transfer", refresh.Transfer.ID},
				{"Direction", refresh.Transfer.Direction},
				{"Status", "completed"},
			})
			return nil
		default:
			return fmt.Errorf("transfer %s direction %q is not resumable", transfer.ID, transfer.Direction)
		}
	}}
	resume.Flags().IntVar(&resumeConcurrency, "concurrency", defaultDataTransferConcurrency, "Transfer concurrency")
	resume.Flags().IntVar(&resumeHashConcurrency, "hash-concurrency", resumeHashConcurrency, "Local file hashing concurrency")
	cmd.AddCommand(resume)
	return cmd
}

func (c *CLI) newDataBackendCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backend",
		Short: "Manage data storage backends",
		Example: strings.TrimSpace(`
  fugue data backend ls
  fugue data backend create prod-r2 --provider cloudflare-r2 --bucket fugue-data --account-id "$FUGUE_DATA_R2_ACCOUNT_ID"
  fugue data backend migrate my-training-project prod-r2 --cutover --confirm
  fugue data backend rotate-credentials prod-r2
`),
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "ls",
			Short: "List data backends",
			Example: strings.TrimSpace(`
  fugue data backend ls
`),
			Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				client, err := c.newClient()
				if err != nil {
					return err
				}
				backends, err := client.ListDataBackends()
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"backends": backends})
				}
				rows := make([][]string, 0, len(backends))
				for _, backend := range backends {
					rows = append(rows, []string{backend.Name, backend.Provider, backend.Bucket})
				}
				renderDataTable(c.stdout, []dataTableColumn{
					{Title: "Backend"},
					{Title: "Provider"},
					{Title: "Bucket"},
				}, rows)
				return nil
			},
		},
		c.newDataBackendCreateCommand(),
		c.newDataBackendMigrateCommand(),
		c.newDataBackendRollbackCommand(),
		c.newDataBackendRotateCredentialsCommand(),
		&cobra.Command{
			Use:   "show <backend>",
			Short: "Show a data backend",
			Example: strings.TrimSpace(`
  fugue data backend show prod-r2
`),
			Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				client, err := c.newClient()
				if err != nil {
					return err
				}
				backend, err := client.GetDataBackend(args[0])
				if err != nil {
					return err
				}
				return writeJSON(c.stdout, map[string]any{"backend": backend})
			},
		},
		&cobra.Command{
			Use:   "delete <backend>",
			Short: "Delete a data backend",
			Example: strings.TrimSpace(`
  fugue data backend delete old-r2
`),
			Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				client, err := c.newClient()
				if err != nil {
					return err
				}
				resp, err := client.DeleteDataBackend(args[0])
				if err != nil {
					return err
				}
				return writeJSON(c.stdout, resp)
			},
		},
	)
	return cmd
}

func (c *CLI) newDataBackendRotateCredentialsCommand() *cobra.Command {
	var accessKeyID, secretAccessKey, sessionToken string
	cmd := &cobra.Command{
		Use:   "rotate-credentials <backend>",
		Short: "Rotate credentials for a data storage backend",
		Example: strings.TrimSpace(`
  fugue data backend rotate-credentials prod-r2
  fugue data backend rotate-credentials prod-r2 \
    --access-key-id "$FUGUE_DATA_BACKEND_ACCESS_KEY_ID" \
    --secret-access-key "$FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY"
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(accessKeyID) == "" || strings.TrimSpace(secretAccessKey) == "" {
				return fmt.Errorf("--access-key-id and --secret-access-key are required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			backend, err := client.RotateDataBackendCredentials(args[0], model.DataBackendCredentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
				Token:           sessionToken,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backend": backend, "rotated": true})
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Data backend", backend.Name},
				{"Provider", backend.Provider},
				{"Credentials", "rotated"},
			})
			return nil
		},
	}
	cmd.Flags().StringVar(&accessKeyID, "access-key-id", os.Getenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID"), "New backend access key id. Defaults to FUGUE_DATA_BACKEND_ACCESS_KEY_ID")
	cmd.Flags().StringVar(&secretAccessKey, "secret-access-key", os.Getenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY"), "New backend secret access key. Defaults to FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY")
	cmd.Flags().StringVar(&sessionToken, "session-token", os.Getenv("FUGUE_DATA_BACKEND_SESSION_TOKEN"), "New backend session token. Defaults to FUGUE_DATA_BACKEND_SESSION_TOKEN")
	return cmd
}

func (c *CLI) newDataBackendCreateCommand() *cobra.Command {
	var provider, bucket, region, endpoint, accountID, prefix, accessKeyID, secretAccessKey, sessionToken string
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a data storage backend",
		Example: strings.TrimSpace(`
  fugue data backend create prod-r2 \
    --provider cloudflare-r2 \
    --bucket fugue-data \
    --account-id "$FUGUE_DATA_R2_ACCOUNT_ID"

  fugue data backend create my-minio \
    --provider minio \
    --bucket fugue-data \
    --endpoint https://minio.example.com
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if provider == model.DataBackendProviderCloudflareR2 && endpoint == "" && accountID != "" {
				endpoint = "https://" + accountID + ".r2.cloudflarestorage.com"
			}
			credentials := map[string]string{}
			if accessKeyID != "" {
				credentials["access_key_id"] = accessKeyID
			}
			if secretAccessKey != "" {
				credentials["secret_access_key"] = secretAccessKey
			}
			if sessionToken != "" {
				credentials["token"] = sessionToken
			}
			req := map[string]any{"name": args[0], "provider": provider, "bucket": bucket, "region": region, "endpoint": endpoint, "prefix": prefix}
			if len(credentials) > 0 {
				req["credentials"] = credentials
			}
			backend, err := client.CreateDataBackend(req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backend": backend})
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Data backend", backend.Name},
				{"Provider", backend.Provider},
				{"Bucket", backend.Bucket},
			})
			return nil
		},
	}
	cmd.Flags().StringVar(&provider, "provider", model.DataBackendProviderCloudflareR2, "Provider: cloudflare-r2, backblaze-b2, s3, hugging-face, minio")
	cmd.Flags().StringVar(&bucket, "bucket", "", "Storage bucket")
	cmd.Flags().StringVar(&region, "region", "", "Storage region")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "S3-compatible endpoint")
	cmd.Flags().StringVar(&accountID, "account-id", "", "Cloudflare account ID for R2 endpoint derivation")
	cmd.Flags().StringVar(&prefix, "prefix", "", "Object key prefix inside the bucket")
	cmd.Flags().StringVar(&accessKeyID, "access-key-id", os.Getenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID"), "Backend access key id. Defaults to FUGUE_DATA_BACKEND_ACCESS_KEY_ID")
	cmd.Flags().StringVar(&secretAccessKey, "secret-access-key", os.Getenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY"), "Backend secret access key. Defaults to FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY")
	cmd.Flags().StringVar(&sessionToken, "session-token", os.Getenv("FUGUE_DATA_BACKEND_SESSION_TOKEN"), "Backend session token. Defaults to FUGUE_DATA_BACKEND_SESSION_TOKEN")
	return cmd
}

func (c *CLI) newDataBackendMigrateCommand() *cobra.Command {
	var dryRun, cutover, confirm bool
	cmd := &cobra.Command{
		Use:   "migrate <workspace> <target-backend>",
		Short: "Copy live workspace objects to another backend and optionally cut over",
		Example: strings.TrimSpace(`
  fugue data backend migrate my-training-project prod-r2 --dry-run
  fugue data backend migrate my-training-project prod-r2 --cutover --confirm
`),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cutover && !confirm {
				return fmt.Errorf("--cutover requires --confirm")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.CreateDataBackendMigration(args[0], args[1], dryRun, cutover)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			rows := [][]string{
				{"Backend migration", resp.Transfer.Source + " -> " + resp.Transfer.Target},
				{"Status", resp.Transfer.Status},
				{"Objects", fmt.Sprintf("%d/%d", resp.Transfer.FilesDone, resp.Transfer.FilesTotal)},
				{"Bytes", formatBytes(resp.Transfer.BytesDone) + "/" + formatBytes(resp.Transfer.BytesTotal)},
			}
			if cutover && resp.Workspace.StorageBackendID == args[1] {
				rows = append(rows, []string{"Workspace backend", resp.Workspace.StorageBackendID})
			}
			renderDataKeyValueTable(c.stdout, rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Plan migration without copying objects")
	cmd.Flags().BoolVar(&cutover, "cutover", false, "Switch the workspace to the target backend after a successful copy")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Required with --cutover")
	return cmd
}

func (c *CLI) newDataBackendRollbackCommand() *cobra.Command {
	var confirm bool
	cmd := &cobra.Command{
		Use:   "rollback <workspace> <migration-transfer-id>",
		Short: "Roll a workspace backend pointer back to the source backend of a migration",
		Example: strings.TrimSpace(`
  fugue data backend rollback my-training-project data_transfer_123 --confirm
`),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !confirm {
				return fmt.Errorf("backend migration rollback requires --confirm")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.RollbackDataBackendMigration(args[0], args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"Workspace backend", resp.Workspace.StorageBackendID},
				{"Rollback transfer", resp.Transfer.ID},
			})
			return nil
		},
	}
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Required to roll back a backend migration")
	return cmd
}

func (c *CLI) newDataGCCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Inspect and sweep unreferenced data objects",
		Example: strings.TrimSpace(`
  fugue data gc sweep
  fugue data gc sweep my-training-project --retention-days 14
  fugue data gc sweep my-training-project --retention-days 14 --confirm
`),
	}
	var retentionDays int
	var confirm bool
	sweep := &cobra.Command{
		Use:   "sweep [workspace]",
		Short: "Sweep unreferenced data objects after the retention window",
		Example: strings.TrimSpace(`
  fugue data gc sweep
  fugue data gc sweep my-training-project --retention-days 14
  fugue data gc sweep my-training-project --retention-days 14 --confirm
`),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			workspaceID := ""
			if len(args) > 0 {
				workspaceID = args[0]
			} else {
				cfg, err := readDataConfig(".")
				if err != nil {
					return err
				}
				workspaceID = cfg.Workspace
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.SweepDataWorkspaceGC(workspaceID, retentionDays, !confirm, confirm)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			mode := "delete"
			if resp.GC.DryRun {
				mode = "dry-run"
			}
			renderDataKeyValueTable(c.stdout, [][]string{
				{"GC sweep", resp.Workspace.Name},
				{"Mode", mode},
				{"Candidates", strconv.Itoa(len(resp.GC.Candidates))},
				{"Deleted", strconv.Itoa(resp.GC.Deleted)},
				{"Deleted bytes", formatBytes(resp.GC.DeletedBytes)},
			})
			return nil
		},
	}
	sweep.Flags().IntVar(&retentionDays, "retention-days", 7, "Only sweep objects older than this many days")
	sweep.Flags().BoolVar(&confirm, "confirm", false, "Actually delete unreferenced objects. Without this, sweep is dry-run")
	cmd.AddCommand(sweep)
	return cmd
}

func (c *CLI) newDataDoctorCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose local data workspace configuration",
		Example: strings.TrimSpace(`
  fugue data doctor
  fugue data doctor --json
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			var warnings []string
			for _, asset := range cfg.Assets {
				if _, err := os.Lstat(asset.Path); err != nil {
					warnings = append(warnings, fmt.Sprintf("%s: %v", asset.Path, err))
				}
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"config": cfg, "warnings": warnings})
			}
			rows := [][]string{{"Data workspace", cfg.Workspace}}
			if len(warnings) == 0 {
				rows = append(rows, []string{"Status", "ok"})
			} else {
				rows = append(rows, []string{"Status", "warning"})
				renderDataKeyValueTable(c.stdout, rows)
				fmt.Fprintln(c.stdout, "\nWarnings:")
				warningRows := make([][]string, 0, len(warnings))
				for _, warning := range warnings {
					warningRows = append(warningRows, []string{warning})
				}
				renderDataTable(c.stdout, []dataTableColumn{{Title: "Warning"}}, warningRows)
				return nil
			}
			renderDataKeyValueTable(c.stdout, rows)
			return nil
		},
	}
}

func (c *CLI) ensureRemoteDataWorkspace(client *Client, cfg dataConfig) (model.DataWorkspace, error) {
	if strings.TrimSpace(cfg.Workspace) == "" {
		return model.DataWorkspace{}, fmt.Errorf("data workspace is missing from .fugue/data.yaml")
	}
	resp, err := client.GetDataWorkspace(cfg.Workspace)
	if err == nil {
		if !dataAssetsEqual(resp.Workspace.Assets, cfg.Assets) {
			updated, patchErr := client.PatchDataWorkspace(resp.Workspace.ID, map[string]any{"assets": cfg.Assets})
			if patchErr != nil {
				return model.DataWorkspace{}, patchErr
			}
			return updated.Workspace, nil
		}
		return resp.Workspace, nil
	}
	workspace, createErr := client.CreateDataWorkspace(map[string]any{"name": cfg.Workspace, "assets": cfg.Assets})
	if createErr != nil {
		return model.DataWorkspace{}, err
	}
	return workspace, nil
}

func ensureDataConfig(root, workspaceName, projectName string, detect bool) (dataConfig, bool, error) {
	if cfg, err := readDataConfig(root); err == nil {
		return cfg, false, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return dataConfig{}, false, err
	}
	if workspaceName == "" {
		workspaceName = filepath.Base(cwd)
	}
	cfg := dataConfig{
		Version:   1,
		Workspace: workspaceName,
		Project:   projectName,
		Ignore:    append([]string(nil), defaultDataIgnore...),
	}
	if detect {
		for _, name := range []string{"data", "datasets", "checkpoints", "ckpt", "outputs", "runs", "wandb", "logs", "cache", "features"} {
			if info, err := os.Lstat(filepath.Join(root, name)); err == nil && (info.IsDir() || info.Mode().IsRegular()) {
				cfg.Assets = append(cfg.Assets, model.NormalizeDataAsset(model.DataAsset{Name: assetNameFromPath(name), Path: "./" + name, Required: true}))
			}
		}
	}
	if err := writeDataConfig(root, cfg); err != nil {
		return dataConfig{}, false, err
	}
	return cfg, true, nil
}

func readDataConfig(root string) (dataConfig, error) {
	raw, err := os.ReadFile(filepath.Join(root, dataConfigPath))
	if err != nil {
		return dataConfig{}, err
	}
	var cfg dataConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return dataConfig{}, err
	}
	if cfg.Version == 0 {
		cfg.Version = 1
	}
	cfg.Workspace = strings.TrimSpace(cfg.Workspace)
	cfg.Ignore = append([]string(nil), cfg.Ignore...)
	cfg.Assets = normalizeConfigAssets(cfg.Assets)
	if cfg.Workspace == "" {
		return dataConfig{}, fmt.Errorf("workspace is required in %s", dataConfigPath)
	}
	return cfg, nil
}

func writeDataConfig(root string, cfg dataConfig) error {
	cfg.Assets = normalizeConfigAssets(cfg.Assets)
	if cfg.Version == 0 {
		cfg.Version = 1
	}
	if len(cfg.Ignore) == 0 {
		cfg.Ignore = append([]string(nil), defaultDataIgnore...)
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	target := filepath.Join(root, dataConfigPath)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	return os.WriteFile(target, raw, 0o644)
}

func cleanConfigPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("path is required")
	}
	if filepath.IsAbs(raw) {
		return "", fmt.Errorf("data asset paths must be relative; use pull --to for absolute restore targets")
	}
	clean := filepath.Clean(raw)
	if clean == "." || strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("data asset path %q escapes the project", raw)
	}
	if !strings.HasPrefix(clean, ".") {
		clean = "." + string(os.PathSeparator) + clean
	}
	return filepath.ToSlash(clean), nil
}

func assetNameFromPath(raw string) string {
	clean := strings.Trim(strings.TrimSpace(filepath.ToSlash(raw)), "/")
	clean = strings.TrimPrefix(clean, "./")
	name := path.Base(clean)
	if name == "." || name == "/" || name == "" {
		name = "data"
	}
	return model.Slugify(name)
}

func normalizeConfigAssets(assets []model.DataAsset) []model.DataAsset {
	out := make([]model.DataAsset, 0, len(assets))
	seen := map[string]struct{}{}
	for _, asset := range assets {
		asset = model.NormalizeDataAsset(asset)
		if asset.Name == "" || asset.Path == "" {
			continue
		}
		if _, exists := seen[asset.Name]; exists {
			continue
		}
		seen[asset.Name] = struct{}{}
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sortDataConfigAssets(cfg *dataConfig) {
	cfg.Assets = normalizeConfigAssets(cfg.Assets)
}

func dataConfigAssetIndex(cfg dataConfig, name string) int {
	for idx, asset := range cfg.Assets {
		if asset.Name == name {
			return idx
		}
	}
	return -1
}

func selectDataPullAssets(cfg dataConfig, assetFlag string, args []string) ([]model.DataAsset, error) {
	assetFlag = strings.TrimSpace(assetFlag)
	if assetFlag != "" && len(args) > 0 {
		return nil, fmt.Errorf("use either --asset or positional asset selectors, not both")
	}
	if assetFlag != "" {
		return selectDataAssets(cfg, []string{assetFlag})
	}
	if len(args) == 0 {
		return nil, nil
	}
	return selectDataAssets(cfg, args)
}

func selectDataEvictAssets(cfg dataConfig, selectors []string) ([]model.DataAsset, error) {
	if len(selectors) == 0 {
		return append([]model.DataAsset(nil), cfg.Assets...), nil
	}
	return selectDataAssets(cfg, selectors)
}

func selectDataAssets(cfg dataConfig, selectors []string) ([]model.DataAsset, error) {
	selected := []model.DataAsset{}
	seen := map[string]struct{}{}
	for _, selector := range selectors {
		asset, err := resolveDataAssetSelector(cfg, selector)
		if err != nil {
			return nil, err
		}
		if asset.Name == "" {
			continue
		}
		if _, ok := seen[asset.Name]; ok {
			continue
		}
		seen[asset.Name] = struct{}{}
		selected = append(selected, asset)
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no assets selected")
	}
	return selected, nil
}

func resolveDataAssetSelector(cfg dataConfig, raw string) (model.DataAsset, error) {
	selector := strings.TrimSpace(filepath.ToSlash(raw))
	if selector == "" {
		return model.DataAsset{}, nil
	}
	if matches := dataAssetSelectorMatches(cfg, selector, dataAssetSelectorNameMatch); len(matches) > 0 {
		return matches[0], nil
	}
	if matches := dataAssetSelectorMatches(cfg, selector, dataAssetSelectorPathMatch); len(matches) > 0 {
		if len(matches) > 1 {
			return model.DataAsset{}, ambiguousDataAssetSelectorError(selector, matches)
		}
		return matches[0], nil
	}
	if matches := dataAssetSelectorMatches(cfg, selector, dataAssetSelectorBasenameMatch); len(matches) > 0 {
		if len(matches) > 1 {
			return model.DataAsset{}, ambiguousDataAssetSelectorError(selector, matches)
		}
		return matches[0], nil
	}
	return model.DataAsset{}, fmt.Errorf("asset %q is not tracked", raw)
}

type dataAssetSelectorMatchMode int

const (
	dataAssetSelectorNameMatch dataAssetSelectorMatchMode = iota
	dataAssetSelectorPathMatch
	dataAssetSelectorBasenameMatch
)

func dataAssetSelectorMatches(cfg dataConfig, selector string, mode dataAssetSelectorMatchMode) []model.DataAsset {
	cleanPath := ""
	if cleaned, err := cleanConfigPath(selector); err == nil {
		cleanPath = cleaned
	}
	matches := []model.DataAsset{}
	for _, asset := range cfg.Assets {
		assetPath := filepath.ToSlash(asset.Path)
		switch mode {
		case dataAssetSelectorNameMatch:
			if asset.Name == selector {
				matches = append(matches, asset)
			}
		case dataAssetSelectorPathMatch:
			if assetPath == selector || (cleanPath != "" && assetPath == cleanPath) {
				matches = append(matches, asset)
			}
		case dataAssetSelectorBasenameMatch:
			if path.Base(strings.TrimPrefix(assetPath, "./")) == selector {
				matches = append(matches, asset)
			}
		}
	}
	return matches
}

func ambiguousDataAssetSelectorError(selector string, matches []model.DataAsset) error {
	labels := make([]string, 0, len(matches))
	for _, asset := range matches {
		labels = append(labels, fmt.Sprintf("%s (%s)", asset.Name, asset.Path))
	}
	sort.Strings(labels)
	return fmt.Errorf("asset selector %q is ambiguous; use an asset name or tracked path: %s", selector, strings.Join(labels, ", "))
}

func estimateDataManifestScan(root string, cfg dataConfig, onlyAsset string) (dataScanEstimate, error) {
	return estimateDataManifestScanWithMissing(root, cfg, onlyAsset, false)
}

func estimateDataManifestScanAllowMissing(root string, cfg dataConfig, onlyAsset string) (dataScanEstimate, error) {
	return estimateDataManifestScanWithMissing(root, cfg, onlyAsset, true)
}

func estimateDataManifestScanWithMissing(root string, cfg dataConfig, onlyAsset string, allowMissing bool) (dataScanEstimate, error) {
	var estimate dataScanEstimate
	for _, asset := range cfg.Assets {
		if onlyAsset != "" && asset.Name != onlyAsset {
			continue
		}
		assetPath := filepath.Join(root, filepath.FromSlash(asset.Path))
		info, err := os.Lstat(assetPath)
		if err != nil {
			if asset.Required && !allowMissing {
				return dataScanEstimate{}, err
			}
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if info.IsDir() {
			err := filepath.WalkDir(assetPath, func(current string, dirEntry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				rel, err := filepath.Rel(assetPath, current)
				if err != nil {
					return err
				}
				relSlash := filepath.ToSlash(rel)
				if relSlash == "." {
					return nil
				}
				if shouldIgnoreDataPath(relSlash, dirEntry.Name(), append(cfg.Ignore, asset.Ignore...)) {
					if dirEntry.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				info, err := dirEntry.Info()
				if err != nil {
					return err
				}
				if info.Mode().IsRegular() {
					estimate.Files++
					estimate.Bytes += info.Size()
				}
				return nil
			})
			if err != nil {
				return dataScanEstimate{}, err
			}
			continue
		}
		if info.Mode().IsRegular() {
			estimate.Files++
			estimate.Bytes += info.Size()
		}
	}
	return estimate, nil
}

func scanDataManifest(root string, cfg dataConfig, onlyAsset string) (model.DataManifest, map[string]string, error) {
	return scanDataManifestWithProgress(root, cfg, onlyAsset, nil)
}

func scanDataManifestWithProgress(root string, cfg dataConfig, onlyAsset string, progress dataTransferProgress) (model.DataManifest, map[string]string, error) {
	return scanDataManifestWithConcurrency(root, cfg, onlyAsset, progress, defaultDataHashWorkerCount())
}

type dataManifestHashJob struct {
	EntryIndex int
	Path       string
	Info       os.FileInfo
}

type dataManifestHashResult struct {
	EntryIndex int
	Path       string
	SHA256     string
	CacheEntry dataHashCacheEntry
	Err        error
}

func scanDataManifestWithConcurrency(root string, cfg dataConfig, onlyAsset string, progress dataTransferProgress, hashConcurrency int) (model.DataManifest, map[string]string, error) {
	manifest, pathsByDigest, _, err := scanDataManifestWithConcurrencyAndMissing(root, cfg, onlyAsset, progress, hashConcurrency, false)
	return manifest, pathsByDigest, err
}

func scanDataManifestWithConcurrencyAllowMissing(root string, cfg dataConfig, onlyAsset string, progress dataTransferProgress, hashConcurrency int) (model.DataManifest, map[string]string, []string, error) {
	return scanDataManifestWithConcurrencyAndMissing(root, cfg, onlyAsset, progress, hashConcurrency, true)
}

func scanDataManifestWithConcurrencyAndMissing(root string, cfg dataConfig, onlyAsset string, progress dataTransferProgress, hashConcurrency int, allowMissing bool) (model.DataManifest, map[string]string, []string, error) {
	entries := []model.DataManifestEntry{}
	pathsByDigest := map[string]string{}
	missingAssets := []string{}
	cache, _ := loadDataHashCache(root)
	cacheLookup := newDataHashCacheLookup(cache)
	nextCacheEntries := []dataHashCacheEntry{}
	hashJobs := []dataManifestHashJob{}
	for _, asset := range cfg.Assets {
		if onlyAsset != "" && asset.Name != onlyAsset {
			continue
		}
		assetPath := filepath.Join(root, filepath.FromSlash(asset.Path))
		info, err := os.Lstat(assetPath)
		if err != nil {
			if asset.Required && !allowMissing {
				return model.DataManifest{}, nil, nil, err
			}
			if asset.Required {
				missingAssets = append(missingAssets, asset.Name)
				continue
			}
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, _ := os.Readlink(assetPath)
			entries = append(entries, model.DataManifestEntry{AssetName: asset.Name, RelativePath: ".", Kind: model.DataManifestEntryKindSymlink, Mode: int64(info.Mode()), MTime: info.ModTime().UTC(), LinkTarget: target})
			continue
		}
		if info.IsDir() {
			err := filepath.WalkDir(assetPath, func(current string, dirEntry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				rel, err := filepath.Rel(assetPath, current)
				if err != nil {
					return err
				}
				relSlash := filepath.ToSlash(rel)
				if relSlash == "." {
					entries = append(entries, model.DataManifestEntry{AssetName: asset.Name, RelativePath: ".", Kind: model.DataManifestEntryKindDir, Mode: int64(info.Mode()), MTime: info.ModTime().UTC()})
					return nil
				}
				if shouldIgnoreDataPath(relSlash, dirEntry.Name(), append(cfg.Ignore, asset.Ignore...)) {
					if dirEntry.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				info, err := dirEntry.Info()
				if err != nil {
					return err
				}
				entry := model.DataManifestEntry{AssetName: asset.Name, RelativePath: relSlash, Mode: int64(info.Mode()), MTime: info.ModTime().UTC()}
				if info.Mode()&os.ModeSymlink != 0 {
					target, _ := os.Readlink(current)
					entry.Kind = model.DataManifestEntryKindSymlink
					entry.LinkTarget = target
				} else if info.IsDir() {
					entry.Kind = model.DataManifestEntryKindDir
				} else if info.Mode().IsRegular() {
					entry.Kind = model.DataManifestEntryKindFile
					entry.Size = info.Size()
				} else {
					return nil
				}
				entries = append(entries, entry)
				if entry.Kind == model.DataManifestEntryKindFile {
					hashJobs = append(hashJobs, dataManifestHashJob{EntryIndex: len(entries) - 1, Path: current, Info: info})
				}
				return nil
			})
			if err != nil {
				return model.DataManifest{}, nil, nil, err
			}
			continue
		}
		if info.Mode().IsRegular() {
			entry := model.DataManifestEntry{AssetName: asset.Name, RelativePath: ".", Kind: model.DataManifestEntryKindFile, Size: info.Size(), Mode: int64(info.Mode()), MTime: info.ModTime().UTC()}
			entries = append(entries, entry)
			hashJobs = append(hashJobs, dataManifestHashJob{EntryIndex: len(entries) - 1, Path: assetPath, Info: info})
		}
	}
	hashResults, err := hashDataManifestFiles(root, cacheLookup, hashJobs, progress, hashConcurrency)
	if err != nil {
		return model.DataManifest{}, nil, nil, err
	}
	for _, result := range hashResults {
		entries[result.EntryIndex].SHA256 = result.SHA256
		entries[result.EntryIndex].ObjectKey = model.DataObjectKey(result.SHA256)
		pathsByDigest[result.SHA256] = result.Path
		nextCacheEntries = append(nextCacheEntries, result.CacheEntry)
	}
	_ = saveDataHashCache(root, cfg, onlyAsset, cache, nextCacheEntries)
	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: entries})
	return manifest, pathsByDigest, missingAssets, nil
}

func hashDataManifestFiles(root string, cache dataHashCacheLookup, jobs []dataManifestHashJob, progress dataTransferProgress, concurrency int) ([]dataManifestHashResult, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	workerCount := normalizeDataConcurrency(concurrency, defaultDataHashWorkerCount(), len(jobs))
	results := make([]dataManifestHashResult, len(jobs))
	if workerCount <= 1 {
		for idx, job := range jobs {
			sum, cacheEntry, err := sha256LocalFileWithCache(root, job.Path, job.Info, cache, progress)
			if err != nil {
				return nil, err
			}
			results[idx] = dataManifestHashResult{EntryIndex: job.EntryIndex, Path: job.Path, SHA256: sum, CacheEntry: cacheEntry}
		}
		return results, nil
	}
	jobCh := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobCh {
			job := jobs[idx]
			sum, cacheEntry, err := sha256LocalFileWithCache(root, job.Path, job.Info, cache, progress)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			results[idx] = dataManifestHashResult{EntryIndex: job.EntryIndex, Path: job.Path, SHA256: sum, CacheEntry: cacheEntry}
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for idx := range jobs {
		select {
		case err := <-errCh:
			close(jobCh)
			wg.Wait()
			return nil, err
		case jobCh <- idx:
		}
	}
	close(jobCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
		return results, nil
	}
}

func shouldIgnoreDataPath(rel, base string, patterns []string) bool {
	rel = filepath.ToSlash(rel)
	if base == ".fugue" || rel == ".fugue" || strings.HasPrefix(rel, ".fugue/") {
		return true
	}
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(filepath.ToSlash(pattern))
		if pattern == "" {
			continue
		}
		if pattern == rel || pattern == base {
			return true
		}
		if ok, _ := path.Match(pattern, rel); ok {
			return true
		}
		if ok, _ := path.Match(pattern, base); ok {
			return true
		}
	}
	return false
}

func loadDataHashCache(root string) (dataHashCache, error) {
	cachePath := filepath.Join(root, filepath.FromSlash(dataHashCachePath))
	raw, err := os.ReadFile(cachePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return dataHashCache{Version: dataHashCacheVersion}, nil
		}
		return dataHashCache{Version: dataHashCacheVersion}, err
	}
	var cache dataHashCache
	if err := yaml.Unmarshal(raw, &cache); err != nil {
		return dataHashCache{Version: dataHashCacheVersion}, err
	}
	if cache.Version == 0 {
		cache.Version = dataHashCacheVersion
	}
	return cache, nil
}

func saveDataHashCache(root string, cfg dataConfig, onlyAsset string, previous dataHashCache, scanned []dataHashCacheEntry) error {
	cachePath := filepath.Join(root, filepath.FromSlash(dataHashCachePath))
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		return err
	}
	byPath := map[string]dataHashCacheEntry{}
	for _, entry := range previous.Entries {
		entry.Path = normalizeDataCachePath(entry.Path)
		if entry.Path == "" || dataCacheEntryInScanScope(entry.Path, cfg, onlyAsset) {
			continue
		}
		byPath[entry.Path] = entry
	}
	for _, entry := range scanned {
		entry.Path = normalizeDataCachePath(entry.Path)
		if entry.Path == "" || strings.TrimSpace(entry.SHA256) == "" {
			continue
		}
		byPath[entry.Path] = entry
	}
	entries := make([]dataHashCacheEntry, 0, len(byPath))
	for _, entry := range byPath {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	raw, err := yaml.Marshal(dataHashCache{Version: dataHashCacheVersion, Entries: entries})
	if err != nil {
		return err
	}
	return os.WriteFile(cachePath, raw, dataHashCachePerm)
}

func newDataHashCacheLookup(cache dataHashCache) dataHashCacheLookup {
	lookup := dataHashCacheLookup{
		byPath:     map[string]dataHashCacheEntry{},
		byIdentity: map[string]dataHashCacheEntry{},
	}
	for _, entry := range cache.Entries {
		entry.Path = normalizeDataCachePath(entry.Path)
		if entry.Path == "" || strings.TrimSpace(entry.SHA256) == "" {
			continue
		}
		lookup.byPath[entry.Path] = entry
		if key := dataHashCacheIdentityKey(entry.Device, entry.Inode, entry.Size, entry.MTimeUnixNano); key != "" {
			lookup.byIdentity[key] = entry
		}
	}
	return lookup
}

func sha256LocalFileWithCache(root, filePath string, info os.FileInfo, cache dataHashCacheLookup, progress dataTransferProgress) (string, dataHashCacheEntry, error) {
	cachePath, err := dataCachePathForFile(root, filePath)
	if err != nil {
		return "", dataHashCacheEntry{}, err
	}
	identity := dataFileIdentity(info)
	baseEntry := dataHashCacheEntry{
		Path:          cachePath,
		Size:          info.Size(),
		MTimeUnixNano: info.ModTime().UnixNano(),
		Device:        identity.Device,
		Inode:         identity.Inode,
	}
	if cached, ok := cache.byPath[cachePath]; ok && dataHashCacheEntryMatchesFile(cached, baseEntry, true) {
		if progress != nil {
			progress(info.Size())
		}
		cached.Path = cachePath
		return cached.SHA256, cached, nil
	}
	if key := dataHashCacheIdentityKey(identity.Device, identity.Inode, info.Size(), info.ModTime().UnixNano()); key != "" {
		if cached, ok := cache.byIdentity[key]; ok && dataHashCacheEntryMatchesFile(cached, baseEntry, false) {
			if progress != nil {
				progress(info.Size())
			}
			cached.Path = cachePath
			return cached.SHA256, cached, nil
		}
	}
	sum, err := sha256LocalFileWithProgress(filePath, progress)
	if err != nil {
		return "", dataHashCacheEntry{}, err
	}
	baseEntry.SHA256 = sum
	baseEntry.ComputedAt = dataHashCacheNow().UTC().Format(time.RFC3339Nano)
	return sum, baseEntry, nil
}

func dataHashCacheEntryMatchesFile(cached, current dataHashCacheEntry, requirePath bool) bool {
	if strings.TrimSpace(cached.SHA256) == "" {
		return false
	}
	if requirePath && normalizeDataCachePath(cached.Path) != normalizeDataCachePath(current.Path) {
		return false
	}
	return cached.Size == current.Size &&
		cached.MTimeUnixNano == current.MTimeUnixNano &&
		cached.Device == current.Device &&
		cached.Inode == current.Inode
}

func dataCachePathForFile(root, filePath string) (string, error) {
	rel, err := filepath.Rel(root, filePath)
	if err != nil {
		return "", err
	}
	return normalizeDataCachePath(rel), nil
}

func normalizeDataCachePath(value string) string {
	value = strings.TrimSpace(filepath.ToSlash(value))
	if value == "" || value == "." {
		return ""
	}
	value = path.Clean(value)
	if strings.HasPrefix(value, "../") || value == ".." {
		return ""
	}
	value = strings.TrimPrefix(value, "./")
	return "./" + value
}

func dataHashCacheIdentityKey(device, inode uint64, size, mtimeUnixNano int64) string {
	if device == 0 && inode == 0 {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d:%d", device, inode, size, mtimeUnixNano)
}

func dataCacheEntryInScanScope(cachePath string, cfg dataConfig, onlyAsset string) bool {
	onlyAsset = strings.TrimSpace(onlyAsset)
	if onlyAsset != "" {
		return dataCacheEntryInAsset(cachePath, cfg, onlyAsset)
	}
	for _, asset := range cfg.Assets {
		if dataCacheEntryInAsset(cachePath, dataConfig{Assets: []model.DataAsset{asset}}, asset.Name) {
			return true
		}
	}
	return false
}

func dataCacheEntryInAsset(cachePath string, cfg dataConfig, assetName string) bool {
	cachePath = normalizeDataCachePath(cachePath)
	if cachePath == "" {
		return false
	}
	for _, asset := range cfg.Assets {
		if asset.Name != assetName {
			continue
		}
		assetPath := normalizeDataCachePath(asset.Path)
		if assetPath == "" {
			continue
		}
		return cachePath == assetPath || strings.HasPrefix(cachePath, strings.TrimSuffix(assetPath, "/")+"/")
	}
	return false
}

func dataUntrackedLargeDirThreshold() int64 {
	raw := strings.TrimSpace(os.Getenv("FUGUE_DATA_LARGE_DIR_THRESHOLD_BYTES"))
	if raw == "" {
		return defaultUntrackedLargeDirThreshold
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return defaultUntrackedLargeDirThreshold
	}
	return value
}

func findUntrackedLargeDataDirectories(root string, cfg dataConfig, threshold int64) ([]untrackedDataDirectory, error) {
	if threshold <= 0 {
		threshold = defaultUntrackedLargeDirThreshold
	}
	tracked := map[string]struct{}{}
	for _, asset := range cfg.Assets {
		clean, err := cleanConfigPath(asset.Path)
		if err != nil {
			continue
		}
		tracked[filepath.Clean(filepath.Join(root, filepath.FromSlash(clean)))] = struct{}{}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	type untrackedDirJob struct {
		name     string
		fullPath string
	}
	jobs := []untrackedDirJob{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		rel := filepath.ToSlash(name)
		if shouldIgnoreDataPath(rel, name, cfg.Ignore) || strings.HasPrefix(name, ".") {
			continue
		}
		fullPath := filepath.Clean(filepath.Join(root, name))
		if _, ok := tracked[fullPath]; ok {
			continue
		}
		jobs = append(jobs, untrackedDirJob{name: name, fullPath: fullPath})
	}
	var out []untrackedDataDirectory
	if len(jobs) == 0 {
		return out, nil
	}
	workerCount := normalizeDataConcurrency(defaultDataHashWorkerCount(), defaultDataHashConcurrency, len(jobs))
	if workerCount <= 1 {
		for _, job := range jobs {
			dir, err := scanUntrackedLargeDataDirectory(job.fullPath, job.name, cfg, threshold)
			if err != nil {
				return nil, err
			}
			if dir.Bytes >= threshold {
				out = append(out, dir)
			}
		}
	} else {
		jobCh := make(chan untrackedDirJob)
		errCh := make(chan error, 1)
		var mu sync.Mutex
		var wg sync.WaitGroup
		worker := func() {
			defer wg.Done()
			for job := range jobCh {
				dir, err := scanUntrackedLargeDataDirectory(job.fullPath, job.name, cfg, threshold)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if dir.Bytes >= threshold {
					mu.Lock()
					out = append(out, dir)
					mu.Unlock()
				}
			}
		}
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go worker()
		}
		for _, job := range jobs {
			select {
			case err := <-errCh:
				close(jobCh)
				wg.Wait()
				return nil, err
			case jobCh <- job:
			}
		}
		close(jobCh)
		wg.Wait()
		select {
		case err := <-errCh:
			return nil, err
		default:
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bytes == out[j].Bytes {
			return out[i].Path < out[j].Path
		}
		return out[i].Bytes > out[j].Bytes
	})
	return out, nil
}

func scanUntrackedLargeDataDirectory(fullPath, name string, cfg dataConfig, threshold int64) (untrackedDataDirectory, error) {
	var bytes int64
	err := filepath.WalkDir(fullPath, func(current string, dirEntry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if dirEntry.IsDir() {
			if current != fullPath && shouldIgnoreDataPath(filepath.ToSlash(strings.TrimPrefix(current, fullPath+string(os.PathSeparator))), dirEntry.Name(), cfg.Ignore) {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			bytes += info.Size()
		}
		return nil
	})
	if err != nil {
		return untrackedDataDirectory{}, err
	}
	if bytes < threshold {
		return untrackedDataDirectory{}, nil
	}
	return untrackedDataDirectory{Path: "./" + filepath.ToSlash(name), Bytes: bytes}, nil
}

func sha256LocalFile(filePath string) (string, error) {
	return sha256LocalFileWithProgress(filePath, nil)
}

type dataProgressReader struct {
	r        io.Reader
	progress dataTransferProgress
}

func (r dataProgressReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 && r.progress != nil {
		r.progress(int64(n))
	}
	return n, err
}

func sha256LocalFileWithProgress(filePath string, progress dataTransferProgress) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := sha256.New()
	reader := io.Reader(file)
	if progress != nil {
		reader = dataProgressReader{r: file, progress: progress}
	}
	if _, err := io.Copy(hasher, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func buildPullPlan(root string, cfg dataConfig, manifest model.DataManifest, overwrite, keepLocal, prune bool) (pullPlan, error) {
	return buildPullPlanWithProgress(root, cfg, manifest, overwrite, keepLocal, prune, nil)
}

func buildPullPlanWithProgress(root string, cfg dataConfig, manifest model.DataManifest, overwrite, keepLocal, prune bool, progress dataTransferProgress) (pullPlan, error) {
	return buildPullPlanWithConcurrency(root, cfg, manifest, overwrite, keepLocal, prune, progress, defaultDataHashWorkerCount())
}

type pullPlanHashJob struct {
	Index  int
	Entry  model.DataManifestEntry
	Target string
}

type pullPlanHashResult struct {
	Index  int
	Entry  model.DataManifestEntry
	Target string
	SHA256 string
	Err    error
}

func buildPullPlanWithConcurrency(root string, cfg dataConfig, manifest model.DataManifest, overwrite, keepLocal, prune bool, progress dataTransferProgress, hashConcurrency int) (pullPlan, error) {
	var plan pullPlan
	expectedByAsset := map[string]map[string]struct{}{}
	hashJobs := []pullPlanHashJob{}
	hashJobIndex := 0
	for _, entry := range manifest.Entries {
		target, err := targetPathForEntry(root, cfg, entry)
		if err != nil {
			return plan, err
		}
		if expectedByAsset[entry.AssetName] == nil {
			expectedByAsset[entry.AssetName] = map[string]struct{}{}
		}
		expectedByAsset[entry.AssetName][target] = struct{}{}
		info, err := os.Lstat(target)
		if err != nil {
			if os.IsNotExist(err) {
				plan.Download = append(plan.Download, entry)
				continue
			}
			if strings.Contains(err.Error(), "not a directory") {
				plan.Conflicts = append(plan.Conflicts, pullConflict{"type", target, "parent path is a file, remote asset requires a directory"})
				continue
			}
			return plan, err
		}
		if entry.Kind == model.DataManifestEntryKindDir {
			if !info.IsDir() {
				plan.Conflicts = append(plan.Conflicts, pullConflict{"type", target, "remote asset is a directory, local path is a file"})
			} else {
				plan.Skip = append(plan.Skip, entry)
			}
			continue
		}
		if info.IsDir() && entry.Kind == model.DataManifestEntryKindFile {
			plan.Conflicts = append(plan.Conflicts, pullConflict{"type", target, "remote asset is a file, local path is a directory"})
			continue
		}
		if entry.Kind == model.DataManifestEntryKindFile && info.Mode().IsRegular() {
			hashJobs = append(hashJobs, pullPlanHashJob{Index: hashJobIndex, Entry: entry, Target: target})
			hashJobIndex++
			continue
		}
		if entry.Kind == model.DataManifestEntryKindSymlink {
			if info.Mode()&os.ModeSymlink == 0 && !overwrite {
				plan.Conflicts = append(plan.Conflicts, pullConflict{"type", target, "remote asset is a symlink, local path is not a symlink"})
			} else if localTarget, err := os.Readlink(target); err == nil && localTarget == entry.LinkTarget {
				plan.Skip = append(plan.Skip, entry)
			} else if overwrite {
				plan.Download = append(plan.Download, entry)
			} else if keepLocal {
				plan.Warnings = append(plan.Warnings, pullConflict{"changed", target, "symlink target differs from version manifest, keeping local symlink"})
			} else {
				plan.Conflicts = append(plan.Conflicts, pullConflict{"changed", target, "symlink target differs from version manifest"})
			}
		}
	}
	hashResults, err := hashPullPlanFiles(hashJobs, progress, hashConcurrency)
	if err != nil {
		return plan, err
	}
	for _, result := range hashResults {
		if result.SHA256 == result.Entry.SHA256 {
			plan.Skip = append(plan.Skip, result.Entry)
		} else if overwrite {
			plan.Download = append(plan.Download, result.Entry)
		} else if keepLocal {
			plan.Warnings = append(plan.Warnings, pullConflict{"changed", result.Target, "checksum differs from version manifest, keeping local file"})
		} else {
			plan.Conflicts = append(plan.Conflicts, pullConflict{"changed", result.Target, "checksum differs from version manifest"})
		}
	}
	for _, asset := range cfg.Assets {
		expected := expectedByAsset[asset.Name]
		if expected == nil {
			continue
		}
		rootPath := filepath.Join(root, filepath.FromSlash(asset.Path))
		info, err := os.Lstat(rootPath)
		if err != nil || !info.IsDir() {
			continue
		}
		_ = filepath.WalkDir(rootPath, func(current string, dirEntry os.DirEntry, err error) error {
			if err != nil || dirEntry.IsDir() {
				return nil
			}
			if _, ok := expected[current]; !ok {
				if prune {
					plan.Prune = append(plan.Prune, current)
				} else {
					plan.Warnings = append(plan.Warnings, pullConflict{"extra", current, "local file is not in version manifest, preserved by default"})
				}
			}
			return nil
		})
	}
	if err := checkPullDiskSpace(root, plan.Download); err != nil {
		return plan, err
	}
	if err := checkPullWriteAccess(root, cfg, plan.Download, plan.Prune); err != nil {
		return plan, err
	}
	return plan, nil
}

func hashPullPlanFiles(jobs []pullPlanHashJob, progress dataTransferProgress, concurrency int) ([]pullPlanHashResult, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	workerCount := normalizeDataConcurrency(concurrency, defaultDataHashWorkerCount(), len(jobs))
	results := make([]pullPlanHashResult, len(jobs))
	if workerCount <= 1 {
		for idx, job := range jobs {
			sum, err := sha256LocalFileWithProgress(job.Target, progress)
			if err != nil {
				return nil, err
			}
			results[idx] = pullPlanHashResult{Index: job.Index, Entry: job.Entry, Target: job.Target, SHA256: sum}
		}
		return results, nil
	}
	jobCh := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobCh {
			job := jobs[idx]
			sum, err := sha256LocalFileWithProgress(job.Target, progress)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			results[idx] = pullPlanHashResult{Index: job.Index, Entry: job.Entry, Target: job.Target, SHA256: sum}
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for idx := range jobs {
		select {
		case err := <-errCh:
			close(jobCh)
			wg.Wait()
			return nil, err
		case jobCh <- idx:
		}
	}
	close(jobCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
		return results, nil
	}
}

func estimatePullPreflightHashBytes(root string, cfg dataConfig, manifest model.DataManifest) (int64, error) {
	var total int64
	for _, entry := range manifest.Entries {
		if entry.Kind != model.DataManifestEntryKindFile {
			continue
		}
		target, err := targetPathForEntry(root, cfg, entry)
		if err != nil {
			return 0, err
		}
		info, err := os.Lstat(target)
		if err != nil {
			if os.IsNotExist(err) || strings.Contains(err.Error(), "not a directory") {
				continue
			}
			return 0, err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
	}
	return total, nil
}

func targetPathForEntry(root string, cfg dataConfig, entry model.DataManifestEntry) (string, error) {
	for _, asset := range cfg.Assets {
		if asset.Name != entry.AssetName {
			continue
		}
		base := filepath.Join(root, filepath.FromSlash(asset.Path))
		if entry.RelativePath == "." || entry.RelativePath == "" {
			return base, nil
		}
		clean := filepath.Clean(filepath.FromSlash(entry.RelativePath))
		if strings.HasPrefix(clean, "..") {
			return "", fmt.Errorf("manifest path %q escapes asset %s", entry.RelativePath, asset.Name)
		}
		return filepath.Join(base, clean), nil
	}
	return "", fmt.Errorf("manifest references unknown asset %s", entry.AssetName)
}

func dataAssetLocalPath(root string, asset model.DataAsset) (string, error) {
	clean, err := cleanConfigPath(asset.Path)
	if err != nil {
		return "", err
	}
	target := filepath.Join(root, filepath.FromSlash(clean))
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absRoot, absTarget)
	if err != nil {
		return "", err
	}
	if rel == "." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." {
		return "", fmt.Errorf("data asset path %q escapes the project", asset.Path)
	}
	return target, nil
}

func removeDataAssetPath(root string, asset model.DataAsset) error {
	target, err := dataAssetLocalPath(root, asset)
	if err != nil {
		return err
	}
	info, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		return os.RemoveAll(target)
	}
	return os.Remove(target)
}

func checkPullDiskSpace(root string, entries []model.DataManifestEntry) error {
	var needed uint64
	for _, entry := range entries {
		if entry.Kind == model.DataManifestEntryKindFile && entry.Size > 0 {
			needed += uint64(entry.Size)
		}
	}
	if needed == 0 {
		return nil
	}
	available, ok := pullAvailableDiskBytes(root)
	if !ok {
		return nil
	}
	if available < needed {
		return fmt.Errorf("not enough disk space: need %s, available %s", formatBytes(int64(needed)), formatBytes(int64(available)))
	}
	return nil
}

func checkPullWriteAccess(root string, cfg dataConfig, entries []model.DataManifestEntry, prune []string) error {
	seen := map[string]struct{}{}
	for _, entry := range entries {
		target, err := targetPathForEntry(root, cfg, entry)
		if err != nil {
			return err
		}
		dir := target
		if entry.Kind != model.DataManifestEntryKindDir {
			dir = filepath.Dir(target)
		}
		existing, err := nearestExistingDir(dir)
		if err != nil {
			return err
		}
		if _, ok := seen[existing]; ok {
			continue
		}
		seen[existing] = struct{}{}
		if err := checkDirectoryWritable(existing); err != nil {
			return fmt.Errorf("target directory is not writable: %s", existing)
		}
	}
	for _, removePath := range prune {
		dir := filepath.Dir(removePath)
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}
		if err := checkDirectoryWritable(dir); err != nil {
			return fmt.Errorf("directory is not writable for prune: %s", dir)
		}
	}
	return nil
}

func checkDirectoryWritable(dir string) error {
	file, err := os.CreateTemp(dir, ".fugue-write-check-*")
	if err != nil {
		return err
	}
	name := file.Name()
	closeErr := file.Close()
	removeErr := os.Remove(name)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

func nearestExistingDir(dir string) (string, error) {
	for {
		info, err := os.Stat(dir)
		if err == nil {
			if !info.IsDir() {
				return "", fmt.Errorf("parent path is not a directory: %s", dir)
			}
			return dir, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", err
		}
		dir = parent
	}
}

type dataDownloadFileJob struct {
	Entry  model.DataManifestEntry
	Target string
}

func dataBlobPlanMap(blobs []dataBlobPlan) map[string]dataBlobPlan {
	out := map[string]dataBlobPlan{}
	for _, blob := range blobs {
		digest := strings.TrimSpace(strings.ToLower(blob.SHA256))
		if digest == "" {
			continue
		}
		out[digest] = blob
	}
	return out
}

func splitDataManifestDownloadEntriesByKind(entries []model.DataManifestEntry) ([]model.DataManifestEntry, []model.DataManifestEntry) {
	nonFiles := []model.DataManifestEntry{}
	files := []model.DataManifestEntry{}
	for _, entry := range entries {
		if entry.Kind == model.DataManifestEntryKindFile {
			files = append(files, entry)
		} else {
			nonFiles = append(nonFiles, entry)
		}
	}
	return nonFiles, files
}

func filterDataManifestEntriesByBlobPage(entries []model.DataManifestEntry, blobs []dataBlobPlan) []model.DataManifestEntry {
	digests := map[string]struct{}{}
	for _, blob := range blobs {
		digest := strings.TrimSpace(strings.ToLower(blob.SHA256))
		if digest == "" {
			continue
		}
		digests[digest] = struct{}{}
	}
	if len(digests) == 0 {
		return nil
	}
	out := []model.DataManifestEntry{}
	for _, entry := range entries {
		digest := strings.TrimSpace(strings.ToLower(entry.SHA256))
		if _, ok := digests[digest]; ok {
			out = append(out, entry)
		}
	}
	return out
}

type dataDownloadCheckpointRecorder struct {
	mu                sync.Mutex
	client            *Client
	transferID        string
	bytesDone         int64
	filesDone         int
	lastCheckpointB   int64
	lastCheckpointF   int
	checkpointEnabled bool
}

func (r *dataDownloadCheckpointRecorder) add(size int64) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.bytesDone += size
	r.filesDone++
	shouldCheckpoint := r.checkpointEnabled &&
		(r.filesDone-r.lastCheckpointF >= dataCheckpointBlobThreshold || r.bytesDone-r.lastCheckpointB >= dataCheckpointByteThreshold)
	bytesDone := r.bytesDone
	filesDone := r.filesDone
	if shouldCheckpoint {
		r.lastCheckpointB = bytesDone
		r.lastCheckpointF = filesDone
	}
	r.mu.Unlock()
	if shouldCheckpoint {
		return r.client.CheckpointDataTransfer(r.transferID, bytesDone, filesDone, nil)
	}
	return nil
}

func (r *dataDownloadCheckpointRecorder) flush() (int64, int, error) {
	if r == nil {
		return 0, 0, nil
	}
	r.mu.Lock()
	bytesDone := r.bytesDone
	filesDone := r.filesDone
	shouldCheckpoint := r.checkpointEnabled && filesDone > 0 && (bytesDone != r.lastCheckpointB || filesDone != r.lastCheckpointF)
	if shouldCheckpoint {
		r.lastCheckpointB = bytesDone
		r.lastCheckpointF = filesDone
	}
	r.mu.Unlock()
	if shouldCheckpoint {
		if err := r.client.CheckpointDataTransfer(r.transferID, bytesDone, filesDone, nil); err != nil {
			return bytesDone, filesDone, err
		}
	}
	return bytesDone, filesDone, nil
}

func (c *CLI) downloadDataManifestEntriesPaged(client *Client, transferID, root string, cfg dataConfig, entries []model.DataManifestEntry, firstBlobs []dataBlobPlan, pageOffset, pageLimit int, nextOffset *int, resume, overwrite, noProgress bool, concurrency int, progress *dataProgressRenderer) (int64, int, error) {
	nonFiles, fileEntries := splitDataManifestDownloadEntriesByKind(entries)
	if len(nonFiles) > 0 {
		if _, _, err := c.downloadDataManifestEntries(client, transferID, root, cfg, nonFiles, nil, resume, overwrite, noProgress, concurrency, progress, pageOffset, pageLimit); err != nil {
			return 0, 0, err
		}
	}
	if len(fileEntries) == 0 {
		return 0, 0, nil
	}
	if pageLimit <= 0 {
		pageLimit = dataTransferBlobPageLimit
	}
	currentBlobs := firstBlobs
	currentOffset := pageOffset
	var totalBytes int64
	var totalFiles int
	for {
		pageEntries := filterDataManifestEntriesByBlobPage(fileEntries, currentBlobs)
		if len(pageEntries) > 0 {
			blobByDigest := dataBlobPlanMap(currentBlobs)
			if err := validatePullBlobs(blobByDigest, pageEntries); err != nil {
				return totalBytes, totalFiles, err
			}
			bytesDone, filesDone, err := c.downloadDataManifestEntries(client, transferID, root, cfg, pageEntries, blobByDigest, resume, overwrite, noProgress, concurrency, progress, currentOffset, pageLimit)
			totalBytes += bytesDone
			totalFiles += filesDone
			if err != nil {
				return totalBytes, totalFiles, err
			}
		}
		if nextOffset == nil {
			break
		}
		currentOffset = *nextOffset
		refresh, err := client.RefreshDataTransferAuthorizationPage(transferID, currentOffset, pageLimit)
		if err != nil {
			return totalBytes, totalFiles, err
		}
		currentBlobs = refresh.Blobs
		nextOffset = refresh.BlobsNextOffset
		if len(currentBlobs) == 0 && nextOffset != nil {
			return totalBytes, totalFiles, fmt.Errorf("transfer blob page at offset %d was empty", currentOffset)
		}
	}
	return totalBytes, totalFiles, nil
}

func (c *CLI) downloadDataManifestEntries(client *Client, transferID, root string, cfg dataConfig, entries []model.DataManifestEntry, blobByDigest map[string]dataBlobPlan, resume, overwrite, noProgress bool, concurrency int, progress *dataProgressRenderer, refreshOffset, refreshLimit int) (int64, int, error) {
	fileJobs := []dataDownloadFileJob{}
	for _, entry := range entries {
		target, err := targetPathForEntry(root, cfg, entry)
		if err != nil {
			return 0, 0, err
		}
		switch entry.Kind {
		case model.DataManifestEntryKindDir:
			if err := os.MkdirAll(target, os.FileMode(entry.Mode)); err != nil {
				return 0, 0, err
			}
		case model.DataManifestEntryKindSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return 0, 0, err
			}
			if overwrite {
				_ = os.Remove(target)
			}
			if err := os.Symlink(entry.LinkTarget, target); err != nil && !os.IsExist(err) {
				return 0, 0, err
			}
		case model.DataManifestEntryKindFile:
			fileJobs = append(fileJobs, dataDownloadFileJob{Entry: entry, Target: target})
		}
	}
	recorder := &dataDownloadCheckpointRecorder{
		client:            client,
		transferID:        transferID,
		checkpointEnabled: strings.TrimSpace(transferID) != "",
	}
	if len(fileJobs) == 0 {
		return recorder.flush()
	}
	totalConcurrency := normalizeDataConcurrency(concurrency, 8, 0)
	workerCount := normalizeDataConcurrency(totalConcurrency, 8, len(fileJobs))
	perFileConcurrency := totalConcurrency / workerCount
	if perFileConcurrency < 1 {
		perFileConcurrency = 1
	}
	blobMu := sync.Mutex{}
	refreshMu := sync.Mutex{}
	printMu := sync.Mutex{}
	digestLocks := map[string]*sync.Mutex{}
	digestLockMu := sync.Mutex{}
	digestLock := func(digest string) *sync.Mutex {
		digestLockMu.Lock()
		defer digestLockMu.Unlock()
		lock := digestLocks[digest]
		if lock == nil {
			lock = &sync.Mutex{}
			digestLocks[digest] = lock
		}
		return lock
	}
	jobCh := make(chan dataDownloadFileJob)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for job := range jobCh {
			entry := job.Entry
			blobMu.Lock()
			blob := blobByDigest[entry.SHA256]
			blobMu.Unlock()
			if blob.DownloadURL == "" {
				select {
				case errCh <- fmt.Errorf("download url missing for %s", entry.SHA256):
				default:
				}
				return
			}
			if noProgress && !c.wantsJSON() {
				printMu.Lock()
				fmt.Fprintf(c.stdout, "Downloading %s  %s\n", job.Target, formatBytes(entry.Size))
				printMu.Unlock()
			}
			lock := digestLock(entry.SHA256)
			lock.Lock()
			if err := retryDataAuthorization(func() error {
				return client.DownloadDataBlobWithProgress(blob.DownloadURL, job.Target, entry.SHA256, entry.Size, resume, overwrite, perFileConcurrency, progress.advance)
			}, func() error {
				refreshMu.Lock()
				defer refreshMu.Unlock()
				var refresh dataTransferAuthorizationResponse
				var err error
				if refreshLimit > 0 {
					refresh, err = client.RefreshDataTransferAuthorizationPage(transferID, refreshOffset, refreshLimit)
				} else {
					refresh, err = client.RefreshDataTransferAuthorization(transferID)
				}
				if err != nil {
					return err
				}
				blobMu.Lock()
				for _, refreshed := range refresh.Blobs {
					blobByDigest[refreshed.SHA256] = refreshed
				}
				refreshed, ok := blobByDigest[entry.SHA256]
				blobMu.Unlock()
				if !ok {
					return fmt.Errorf("refreshed transfer plan is missing blob %s", entry.SHA256)
				}
				blob = refreshed
				return nil
			}); err != nil {
				lock.Unlock()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			lock.Unlock()
			if err := recorder.add(entry.Size); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for _, job := range fileJobs {
		select {
		case err := <-errCh:
			close(jobCh)
			wg.Wait()
			return 0, 0, err
		case jobCh <- job:
		}
	}
	close(jobCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return 0, 0, err
	default:
		return recorder.flush()
	}
}

func validatePullBlobs(blobs map[string]dataBlobPlan, entries []model.DataManifestEntry) error {
	for _, entry := range entries {
		if entry.Kind != model.DataManifestEntryKindFile {
			continue
		}
		blob := blobs[entry.SHA256]
		if strings.TrimSpace(blob.DownloadURL) == "" || !blob.Exists {
			return fmt.Errorf("remote blob is missing for %s (%s)", entry.AssetName+"/"+entry.RelativePath, entry.SHA256)
		}
	}
	return nil
}

func verifyPulledManifest(root string, cfg dataConfig, manifest model.DataManifest) error {
	return verifyPulledManifestWithProgress(root, cfg, manifest, nil)
}

func verifyPulledManifestWithProgress(root string, cfg dataConfig, manifest model.DataManifest, progress dataTransferProgress) error {
	return verifyPulledManifestWithConcurrency(root, cfg, manifest, progress, defaultDataHashWorkerCount())
}

func verifyPulledManifestWithConcurrency(root string, cfg dataConfig, manifest model.DataManifest, progress dataTransferProgress, hashConcurrency int) error {
	type verifyHashJob struct {
		Entry  model.DataManifestEntry
		Target string
	}
	jobs := []verifyHashJob{}
	for _, entry := range manifest.Entries {
		target, err := targetPathForEntry(root, cfg, entry)
		if err != nil {
			return err
		}
		info, err := os.Lstat(target)
		if err != nil {
			return fmt.Errorf("verify %s: %w", target, err)
		}
		switch entry.Kind {
		case model.DataManifestEntryKindDir:
			if !info.IsDir() {
				return fmt.Errorf("verify %s: expected directory", target)
			}
		case model.DataManifestEntryKindSymlink:
			if info.Mode()&os.ModeSymlink == 0 {
				return fmt.Errorf("verify %s: expected symlink", target)
			}
			got, err := os.Readlink(target)
			if err != nil {
				return err
			}
			if got != entry.LinkTarget {
				return fmt.Errorf("verify %s: expected symlink target %q, got %q", target, entry.LinkTarget, got)
			}
		case model.DataManifestEntryKindFile:
			if !info.Mode().IsRegular() {
				return fmt.Errorf("verify %s: expected file", target)
			}
			jobs = append(jobs, verifyHashJob{Entry: entry, Target: target})
		}
	}
	if len(jobs) == 0 {
		return nil
	}
	workerCount := normalizeDataConcurrency(hashConcurrency, defaultDataHashWorkerCount(), len(jobs))
	if workerCount <= 1 {
		for _, job := range jobs {
			got, err := sha256LocalFileWithProgress(job.Target, progress)
			if err != nil {
				return err
			}
			if got != job.Entry.SHA256 {
				return fmt.Errorf("verify %s: checksum mismatch", job.Target)
			}
		}
		return nil
	}
	jobCh := make(chan verifyHashJob)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for job := range jobCh {
			got, err := sha256LocalFileWithProgress(job.Target, progress)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if got != job.Entry.SHA256 {
				select {
				case errCh <- fmt.Errorf("verify %s: checksum mismatch", job.Target):
				default:
				}
				return
			}
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for _, job := range jobs {
		select {
		case err := <-errCh:
			close(jobCh)
			wg.Wait()
			return err
		case jobCh <- job:
		}
	}
	close(jobCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func renderPullPreflight(w io.Writer, workspace model.DataWorkspace, snapshot model.DataSnapshot, plan pullPlan) {
	renderDataKeyValueTable(w, [][]string{
		{"Data workspace", workspace.Name},
		{"Version", snapshot.Version},
	})
	if len(plan.Conflicts) > 0 {
		fmt.Fprintln(w, "\nPull preflight found conflicts.")
		rows := make([][]string, 0, len(plan.Conflicts))
		for _, conflict := range plan.Conflicts {
			rows = append(rows, []string{conflict.Kind, conflict.Path, conflict.Reason})
		}
		renderDataTable(w, []dataTableColumn{
			{Title: "Conflict"},
			{Title: "Path"},
			{Title: "Reason"},
		}, rows)
	}
	if len(plan.Warnings) > 0 {
		fmt.Fprintln(w, "\nWarnings:")
		rows := make([][]string, 0, len(plan.Warnings))
		for _, warning := range plan.Warnings {
			rows = append(rows, []string{warning.Kind, warning.Path, warning.Reason})
		}
		renderDataTable(w, []dataTableColumn{
			{Title: "Warning"},
			{Title: "Path"},
			{Title: "Reason"},
		}, rows)
	}
	if len(plan.Conflicts) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "No files were changed.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Run one of:")
		fmt.Fprintln(w, "  fugue data pull --dry-run")
		fmt.Fprintln(w, "  fugue data pull --keep-local")
		fmt.Fprintln(w, "  fugue data pull --overwrite")
		fmt.Fprintln(w, "  fugue data pull --prune --confirm")
		fmt.Fprintln(w, "  fugue data push --version local-before-pull")
		return
	}
	fmt.Fprintln(w)
	renderDataKeyValueTable(w, [][]string{
		{"Planning download", strconv.Itoa(len(plan.Download))},
		{"Files to skip", strconv.Itoa(len(plan.Skip))},
	})
}

func renderDataEvictPlan(w io.Writer, workspace model.DataWorkspace, snapshot model.DataSnapshot, plan dataEvictPlan, dryRun bool) {
	mode := "delete"
	if dryRun {
		mode = "dry-run"
	}
	renderDataKeyValueTable(w, [][]string{
		{"Data workspace", workspace.Name},
		{"Latest version", snapshot.Version},
		{"Mode", mode},
	})
	rows := make([][]string, 0, len(plan.Evict)+len(plan.Skip)+len(plan.Blocked))
	for _, item := range plan.Evict {
		rows = append(rows, []string{"evict", item.Name, item.Path, strconv.Itoa(item.Files), formatBytes(item.Bytes), ""})
	}
	for _, item := range plan.Skip {
		rows = append(rows, []string{"skip", item.Name, item.Path, strconv.Itoa(item.Files), formatBytes(item.Bytes), item.Reason})
	}
	for _, item := range plan.Blocked {
		rows = append(rows, []string{"blocked", item.Name, item.Path, strconv.Itoa(item.Files), formatBytes(item.Bytes), item.Reason})
	}
	if len(rows) == 0 {
		fmt.Fprintln(w, "\nNo tracked assets can be evicted.")
		return
	}
	fmt.Fprintln(w)
	renderDataTable(w, []dataTableColumn{
		{Title: "Action"},
		{Title: "Asset"},
		{Title: "Local path"},
		{Title: "Files", Align: dataTableAlignRight},
		{Title: "Size", Align: dataTableAlignRight},
		{Title: "Reason"},
	}, rows)
	if dryRun && len(plan.Evict) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Run `fugue data evict --confirm` to delete the safe local asset paths.")
	}
}

func manifestStatsByAsset(manifest model.DataManifest) map[string]struct {
	Files int
	Bytes int64
} {
	out := map[string]struct {
		Files int
		Bytes int64
	}{}
	for _, entry := range manifest.Entries {
		if entry.Kind != model.DataManifestEntryKindFile {
			continue
		}
		stats := out[entry.AssetName]
		stats.Files++
		stats.Bytes += entry.Size
		out[entry.AssetName] = stats
	}
	return out
}

func dataSnapshotFileCount(snapshot model.DataSnapshot) int {
	if snapshot.FileCount > 0 {
		return snapshot.FileCount
	}
	return snapshot.Manifest.FileCount
}

func dataSnapshotTotalBytes(snapshot model.DataSnapshot) int64 {
	if snapshot.TotalBytes > 0 {
		return snapshot.TotalBytes
	}
	return snapshot.Manifest.TotalBytes
}

func manifestHasAsset(manifest model.DataManifest, assetName string) bool {
	for _, entry := range manifest.Entries {
		if entry.AssetName == assetName {
			return true
		}
	}
	return false
}

func buildDataEvictPlan(root string, assets []model.DataAsset, localManifest, latestManifest model.DataManifest) (dataEvictPlan, error) {
	var plan dataEvictPlan
	localByAsset := manifestStatsByAsset(localManifest)
	remoteByAsset := manifestStatsByAsset(latestManifest)
	for _, asset := range assets {
		target, err := dataAssetLocalPath(root, asset)
		if err != nil {
			return plan, err
		}
		remoteStats := remoteByAsset[asset.Name]
		localStats := localByAsset[asset.Name]
		item := dataEvictPlanAsset{
			Name:  asset.Name,
			Path:  asset.Path,
			Files: remoteStats.Files,
			Bytes: remoteStats.Bytes,
		}
		if item.Files == 0 && item.Bytes == 0 {
			item.Files = localStats.Files
			item.Bytes = localStats.Bytes
		}
		if !manifestHasAsset(latestManifest, asset.Name) {
			item.Status = "blocked"
			item.Reason = "asset is not present in the latest remote version"
			plan.Blocked = append(plan.Blocked, item)
			continue
		}
		if _, err := os.Lstat(target); err != nil {
			if os.IsNotExist(err) {
				item.Status = "already evicted"
				plan.Skip = append(plan.Skip, item)
				continue
			}
			item.Status = "blocked"
			item.Reason = err.Error()
			plan.Blocked = append(plan.Blocked, item)
			continue
		}
		if !manifestHasAsset(localManifest, asset.Name) {
			item.Status = "blocked"
			item.Reason = "local path exists but no manifest entries were produced"
			plan.Blocked = append(plan.Blocked, item)
			continue
		}
		if manifestAssetDigest(localManifest, asset.Name) != manifestAssetDigest(latestManifest, asset.Name) {
			item.Status = "blocked"
			item.Reason = "local content differs from the latest remote version; run fugue data push first"
			plan.Blocked = append(plan.Blocked, item)
			continue
		}
		item.Status = "evict"
		plan.Evict = append(plan.Evict, item)
	}
	return plan, nil
}

func totalDataEvictPlanBytes(items []dataEvictPlanAsset) int64 {
	var total int64
	for _, item := range items {
		total += item.Bytes
	}
	return total
}

func manifestAssetDigest(manifest model.DataManifest, assetName string) string {
	var b strings.Builder
	for _, entry := range manifest.Entries {
		if entry.AssetName != assetName {
			continue
		}
		b.WriteString(entry.RelativePath)
		b.WriteByte(0)
		b.WriteString(entry.SHA256)
		b.WriteByte(0)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func dataAssetsEqual(a, b []model.DataAsset) bool {
	rawA, _ := json.Marshal(normalizeConfigAssets(a))
	rawB, _ := json.Marshal(normalizeConfigAssets(b))
	return bytes.Equal(rawA, rawB)
}

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func diffDataManifests(from, to model.DataManifest) map[string][]model.DataManifestEntry {
	fromByPath := manifestEntriesByStablePath(from)
	toByPath := manifestEntriesByStablePath(to)
	diff := map[string][]model.DataManifestEntry{
		"added":   {},
		"removed": {},
		"changed": {},
	}
	for key, toEntry := range toByPath {
		fromEntry, exists := fromByPath[key]
		if !exists {
			diff["added"] = append(diff["added"], toEntry)
			continue
		}
		if fromEntry.Kind != toEntry.Kind || fromEntry.SHA256 != toEntry.SHA256 || fromEntry.LinkTarget != toEntry.LinkTarget {
			diff["changed"] = append(diff["changed"], toEntry)
		}
	}
	for key, fromEntry := range fromByPath {
		if _, exists := toByPath[key]; !exists {
			diff["removed"] = append(diff["removed"], fromEntry)
		}
	}
	return diff
}

func manifestEntriesByStablePath(manifest model.DataManifest) map[string]model.DataManifestEntry {
	out := map[string]model.DataManifestEntry{}
	for _, entry := range manifest.Entries {
		out[entry.AssetName+"/"+entry.RelativePath] = entry
	}
	return out
}

func renderDataWorkspace(c *CLI, workspace model.DataWorkspace) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{"workspace": workspace})
	}
	renderDataKeyValueTable(c.stdout, [][]string{
		{"Data workspace", workspace.Name},
		{"Backend", workspace.StorageBackendID},
		{"Used", formatBytes(workspace.UsedBytes)},
	})
	return nil
}

func (c *CLI) uploadDataPlanBlobs(client *Client, workspaceID, transferID, manifestDigest string, blobs []dataBlobPlan, pathsByDigest map[string]string, resume, noProgress bool, concurrency int, totalBytes int64, pageOffset, pageLimit int, nextOffset *int) error {
	if concurrency <= 0 {
		concurrency = defaultDataTransferConcurrency
	}
	if totalBytes <= 0 {
		totalBytes = totalDataBlobBytes(blobs)
	}
	if pageLimit <= 0 {
		pageLimit = dataTransferBlobPageLimit
	}
	progress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Upload", totalBytes)
	uploader, err := c.newDataUploadExecutor(client, workspaceID, transferID, manifestDigest, pathsByDigest, resume, noProgress, concurrency, pageLimit, progress)
	if err != nil {
		return err
	}
	currentBlobs := blobs
	currentOffset := pageOffset
	for {
		if err := uploader.uploadPage(currentBlobs, currentOffset); err != nil {
			return err
		}
		if nextOffset == nil {
			break
		}
		currentOffset = *nextOffset
		refresh, err := client.RefreshDataTransferAuthorizationPage(transferID, currentOffset, pageLimit)
		if err != nil {
			return err
		}
		currentBlobs = refresh.Blobs
		nextOffset = refresh.BlobsNextOffset
		if len(currentBlobs) == 0 && nextOffset != nil {
			return fmt.Errorf("transfer blob page at offset %d was empty", currentOffset)
		}
	}
	if err := uploader.flushCheckpoints(true); err != nil {
		return err
	}
	progress.finish()
	uploader.printCheckpointWarning()
	return nil
}

type dataUploadExecutor struct {
	cli            *CLI
	client         *Client
	workspaceID    string
	transferID     string
	manifestDigest string
	pathsByDigest  map[string]string
	resume         bool
	noProgress     bool
	concurrency    int
	pageLimit      int
	progress       *dataProgressRenderer

	mu              sync.Mutex
	state           dataTransferState
	checkpointBlobs []dataBlobPlan
	checkpointBytes int64
	checkpointWarns int
	checkpointErr   error
}

func (c *CLI) newDataUploadExecutor(client *Client, workspaceID, transferID, manifestDigest string, pathsByDigest map[string]string, resume, noProgress bool, concurrency, pageLimit int, progress *dataProgressRenderer) (*dataUploadExecutor, error) {
	state := dataTransferState{
		TransferID:     transferID,
		Direction:      model.DataTransferDirectionUpload,
		WorkspaceID:    workspaceID,
		ManifestDigest: manifestDigest,
	}
	if resume {
		loaded, ok, err := loadDataTransferState(".", transferID)
		if err != nil {
			return nil, err
		}
		if ok {
			state = loaded
			if state.TransferID == "" {
				state.TransferID = transferID
			}
			if state.Direction == "" {
				state.Direction = model.DataTransferDirectionUpload
			}
			if state.WorkspaceID == "" {
				state.WorkspaceID = workspaceID
			}
			if state.ManifestDigest == "" {
				state.ManifestDigest = manifestDigest
			}
		}
	}
	return &dataUploadExecutor{
		cli:            c,
		client:         client,
		workspaceID:    workspaceID,
		transferID:     transferID,
		manifestDigest: manifestDigest,
		pathsByDigest:  pathsByDigest,
		resume:         resume,
		noProgress:     noProgress,
		concurrency:    concurrency,
		pageLimit:      pageLimit,
		progress:       progress,
		state:          state,
	}, nil
}

func (u *dataUploadExecutor) uploadPage(blobs []dataBlobPlan, pageOffset int) error {
	if len(blobs) == 0 {
		return nil
	}
	if u.concurrency <= 1 {
		for _, blob := range blobs {
			if err := u.uploadOne(blob, pageOffset, 1); err != nil {
				return err
			}
		}
		return nil
	}
	var uploadBlobs []dataBlobPlan
	for _, blob := range blobs {
		blob = u.mergeBlob(blob)
		if blob.Exists {
			u.progress.advance(blob.Size)
			continue
		}
		uploadBlobs = append(uploadBlobs, blob)
	}
	if len(uploadBlobs) == 0 {
		return nil
	}
	workerCount := u.concurrency
	if workerCount > len(uploadBlobs) {
		workerCount = len(uploadBlobs)
	}
	perBlobConcurrency := u.concurrency / workerCount
	if perBlobConcurrency < 1 {
		perBlobConcurrency = 1
	}
	jobs := make(chan dataBlobPlan)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for blob := range jobs {
			if err := u.uploadOne(blob, pageOffset, perBlobConcurrency); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for _, blob := range uploadBlobs {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case jobs <- blob:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (u *dataUploadExecutor) uploadOne(blob dataBlobPlan, pageOffset, blobConcurrency int) error {
	blob = u.mergeBlob(blob)
	if blob.Exists {
		u.progress.advance(blob.Size)
		return nil
	}
	if blob.UploadMode != model.DataBlobUploadModeMultipart && strings.TrimSpace(blob.UploadURL) == "" {
		return fmt.Errorf("upload url missing for blob %s (mode=%q object_key=%q parts=%d exists=%t)", blob.SHA256, blob.UploadMode, blob.ObjectKey, len(blob.Parts), blob.Exists)
	}
	sourcePath := u.pathsByDigest[blob.SHA256]
	if sourcePath == "" {
		return fmt.Errorf("missing local source for blob %s", blob.SHA256)
	}
	u.printUploadLine(blob)
	if blob.UploadMode == model.DataBlobUploadModeMultipart {
		return u.uploadMultipart(blob, sourcePath, pageOffset, blobConcurrency)
	}
	if strings.TrimSpace(blob.UploadURL) == "" {
		return nil
	}
	err := retryDataAuthorization(func() error {
		return u.client.PutDataBlobWithProgress(blob.UploadURL, sourcePath, u.progress.advance)
	}, func() error {
		refresh, err := u.client.RefreshDataTransferAuthorizationPage(u.transferID, pageOffset, u.pageLimit)
		if err != nil {
			return err
		}
		refreshed, ok := dataPlanBlobByDigest(refresh.Blobs, blob.SHA256)
		if !ok {
			return fmt.Errorf("refreshed transfer plan is missing blob %s", blob.SHA256)
		}
		blob = u.mergeBlob(refreshed)
		return nil
	})
	if err != nil {
		return err
	}
	return u.recordCompletedBlob(markDataBlobUploaded(blob, nil))
}

func (u *dataUploadExecutor) uploadMultipart(blob dataBlobPlan, sourcePath string, pageOffset, blobConcurrency int) error {
	var completed []model.DataTransferPart
	err := retryDataAuthorization(func() error {
		var uploadErr error
		completed, uploadErr = u.cli.uploadMultipartDataBlob(u.client, sourcePath, u.transferID, blob, u.resume, blobConcurrency, u.progress.advance)
		return uploadErr
	}, func() error {
		refresh, err := u.client.RefreshDataTransferAuthorizationPage(u.transferID, pageOffset, u.pageLimit)
		if err != nil {
			return err
		}
		refreshed, ok := dataPlanBlobByDigest(refresh.Blobs, blob.SHA256)
		if !ok {
			return fmt.Errorf("refreshed transfer plan is missing blob %s", blob.SHA256)
		}
		blob = u.mergeBlob(refreshed)
		return nil
	})
	if err != nil {
		return err
	}
	if _, err := u.client.CompleteDataMultipartUpload(u.transferID, blob.SHA256, blob.UploadID, completed); err != nil {
		return err
	}
	return u.recordCompletedBlob(markDataBlobUploaded(blob, completed))
}

func (u *dataUploadExecutor) mergeBlob(blob dataBlobPlan) dataBlobPlan {
	u.mu.Lock()
	defer u.mu.Unlock()
	return mergeDataBlobWithState(blob, u.state)
}

func (u *dataUploadExecutor) printUploadLine(blob dataBlobPlan) {
	if !u.noProgress || u.cli.wantsJSON() {
		return
	}
	mode := blob.UploadMode
	if mode == "" {
		mode = model.DataBlobUploadModeSingle
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	fmt.Fprintf(u.cli.stdout, "Uploading %s  %s  %s\n", shortDataDigest(blob.SHA256), formatBytes(blob.Size), mode)
}

func (u *dataUploadExecutor) recordCompletedBlob(blob dataBlobPlan) error {
	u.mu.Lock()
	u.state = upsertDataTransferStateBlob(u.state, blob)
	u.checkpointBlobs = append(u.checkpointBlobs, blob)
	if blob.Size > 0 {
		u.checkpointBytes += blob.Size
	}
	u.mu.Unlock()
	return u.flushCheckpoints(false)
}

func (u *dataUploadExecutor) flushCheckpoints(force bool) error {
	u.mu.Lock()
	if len(u.checkpointBlobs) == 0 {
		u.mu.Unlock()
		return nil
	}
	if !force && len(u.checkpointBlobs) < dataCheckpointBlobThreshold && u.checkpointBytes < dataCheckpointByteThreshold {
		u.mu.Unlock()
		return nil
	}
	batch := append([]dataBlobPlan(nil), u.checkpointBlobs...)
	state := cloneDataTransferState(u.state)
	u.checkpointBlobs = u.checkpointBlobs[:0]
	u.checkpointBytes = 0
	u.mu.Unlock()
	if u.resume {
		if err := saveDataTransferState(".", state); err != nil {
			return err
		}
	}
	if err := u.client.CheckpointDataTransfer(u.transferID, -1, -1, batch); err != nil {
		if isTransientDataControlPlaneError(err) {
			u.noteCheckpointWarning(err)
			return nil
		}
		return err
	}
	return nil
}

func (u *dataUploadExecutor) noteCheckpointWarning(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.checkpointWarns++
	u.checkpointErr = err
}

func (u *dataUploadExecutor) printCheckpointWarning() {
	u.mu.Lock()
	count := u.checkpointWarns
	err := u.checkpointErr
	u.mu.Unlock()
	if count == 0 || err == nil || u.cli == nil || u.cli.wantsJSON() {
		return
	}
	u.cli.progressf("warning=data transfer checkpoint sync was delayed for %d batch(es); upload continued and the final snapshot remains authoritative: %v", count, err)
}

func (c *CLI) uploadMultipartDataBlob(client *Client, sourcePath, transferID string, blob dataBlobPlan, resume bool, concurrency int, progress dataTransferProgress) ([]model.DataTransferPart, error) {
	if blob.UploadID == "" {
		return nil, fmt.Errorf("multipart upload id is missing for %s", blob.SHA256)
	}
	if len(blob.Parts) == 0 {
		return nil, fmt.Errorf("multipart upload plan has no parts for %s", blob.SHA256)
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(blob.Parts) {
		concurrency = len(blob.Parts)
	}
	parts := append([]model.DataTransferPart(nil), blob.Parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	if resume {
		if remoteParts, err := client.ListDataMultipartParts(transferID, blob.SHA256); err == nil {
			remoteByNumber := map[int32]model.DataTransferPart{}
			for _, part := range remoteParts {
				remoteByNumber[part.PartNumber] = part
			}
			for idx := range parts {
				if remote, ok := remoteByNumber[parts[idx].PartNumber]; ok && remote.ETag != "" {
					parts[idx].Completed = true
					parts[idx].ETag = remote.ETag
				}
			}
		}
	}
	for _, part := range parts {
		if part.Completed && strings.TrimSpace(part.ETag) != "" && progress != nil {
			progress(part.Size)
		}
	}
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var mu sync.Mutex
	var wg sync.WaitGroup
	stateSave := func() error {
		if !resume {
			return nil
		}
		current := blob
		current.Parts = append([]model.DataTransferPart(nil), parts...)
		if _, err := updateDataTransferState(".", transferID, func(loaded dataTransferState, ok bool) (dataTransferState, error) {
			if !ok {
				loaded = dataTransferState{TransferID: transferID, Direction: model.DataTransferDirectionUpload}
			}
			loaded = upsertDataTransferStateBlob(loaded, current)
			return loaded, nil
		}); err != nil {
			return err
		}
		return client.CheckpointDataTransfer(transferID, -1, -1, []dataBlobPlan{current})
	}
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			mu.Lock()
			part := parts[idx]
			mu.Unlock()
			if part.Completed && strings.TrimSpace(part.ETag) != "" {
				continue
			}
			etag, err := client.UploadDataBlobPartWithProgress(part.UploadURL, sourcePath, part.Offset, part.Size, progress)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			mu.Lock()
			parts[idx].ETag = etag
			parts[idx].Completed = true
			saveErr := stateSave()
			mu.Unlock()
			if saveErr != nil {
				select {
				case errCh <- saveErr:
				default:
				}
				return
			}
		}
	}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	for idx := range parts {
		if parts[idx].Completed && strings.TrimSpace(parts[idx].ETag) != "" {
			continue
		}
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return nil, err
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	for _, part := range parts {
		if strings.TrimSpace(part.ETag) == "" {
			return nil, fmt.Errorf("multipart part %d for %s did not return an etag", part.PartNumber, blob.SHA256)
		}
	}
	return parts, nil
}

func retryDataAuthorization(run func() error, refresh func() error) error {
	err := run()
	if !errors.Is(err, errDataAuthorizationExpired) {
		return err
	}
	if refreshErr := refresh(); refreshErr != nil {
		return refreshErr
	}
	return run()
}

func newDataProgressRenderer(w io.Writer, enabled bool, label string, total int64) *dataProgressRenderer {
	return &dataProgressRenderer{w: w, enabled: enabled, label: label, total: total, start: time.Now()}
}

func (p *dataProgressRenderer) advance(delta int64) {
	if p == nil || !p.enabled || delta == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.done += delta
	if p.done < 0 {
		p.done = 0
	}
	if p.total > 0 && p.done > p.total {
		p.done = p.total
	}
	now := time.Now()
	if p.start.IsZero() {
		p.start = now
	}
	if !p.last.IsZero() && now.Sub(p.last) < 500*time.Millisecond && (p.total <= 0 || p.done < p.total) {
		return
	}
	p.last = now
	final := p.total > 0 && p.done >= p.total
	p.writeLineLocked(p.renderLocked(now), final)
	if final {
		p.finished = true
	}
}

func (p *dataProgressRenderer) finish() {
	if p == nil || !p.enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	if p.total > 0 {
		p.done = p.total
	}
	if p.done == 0 && p.total <= 0 && !p.lineActive {
		return
	}
	now := time.Now()
	p.last = now
	p.writeLineLocked(p.renderLocked(now), true)
	p.finished = true
}

func (p *dataProgressRenderer) renderLocked(now time.Time) string {
	elapsed := now.Sub(p.start)
	if elapsed < time.Second {
		elapsed = time.Second
	}
	rate := float64(p.done) / elapsed.Seconds()
	rateText := formatBytes(int64(rate)) + "/s"
	if p.total <= 0 {
		return fmt.Sprintf("%s  %s  %s  elapsed %s", p.label, formatBytes(p.done), rateText, formatProgressDuration(elapsed))
	}
	percent := float64(p.done) / float64(p.total) * 100
	if percent > 100 {
		percent = 100
	}
	eta := "ETA --"
	if p.done >= p.total {
		eta = "ETA 0s"
	} else if rate > 0 {
		remaining := float64(p.total-p.done) / rate
		eta = "ETA " + formatProgressDuration(time.Duration(remaining*float64(time.Second)))
	}
	return fmt.Sprintf("%s  %s  %5.1f%%  %s/%s  %s  %s", p.label, dataProgressBar(percent, 20), percent, formatBytes(p.done), formatBytes(p.total), rateText, eta)
}

func (p *dataProgressRenderer) writeLineLocked(line string, final bool) {
	width := dataDisplayWidth(line)
	padding := ""
	if p.lastWidth > width {
		padding = strings.Repeat(" ", p.lastWidth-width)
	}
	if p.lineActive || !final {
		fmt.Fprint(p.w, "\r")
	}
	fmt.Fprint(p.w, line, padding)
	if final {
		fmt.Fprint(p.w, "\n")
		p.lineActive = false
		p.lastWidth = 0
		return
	}
	p.lineActive = true
	p.lastWidth = width
}

func dataProgressBar(percent float64, width int) string {
	if width <= 0 {
		width = 20
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	units := int(percent / 100 * float64(width*8))
	if percent > 0 && units == 0 {
		units = 1
	}
	if units > width*8 {
		units = width * 8
	}
	full := units / 8
	partial := units % 8
	bar := strings.Repeat("█", full)
	if partial > 0 && full < width {
		partials := []string{"▏", "▎", "▍", "▌", "▋", "▊", "▉"}
		bar += partials[partial-1]
		full++
	}
	return bar + strings.Repeat("░", width-full)
}

func renderDataTable(w io.Writer, columns []dataTableColumn, rows [][]string) {
	if len(columns) == 0 {
		return
	}
	widths := make([]int, len(columns))
	headers := make([]string, len(columns))
	for idx, column := range columns {
		headers[idx] = dataTableCell(column.Title)
		widths[idx] = dataDisplayWidth(headers[idx])
	}
	cells := make([][]string, 0, len(rows))
	for _, row := range rows {
		clean := make([]string, len(columns))
		for idx := range columns {
			if idx < len(row) {
				clean[idx] = dataTableCell(row[idx])
			}
			if width := dataDisplayWidth(clean[idx]); width > widths[idx] {
				widths[idx] = width
			}
		}
		cells = append(cells, clean)
	}
	renderDataTableBorder(w, "┌", "┬", "┐", widths)
	renderDataTableRow(w, columns, headers, widths)
	renderDataTableBorder(w, "├", "┼", "┤", widths)
	for _, row := range cells {
		renderDataTableRow(w, columns, row, widths)
	}
	renderDataTableBorder(w, "└", "┴", "┘", widths)
}

func renderDataKeyValueTable(w io.Writer, rows [][]string) {
	renderDataTable(w, []dataTableColumn{
		{Title: "Field"},
		{Title: "Value"},
	}, rows)
}

func renderDataAssetsTable(w io.Writer, assets []model.DataAsset) {
	rows := make([][]string, 0, len(assets))
	for _, asset := range assets {
		rows = append(rows, []string{asset.Name, asset.Path})
	}
	renderDataTable(w, []dataTableColumn{
		{Title: "Asset"},
		{Title: "Local path"},
	}, rows)
}

func renderDataWorkspaceAssetsTable(w io.Writer, assets []model.DataAsset, manifest model.DataManifest) {
	statsByAsset := manifestStatsByAsset(manifest)
	rows := make([][]string, 0, len(assets))
	for _, asset := range assets {
		stats := statsByAsset[asset.Name]
		rows = append(rows, []string{asset.Name, asset.Path, fmt.Sprintf("%d", stats.Files), formatBytes(stats.Bytes)})
	}
	renderDataTable(w, []dataTableColumn{
		{Title: "Asset"},
		{Title: "Local path"},
		{Title: "Files", Align: dataTableAlignRight},
		{Title: "Latest size", Align: dataTableAlignRight},
	}, rows)
}

func renderDataTableBorder(w io.Writer, left, middle, right string, widths []int) {
	fmt.Fprint(w, left)
	for idx, width := range widths {
		if idx > 0 {
			fmt.Fprint(w, middle)
		}
		fmt.Fprint(w, strings.Repeat("─", width+2))
	}
	fmt.Fprintln(w, right)
}

func renderDataTableRow(w io.Writer, columns []dataTableColumn, row []string, widths []int) {
	fmt.Fprint(w, "│")
	for idx := range columns {
		value := ""
		if idx < len(row) {
			value = row[idx]
		}
		fmt.Fprintf(w, " %s │", dataTablePad(value, widths[idx], columns[idx].Align))
	}
	fmt.Fprintln(w)
}

func dataTablePad(value string, width int, align dataTableAlign) string {
	padding := width - dataDisplayWidth(value)
	if padding <= 0 {
		return value
	}
	if align == dataTableAlignRight {
		return strings.Repeat(" ", padding) + value
	}
	return value + strings.Repeat(" ", padding)
}

func dataTableCell(value string) string {
	value = strings.NewReplacer("\r", " ", "\n", " ", "\t", "    ").Replace(strings.TrimSpace(value))
	if value == "" {
		return "-"
	}
	return value
}

func dataDisplayWidth(value string) int {
	width := 0
	for _, r := range value {
		switch {
		case r == '\t':
			width += 4
		case r == '\r' || r == '\n' || r < 0x20:
		case dataRuneIsWide(r):
			width += 2
		default:
			width++
		}
	}
	return width
}

func dataRuneIsWide(r rune) bool {
	return r >= 0x1100 && (r <= 0x115f ||
		r == 0x2329 || r == 0x232a ||
		(r >= 0x2e80 && r <= 0xa4cf) ||
		(r >= 0xac00 && r <= 0xd7a3) ||
		(r >= 0xf900 && r <= 0xfaff) ||
		(r >= 0xfe10 && r <= 0xfe19) ||
		(r >= 0xfe30 && r <= 0xfe6f) ||
		(r >= 0xff00 && r <= 0xff60) ||
		(r >= 0xffe0 && r <= 0xffe6))
}

func formatProgressDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	totalSeconds := int64(d / time.Second)
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	if hours > 0 {
		return fmt.Sprintf("%dh%02dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

func totalDataBlobBytes(blobs []dataBlobPlan) int64 {
	var total int64
	for _, blob := range blobs {
		if blob.Size > 0 {
			total += blob.Size
		}
	}
	return total
}

func totalManifestFileBytes(entries []model.DataManifestEntry) int64 {
	var total int64
	for _, entry := range entries {
		if entry.Kind == model.DataManifestEntryKindFile && entry.Size > 0 {
			total += entry.Size
		}
	}
	return total
}

func digestDataManifest(manifest model.DataManifest) string {
	manifest = model.NormalizeDataManifest(manifest)
	raw, _ := json.Marshal(manifest.Entries)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func shortDataDigest(digest string) string {
	digest = strings.TrimSpace(digest)
	if len(digest) <= 12 {
		return digest
	}
	return digest[:12]
}

func dataPlanBlobByDigest(blobs []dataBlobPlan, digest string) (dataBlobPlan, bool) {
	for _, blob := range blobs {
		if strings.EqualFold(blob.SHA256, digest) {
			return blob, true
		}
	}
	return dataBlobPlan{}, false
}

func mergeDataPlanBlobsWithState(blobs []dataBlobPlan, state dataTransferState) []dataBlobPlan {
	out := make([]dataBlobPlan, 0, len(blobs))
	for _, blob := range blobs {
		out = append(out, mergeDataBlobWithState(blob, state))
	}
	return out
}

func uploadResumeStateFromRefresh(refresh dataTransferAuthorizationResponse, manifestDigest string, loaded dataTransferState, loadedOK bool) dataTransferState {
	state := dataTransferState{
		TransferID:     refresh.Transfer.ID,
		Direction:      model.DataTransferDirectionUpload,
		WorkspaceID:    refresh.Transfer.WorkspaceID,
		Version:        refresh.Transfer.Version,
		ManifestDigest: manifestDigest,
	}
	if loadedOK {
		state = loaded
	}
	state.TransferID = refresh.Transfer.ID
	state.Direction = model.DataTransferDirectionUpload
	state.WorkspaceID = refresh.Transfer.WorkspaceID
	state.Version = refresh.Transfer.Version
	state.ManifestDigest = manifestDigest
	for _, blob := range refresh.Blobs {
		state = upsertDataTransferStateBlob(state, mergeDataBlobWithState(blob, state))
	}
	return state
}

func mergeDataBlobWithState(blob dataBlobPlan, state dataTransferState) dataBlobPlan {
	for _, saved := range state.Blobs {
		if !strings.EqualFold(saved.SHA256, blob.SHA256) {
			continue
		}
		if saved.UploadID != "" && blob.UploadID != "" && saved.UploadID != blob.UploadID {
			return blob
		}
		completedByNumber := map[int32]model.DataTransferPart{}
		for _, part := range saved.Parts {
			if part.Completed || strings.TrimSpace(part.ETag) != "" {
				completedByNumber[part.PartNumber] = part
			}
		}
		for idx := range blob.Parts {
			if part, ok := completedByNumber[blob.Parts[idx].PartNumber]; ok {
				blob.Parts[idx].Completed = true
				blob.Parts[idx].ETag = part.ETag
			}
		}
		if saved.Exists {
			blob.Exists = true
		}
		return blob
	}
	return blob
}

func markDataBlobUploaded(blob dataBlobPlan, parts []model.DataTransferPart) dataBlobPlan {
	blob.Exists = true
	if len(parts) > 0 {
		blob.Parts = append([]model.DataTransferPart(nil), parts...)
		for idx := range blob.Parts {
			blob.Parts[idx].Completed = true
		}
	}
	blob.UploadURL = ""
	blob.DownloadURL = ""
	for idx := range blob.Parts {
		blob.Parts[idx].UploadURL = ""
		blob.Parts[idx].DownloadURL = ""
	}
	return blob
}

func upsertDataTransferStateBlob(state dataTransferState, blob dataBlobPlan) dataTransferState {
	for idx := range state.Blobs {
		if strings.EqualFold(state.Blobs[idx].SHA256, blob.SHA256) {
			state.Blobs[idx] = sanitizeDataStateBlob(blob)
			return state
		}
	}
	state.Blobs = append(state.Blobs, sanitizeDataStateBlob(blob))
	return state
}

func sanitizeDataStateBlob(blob dataBlobPlan) dataBlobPlan {
	blob.Parts = append([]model.DataTransferPart(nil), blob.Parts...)
	blob.UploadURL = ""
	blob.DownloadURL = ""
	blob.ExpiresAt = time.Time{}
	for idx := range blob.Parts {
		blob.Parts[idx].UploadURL = ""
		blob.Parts[idx].DownloadURL = ""
		blob.Parts[idx].ExpiresAt = time.Time{}
	}
	return blob
}

func cloneDataTransferState(state dataTransferState) dataTransferState {
	state.Blobs = append([]dataBlobPlan(nil), state.Blobs...)
	for idx := range state.Blobs {
		state.Blobs[idx] = sanitizeDataStateBlob(state.Blobs[idx])
	}
	return state
}

func dataTransferStatePath(root, transferID string) string {
	return filepath.Join(root, dataTransferStateDir, strings.TrimSpace(transferID)+".json")
}

func saveDataTransferState(root string, state dataTransferState) error {
	dataTransferStateMu.Lock()
	defer dataTransferStateMu.Unlock()
	return saveDataTransferStateLocked(root, state)
}

func saveDataTransferStateLocked(root string, state dataTransferState) error {
	if strings.TrimSpace(state.TransferID) == "" {
		return fmt.Errorf("transfer id is required for transfer state")
	}
	now := time.Now().UTC()
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now
	state.Blobs = append([]dataBlobPlan(nil), state.Blobs...)
	for idx := range state.Blobs {
		state.Blobs[idx] = sanitizeDataStateBlob(state.Blobs[idx])
	}
	dir := filepath.Join(root, dataTransferStateDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	targetPath := dataTransferStatePath(root, state.TransferID)
	tmp, err := os.CreateTemp(dir, "."+strings.TrimSpace(state.TransferID)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(dataTransferStatePerm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, targetPath)
}

func loadDataTransferState(root, transferID string) (dataTransferState, bool, error) {
	dataTransferStateMu.Lock()
	defer dataTransferStateMu.Unlock()
	return loadDataTransferStateLocked(root, transferID)
}

func loadDataTransferStateLocked(root, transferID string) (dataTransferState, bool, error) {
	raw, err := os.ReadFile(dataTransferStatePath(root, transferID))
	if err != nil {
		if os.IsNotExist(err) {
			return dataTransferState{}, false, nil
		}
		return dataTransferState{}, false, err
	}
	var state dataTransferState
	if err := json.Unmarshal(raw, &state); err != nil {
		return dataTransferState{}, false, fmt.Errorf("read transfer state %s: %w", transferID, err)
	}
	return state, true, nil
}

func updateDataTransferState(root, transferID string, update func(dataTransferState, bool) (dataTransferState, error)) (dataTransferState, error) {
	if strings.TrimSpace(transferID) == "" {
		return dataTransferState{}, fmt.Errorf("transfer id is required for transfer state")
	}
	if update == nil {
		return dataTransferState{}, fmt.Errorf("transfer state update function is required")
	}
	dataTransferStateMu.Lock()
	defer dataTransferStateMu.Unlock()
	loaded, ok, err := loadDataTransferStateLocked(root, transferID)
	if err != nil {
		return dataTransferState{}, err
	}
	next, err := update(loaded, ok)
	if err != nil {
		return dataTransferState{}, err
	}
	if next.TransferID == "" {
		next.TransferID = transferID
	}
	if err := saveDataTransferStateLocked(root, next); err != nil {
		return dataTransferState{}, err
	}
	return next, nil
}

func findDataTransferState(root, direction, workspaceID, snapshotID, manifestDigest string) (dataTransferState, bool, error) {
	dataTransferStateMu.Lock()
	defer dataTransferStateMu.Unlock()
	return findDataTransferStateLocked(root, direction, workspaceID, snapshotID, manifestDigest)
}

func findDataTransferStateLocked(root, direction, workspaceID, snapshotID, manifestDigest string) (dataTransferState, bool, error) {
	dir := filepath.Join(root, dataTransferStateDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return dataTransferState{}, false, nil
		}
		return dataTransferState{}, false, err
	}
	var best dataTransferState
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return dataTransferState{}, false, err
		}
		var state dataTransferState
		if err := json.Unmarshal(raw, &state); err != nil {
			continue
		}
		if direction != "" && state.Direction != direction {
			continue
		}
		if workspaceID != "" && state.WorkspaceID != workspaceID {
			continue
		}
		if snapshotID != "" && state.SnapshotID != snapshotID {
			continue
		}
		if manifestDigest != "" && state.ManifestDigest != manifestDigest {
			continue
		}
		if best.TransferID == "" || state.UpdatedAt.After(best.UpdatedAt) {
			best = state
		}
	}
	if best.TransferID == "" {
		return dataTransferState{}, false, nil
	}
	return best, true, nil
}

func removeDataTransferState(root, transferID string) error {
	dataTransferStateMu.Lock()
	defer dataTransferStateMu.Unlock()
	err := os.Remove(dataTransferStatePath(root, transferID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (c *Client) ListDataWorkspaces() ([]model.DataWorkspace, error) {
	var resp struct {
		Workspaces []model.DataWorkspace `json:"workspaces"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/data/workspaces", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Workspaces, nil
}

func (c *Client) CreateDataWorkspace(req map[string]any) (model.DataWorkspace, error) {
	var resp struct {
		Workspace model.DataWorkspace `json:"workspace"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/data/workspaces", req, &resp); err != nil {
		return model.DataWorkspace{}, err
	}
	return resp.Workspace, nil
}

func (c *Client) GetDataWorkspace(idOrName string) (dataWorkspaceEnvelope, error) {
	var resp dataWorkspaceEnvelope
	if err := c.doJSON(http.MethodGet, path.Join("/v1/data/workspaces", idOrName), nil, &resp); err != nil {
		return dataWorkspaceEnvelope{}, err
	}
	return resp, nil
}

func (c *Client) PatchDataWorkspace(id string, req map[string]any) (dataWorkspaceEnvelope, error) {
	var resp dataWorkspaceEnvelope
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/data/workspaces", id), req, &resp); err != nil {
		return dataWorkspaceEnvelope{}, err
	}
	return resp, nil
}

func (c *Client) SweepDataWorkspaceGC(workspaceID string, retentionDays int, dryRun, confirm bool) (dataGCSweepResponse, error) {
	var resp dataGCSweepResponse
	req := map[string]any{"retention_days": retentionDays, "dry_run": dryRun, "confirm": confirm}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/workspaces", workspaceID, "gc/sweep"), req, &resp); err != nil {
		return dataGCSweepResponse{}, err
	}
	return resp, nil
}

func (c *Client) ListDataSnapshots(workspaceID string) ([]model.DataSnapshot, error) {
	var resp struct {
		Snapshots []model.DataSnapshot `json:"snapshots"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/data/workspaces", workspaceID, "snapshots"), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Snapshots, nil
}

func (c *Client) GetDataSnapshot(workspaceID, snapshotID string) (dataSnapshotEnvelope, error) {
	var resp dataSnapshotEnvelope
	if err := c.doJSON(http.MethodGet, path.Join("/v1/data/workspaces", workspaceID, "snapshots", snapshotID), nil, &resp); err != nil {
		return dataSnapshotEnvelope{}, err
	}
	return resp, nil
}

func (c *Client) PlanDataUpload(workspaceID, version, message string, manifest model.DataManifest) (dataUploadPlanResponse, error) {
	var resp dataUploadPlanResponse
	relative := path.Join("/v1/data/workspaces", workspaceID, "transfers/plan-upload") + "?blob_limit=" + strconv.Itoa(dataTransferBlobPageLimit)
	if err := c.doJSONWithTimeout(http.MethodPost, relative, map[string]any{"version": version, "message": message, "manifest": manifest}, &resp, dataControlPlaneRequestTimeout); err != nil {
		return dataUploadPlanResponse{}, err
	}
	return resp, nil
}

func (c *Client) PlanDataDownload(workspaceID, version string, assets []string) (dataDownloadPlanResponse, error) {
	var resp dataDownloadPlanResponse
	relative := path.Join("/v1/data/workspaces", workspaceID, "transfers/plan-download") + "?blob_limit=" + strconv.Itoa(dataTransferBlobPageLimit)
	if err := c.doJSONWithTimeout(http.MethodPost, relative, map[string]any{"version": version, "assets": assets}, &resp, dataControlPlaneRequestTimeout); err != nil {
		return dataDownloadPlanResponse{}, err
	}
	return resp, nil
}

func (c *Client) CompleteDataTransfer(transferID string, req map[string]any) (dataTransferCompleteResponse, error) {
	var resp dataTransferCompleteResponse
	if err := c.doJSONWithTimeout(http.MethodPost, path.Join("/v1/data/transfers", transferID, "complete"), req, &resp, dataControlPlaneRequestTimeout); err != nil {
		return dataTransferCompleteResponse{}, err
	}
	return resp, nil
}

func (c *Client) RefreshDataTransferAuthorization(transferID string) (dataTransferAuthorizationResponse, error) {
	return c.RefreshDataTransferAuthorizationPage(transferID, -1, 0)
}

func (c *Client) RefreshDataTransferAuthorizationPage(transferID string, offset, limit int) (dataTransferAuthorizationResponse, error) {
	var resp dataTransferAuthorizationResponse
	relative := path.Join("/v1/data/transfers", transferID, "refresh")
	query := url.Values{}
	if offset >= 0 {
		query.Set("blob_offset", strconv.Itoa(offset))
	}
	if limit > 0 {
		query.Set("blob_limit", strconv.Itoa(limit))
	}
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	if err := c.doDataTransferControlPlaneJSONWithRetry(http.MethodPost, relative, nil, &resp, dataControlPlaneRequestTimeout); err != nil {
		return dataTransferAuthorizationResponse{}, err
	}
	return resp, nil
}

func (c *Client) CheckpointDataTransfer(transferID string, bytesDone int64, filesDone int, blobs []dataBlobPlan) error {
	req := map[string]any{}
	if bytesDone >= 0 {
		req["bytes_done"] = bytesDone
	}
	if filesDone >= 0 {
		req["files_done"] = filesDone
	}
	if len(blobs) > 0 {
		req["blobs"] = blobs
	}
	if len(req) == 0 {
		return nil
	}
	var resp map[string]any
	return c.doDataTransferControlPlaneJSONWithRetry(http.MethodPost, path.Join("/v1/data/transfers", transferID, "checkpoint"), req, &resp, dataControlPlaneRequestTimeout)
}

func (c *Client) doDataTransferControlPlaneJSONWithRetry(method, relativePath string, requestBody any, responseBody any, timeout time.Duration) error {
	var lastErr error
	for attempt := 1; attempt <= dataControlPlaneTransferMaxAttempts; attempt++ {
		err := c.doDataTransferControlPlaneJSON(method, relativePath, requestBody, responseBody, timeout)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransientDataControlPlaneError(err) || attempt == dataControlPlaneTransferMaxAttempts {
			return err
		}
		dataControlPlaneRetrySleep(dataControlPlaneRetryDelay(attempt))
	}
	return lastErr
}

func (c *Client) doDataTransferControlPlaneJSON(method, relativePath string, requestBody any, responseBody any, timeout time.Duration) error {
	payload, err := c.doDataTransferControlPlaneJSONRaw(method, relativePath, requestBody, timeout)
	if err != nil {
		return err
	}
	if responseBody == nil {
		return nil
	}
	if err := json.Unmarshal(payload, responseBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) doDataTransferControlPlaneJSONRaw(method, relativePath string, requestBody any, timeout time.Duration) ([]byte, error) {
	var body io.Reader
	if requestBody != nil {
		raw, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, c.resolveURL(relativePath), body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	if requestBody != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	return c.do(httpReq)
}

func isTransientDataControlPlaneError(err error) bool {
	if err == nil {
		return false
	}
	if isRetryableHTTPClientError(err) {
		return true
	}
	if statusCode, retryable, ok := dataControlPlaneErrorStatus(err); ok {
		if retryable {
			return true
		}
		switch statusCode {
		case http.StatusUnauthorized, http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "unexpected eof") ||
		strings.Contains(lower, "connection reset") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "server closed idle connection")
}

func dataControlPlaneErrorStatus(err error) (int, bool, bool) {
	var apiErr *apiServerError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode, apiErr.IsRetryable(), true
	}
	lower := strings.ToLower(err.Error())
	idx := strings.Index(lower, "status=")
	if idx < 0 {
		return 0, false, false
	}
	start := idx + len("status=")
	end := start
	for end < len(lower) && lower[end] >= '0' && lower[end] <= '9' {
		end++
	}
	if end == start {
		return 0, false, false
	}
	statusCode, parseErr := strconv.Atoi(lower[start:end])
	if parseErr != nil {
		return 0, false, false
	}
	return statusCode, false, true
}

func dataControlPlaneRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Duration(250*(1<<(attempt-1))) * time.Millisecond
	if delay > 5*time.Second {
		return 5 * time.Second
	}
	return delay
}

func (c *Client) ListDataMultipartParts(transferID, sha256Digest string) ([]model.DataTransferPart, error) {
	var resp struct {
		Parts []model.DataTransferPart `json:"parts"`
	}
	relative := path.Join("/v1/data/transfers", transferID, "multipart/parts") + "?sha256=" + url.QueryEscape(sha256Digest)
	if err := c.doJSON(http.MethodGet, relative, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Parts, nil
}

func (c *Client) CompleteDataMultipartUpload(transferID, sha256Digest, uploadID string, parts []model.DataTransferPart) (map[string]any, error) {
	var resp map[string]any
	req := map[string]any{"sha256": sha256Digest, "upload_id": uploadID, "parts": parts}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/transfers", transferID, "multipart/complete"), req, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) AbortDataMultipartUpload(transferID, sha256Digest, uploadID string) (map[string]any, error) {
	var resp map[string]any
	req := map[string]any{"sha256": sha256Digest, "upload_id": uploadID}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/transfers", transferID, "multipart/abort"), req, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) PutDataBlob(uploadURL, sourcePath string) error {
	return c.PutDataBlobWithProgress(uploadURL, sourcePath, nil)
}

func (c *Client) PutDataBlobWithProgress(uploadURL, sourcePath string, progress dataTransferProgress) error {
	var lastErr error
	for attempt := 1; attempt <= dataObjectTransferMaxAttempts; attempt++ {
		var attemptBytes int64
		err := c.putDataBlobWithProgressOnce(uploadURL, sourcePath, func(delta int64) {
			attemptBytes += delta
			if progress != nil {
				progress(delta)
			}
		})
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransientDataObjectError(err) || attempt == dataObjectTransferMaxAttempts {
			return err
		}
		if progress != nil && attemptBytes > 0 {
			progress(-attemptBytes)
		}
		time.Sleep(dataObjectRetryDelay(attempt))
	}
	return lastErr
}

func (c *Client) putDataBlobWithProgressOnce(uploadURL, sourcePath string, progress dataTransferProgress) error {
	file, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer file.Close()
	body := io.Reader(file)
	if progress != nil {
		body = dataProgressReader{r: file, progress: progress}
	}
	req, err := http.NewRequest(http.MethodPut, uploadURL, body)
	if err != nil {
		return err
	}
	if info, err := file.Stat(); err == nil {
		req.ContentLength = info.Size()
	}
	if isFugueManagedDataBlobURL(uploadURL) {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return c.doDataObjectRequest(req)
}

func (c *Client) UploadDataBlobPart(uploadURL, sourcePath string, offset, size int64) (string, error) {
	return c.UploadDataBlobPartWithProgress(uploadURL, sourcePath, offset, size, nil)
}

func (c *Client) UploadDataBlobPartWithProgress(uploadURL, sourcePath string, offset, size int64, progress dataTransferProgress) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= dataObjectTransferMaxAttempts; attempt++ {
		var attemptBytes int64
		etag, err := c.uploadDataBlobPartWithProgressOnce(uploadURL, sourcePath, offset, size, func(delta int64) {
			attemptBytes += delta
			if progress != nil {
				progress(delta)
			}
		})
		if err == nil {
			return etag, nil
		}
		lastErr = err
		if !isTransientDataObjectError(err) || attempt == dataObjectTransferMaxAttempts {
			return "", err
		}
		if progress != nil && attemptBytes > 0 {
			progress(-attemptBytes)
		}
		time.Sleep(dataObjectRetryDelay(attempt))
	}
	return "", lastErr
}

func (c *Client) uploadDataBlobPartWithProgressOnce(uploadURL, sourcePath string, offset, size int64, progress dataTransferProgress) (string, error) {
	if strings.TrimSpace(uploadURL) == "" {
		return "", fmt.Errorf("multipart part upload url is missing")
	}
	if size <= 0 {
		return "", fmt.Errorf("multipart part size must be positive")
	}
	file, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	body := io.Reader(io.NewSectionReader(file, offset, size))
	if progress != nil {
		body = dataProgressReader{r: body, progress: progress}
	}
	req, err := http.NewRequest(http.MethodPut, uploadURL, body)
	if err != nil {
		return "", err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.doDataObjectRequestWithResponse(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	etag := strings.Trim(resp.Header.Get("ETag"), "\"")
	if etag == "" {
		return "", fmt.Errorf("multipart part upload did not return an ETag")
	}
	return etag, nil
}

func (c *Client) DownloadDataBlob(downloadURL, targetPath, expectedSHA string, size int64, resume, overwrite bool, concurrency int) error {
	return c.DownloadDataBlobWithProgress(downloadURL, targetPath, expectedSHA, size, resume, overwrite, concurrency, nil)
}

func (c *Client) DownloadDataBlobWithProgress(downloadURL, targetPath, expectedSHA string, size int64, resume, overwrite bool, concurrency int, progress dataTransferProgress) error {
	if size > dataDownloadPartSize && concurrency > 1 {
		return c.downloadDataBlobMultipart(downloadURL, targetPath, expectedSHA, size, resume, overwrite, concurrency, dataDownloadPartSize, progress)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}
	tmpDir := filepath.Join(".fugue", "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	tmpPath := filepath.Join(tmpDir, expectedSHA+".part")
	if !resume {
		_ = os.Remove(tmpPath)
	}
	var offset int64
	if info, err := os.Stat(tmpPath); err == nil {
		offset = info.Size()
		if offset > 0 {
			if sum, err := sha256LocalFile(tmpPath); err == nil && sum == expectedSHA {
				if overwrite {
					_ = os.Remove(targetPath)
				}
				if progress != nil {
					progress(size)
				}
				return os.Rename(tmpPath, targetPath)
			}
			if progress != nil {
				progress(offset)
			}
		}
	}
	flags := os.O_CREATE | os.O_WRONLY
	if offset > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	tmp, err := os.OpenFile(tmpPath, flags, 0o600)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodGet, downloadURL, nil)
	if err != nil {
		tmp.Close()
		return err
	}
	if isFugueManagedDataBlobURL(downloadURL) {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := c.doRawDataObjectRequest(req)
	if err != nil {
		tmp.Close()
		return err
	}
	defer resp.Body.Close()
	if offset > 0 && resp.StatusCode == http.StatusOK {
		if err := tmp.Truncate(0); err != nil {
			tmp.Close()
			return err
		}
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			tmp.Close()
			return err
		}
		if progress != nil {
			progress(-offset)
		}
		offset = 0
	}
	if offset > 0 && resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		tmp.Close()
		_ = os.Remove(tmpPath)
		if progress != nil {
			progress(-offset)
		}
		return c.DownloadDataBlobWithProgress(downloadURL, targetPath, expectedSHA, size, false, overwrite, concurrency, progress)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusBadRequest {
		tmp.Close()
		return errDataAuthorizationExpired
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		tmp.Close()
		return fmt.Errorf("download failed: status=%d", resp.StatusCode)
	}
	body := io.Reader(resp.Body)
	if progress != nil {
		body = dataProgressReader{r: resp.Body, progress: progress}
	}
	if _, err := io.Copy(tmp, body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	sum, err := sha256LocalFile(tmpPath)
	if err != nil {
		return err
	}
	if sum != expectedSHA {
		return fmt.Errorf("download checksum mismatch for %s", targetPath)
	}
	if overwrite {
		_ = os.Remove(targetPath)
	}
	return os.Rename(tmpPath, targetPath)
}

func (c *Client) downloadDataBlobMultipart(downloadURL, targetPath, expectedSHA string, size int64, resume, overwrite bool, concurrency int, partSize int64, progress dataTransferProgress) error {
	if size <= 0 {
		return c.DownloadDataBlobWithProgress(downloadURL, targetPath, expectedSHA, size, resume, overwrite, 1, progress)
	}
	if partSize <= 0 {
		partSize = dataDownloadPartSize
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}
	tmpDir := filepath.Join(".fugue", "tmp")
	partDir := filepath.Join(tmpDir, expectedSHA+".download-parts")
	tmpPath := filepath.Join(tmpDir, expectedSHA+".part")
	if !resume {
		_ = os.Remove(tmpPath)
		_ = os.RemoveAll(partDir)
	}
	if err := os.MkdirAll(partDir, 0o700); err != nil {
		return err
	}
	partCount := int((size + partSize - 1) / partSize)
	if concurrency > partCount {
		concurrency = partCount
	}
	type downloadPart struct {
		index  int
		offset int64
		size   int64
		path   string
	}
	parts := make([]downloadPart, 0, partCount)
	for idx := 0; idx < partCount; idx++ {
		offset := int64(idx) * partSize
		currentSize := partSize
		if remaining := size - offset; remaining < currentSize {
			currentSize = remaining
		}
		parts = append(parts, downloadPart{
			index:  idx,
			offset: offset,
			size:   currentSize,
			path:   filepath.Join(partDir, fmt.Sprintf("%06d.part", idx+1)),
		})
	}
	jobs := make(chan downloadPart)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for part := range jobs {
			if err := c.downloadDataBlobRangePart(downloadURL, part.path, part.offset, part.size, resume, progress); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	for _, part := range parts {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case jobs <- part:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	for _, part := range parts {
		file, err := os.Open(part.path)
		if err != nil {
			_ = tmp.Close()
			return err
		}
		if _, err := io.Copy(tmp, file); err != nil {
			_ = file.Close()
			_ = tmp.Close()
			return err
		}
		if err := file.Close(); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	sum, err := sha256LocalFile(tmpPath)
	if err != nil {
		return err
	}
	if sum != expectedSHA {
		return fmt.Errorf("download checksum mismatch for %s", targetPath)
	}
	if overwrite {
		_ = os.Remove(targetPath)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return err
	}
	_ = os.RemoveAll(partDir)
	return nil
}

func (c *Client) downloadDataBlobRangePart(downloadURL, partPath string, offset, size int64, resume bool, progress dataTransferProgress) error {
	if size <= 0 {
		return nil
	}
	if !resume {
		_ = os.Remove(partPath)
	}
	var existing int64
	if info, err := os.Stat(partPath); err == nil {
		existing = info.Size()
		if existing == size {
			if progress != nil {
				progress(size)
			}
			return nil
		}
		if existing > size {
			if err := os.Remove(partPath); err != nil {
				return err
			}
			existing = 0
		} else if existing > 0 && progress != nil {
			progress(existing)
		}
	}
	file, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	start := offset + existing
	end := offset + size - 1
	req, err := http.NewRequest(http.MethodGet, downloadURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	if isFugueManagedDataBlobURL(downloadURL) {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.doRawDataObjectRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusBadRequest {
		return errDataAuthorizationExpired
	}
	if resp.StatusCode != http.StatusPartialContent {
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			_ = os.Remove(partPath)
			if existing > 0 && progress != nil {
				progress(-existing)
			}
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if trimmed := strings.TrimSpace(string(body)); trimmed != "" {
			return fmt.Errorf("range download failed: status=%d body=%s", resp.StatusCode, trimmed)
		}
		return fmt.Errorf("range download failed: status=%d", resp.StatusCode)
	}
	body := io.Reader(resp.Body)
	if progress != nil {
		body = dataProgressReader{r: resp.Body, progress: progress}
	}
	if _, err := io.Copy(file, body); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != size {
		return fmt.Errorf("range download incomplete for %s: got %d, expected %d", partPath, info.Size(), size)
	}
	return nil
}

func (c *Client) doDataObjectRequest(req *http.Request) error {
	resp, err := c.doDataObjectRequestWithResponse(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}

func (c *Client) doDataObjectRequestWithResponse(req *http.Request) (*http.Response, error) {
	resp, err := c.doRawDataObjectRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusBadRequest {
		_ = resp.Body.Close()
		return nil, errDataAuthorizationExpired
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, dataObjectTransferError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	return resp, nil
}

func (c *Client) doRawDataObjectRequest(req *http.Request) (*http.Response, error) {
	if isFugueManagedDataBlobURL(req.URL.String()) && strings.TrimSpace(req.Header.Get("Authorization")) == "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	httpClient := c.dataObjectHTTPClient
	if httpClient == nil {
		httpClient = c.httpClient
	}
	if httpClient == nil {
		return nil, fmt.Errorf("data object http client is not configured")
	}
	return httpClient.Do(req)
}

func newDataObjectHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   dataObjectDialTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.TLSHandshakeTimeout = dataObjectTLSHandshakeTimeout
	transport.ResponseHeaderTimeout = dataObjectResponseHeaderTimeout
	transport.ExpectContinueTimeout = time.Second
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: transport}
}

type dataObjectTransferError struct {
	StatusCode int
	Body       string
}

func (e dataObjectTransferError) Error() string {
	if strings.TrimSpace(e.Body) != "" {
		return fmt.Sprintf("object transfer failed: status=%d body=%s", e.StatusCode, e.Body)
	}
	return fmt.Sprintf("object transfer failed: status=%d", e.StatusCode)
}

func isTransientDataObjectError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() || strings.Contains(strings.ToLower(urlErr.Error()), "connection reset") || strings.Contains(strings.ToLower(urlErr.Error()), "connection refused") {
			return true
		}
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "connection reset") || strings.Contains(lower, "connection refused") || strings.Contains(lower, "unexpected eof") {
		return true
	}
	var objectErr dataObjectTransferError
	if !errors.As(err, &objectErr) {
		return false
	}
	switch objectErr.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func dataObjectRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Duration(200*(1<<(attempt-1))) * time.Millisecond
	if delay > 3*time.Second {
		return 3 * time.Second
	}
	return delay
}

func isFugueManagedDataBlobURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.Contains(parsed.Path, "/v1/data/blobs/")
}

func (c *Client) ListDataTransfers() ([]model.DataTransfer, error) {
	var resp struct {
		Transfers []model.DataTransfer `json:"transfers"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/data/transfers", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Transfers, nil
}

func (c *Client) GetDataTransfer(id string) (map[string]any, error) {
	var resp map[string]any
	relative := path.Join("/v1/data/transfers", id) + "?summary=true"
	if err := c.doJSON(http.MethodGet, relative, nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) GetDataTransferModel(id string) (model.DataTransfer, error) {
	var resp struct {
		Transfer model.DataTransfer `json:"transfer"`
	}
	relative := path.Join("/v1/data/transfers", id) + "?summary=true"
	if err := c.doJSON(http.MethodGet, relative, nil, &resp); err != nil {
		return model.DataTransfer{}, err
	}
	return resp.Transfer, nil
}

func (c *Client) CancelDataTransfer(id string) (map[string]any, error) {
	var resp map[string]any
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/transfers", id, "cancel"), nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ListDataBackends() ([]model.DataBackend, error) {
	var resp struct {
		Backends []model.DataBackend `json:"backends"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/data/backends", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Backends, nil
}

func (c *Client) CreateDataBackend(req map[string]any) (model.DataBackend, error) {
	var resp struct {
		Backend model.DataBackend `json:"backend"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/data/backends", req, &resp); err != nil {
		return model.DataBackend{}, err
	}
	return resp.Backend, nil
}

func (c *Client) GetDataBackend(id string) (model.DataBackend, error) {
	var resp struct {
		Backend model.DataBackend `json:"backend"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/data/backends", id), nil, &resp); err != nil {
		return model.DataBackend{}, err
	}
	return resp.Backend, nil
}

func (c *Client) DeleteDataBackend(id string) (map[string]any, error) {
	var resp map[string]any
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/data/backends", id), nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) RotateDataBackendCredentials(id string, credentials model.DataBackendCredentials) (model.DataBackend, error) {
	var resp struct {
		Backend model.DataBackend `json:"backend"`
	}
	req := map[string]any{"credentials": credentials}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/backends", id, "credentials"), req, &resp); err != nil {
		return model.DataBackend{}, err
	}
	return resp.Backend, nil
}

func (c *Client) CreateDataBackendMigration(workspaceID, targetBackendID string, dryRun, cutover bool) (dataBackendMigrationResponse, error) {
	var resp dataBackendMigrationResponse
	req := map[string]any{"target_backend_id": targetBackendID, "dry_run": dryRun, "cutover": cutover}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/workspaces", workspaceID, "backend-migrations"), req, &resp); err != nil {
		return dataBackendMigrationResponse{}, err
	}
	return resp, nil
}

func (c *Client) RollbackDataBackendMigration(workspaceID, transferID string) (dataBackendMigrationResponse, error) {
	var resp dataBackendMigrationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/workspaces", workspaceID, "backend-migrations", transferID, "rollback"), nil, &resp); err != nil {
		return dataBackendMigrationResponse{}, err
	}
	return resp, nil
}

func (c *Client) CreateDataGrant(workspaceID, snapshotID, mode string, expiresInMinutes int) (map[string]any, error) {
	var resp map[string]any
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/workspaces", workspaceID, "grants"), map[string]any{"snapshot_id": snapshotID, "mode": mode, "expires_in_minutes": expiresInMinutes}, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) RevokeDataGrant(id string) (map[string]any, error) {
	var resp map[string]any
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/data/grants", id), nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ListDataWorkspaceAccessGrants(workspaceID string) ([]model.DataWorkspaceAccessGrant, error) {
	var resp struct {
		Grants []model.DataWorkspaceAccessGrant `json:"grants"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/data/workspaces", workspaceID, "access"), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Grants, nil
}

func (c *Client) GrantDataWorkspaceAccess(workspaceID, subjectType, subjectID, role string) (model.DataWorkspaceAccessGrant, error) {
	var resp struct {
		Grant model.DataWorkspaceAccessGrant `json:"grant"`
	}
	req := map[string]any{
		"subject_type": subjectType,
		"subject_id":   subjectID,
		"role":         role,
	}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/workspaces", workspaceID, "access"), req, &resp); err != nil {
		return model.DataWorkspaceAccessGrant{}, err
	}
	return resp.Grant, nil
}

func (c *Client) RevokeDataWorkspaceAccess(workspaceID, subjectType, subjectID string) (bool, error) {
	var resp struct {
		Removed bool `json:"removed"`
	}
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/data/workspaces", workspaceID, "access", subjectType, subjectID), nil, &resp); err != nil {
		return false, err
	}
	return resp.Removed, nil
}

func dataURLJoin(rawBase, rawPath string) string {
	u, err := url.Parse(rawBase)
	if err != nil {
		return rawBase
	}
	u.Path = path.Join(u.Path, rawPath)
	return u.String()
}

var errDataNoop = errors.New("data noop")
var errDataAuthorizationExpired = errors.New("data object authorization expired")
