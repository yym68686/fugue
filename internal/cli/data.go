package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
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
	dataConfigPath                          = ".fugue/data.yaml"
	dataTransferStateDir                    = ".fugue/transfers"
	dataTransferStatePerm                   = 0o600
	dataDownloadPartSize                    = 64 * 1024 * 1024
	defaultUntrackedLargeDirThreshold int64 = 1 << 30
)

var defaultDataIgnore = []string{
	".git",
	".venv",
	"__pycache__",
	"*.tmp",
	"*.lock",
	".DS_Store",
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
	Workspace model.DataWorkspace `json:"workspace"`
	Transfer  model.DataTransfer  `json:"transfer"`
	Manifest  model.DataManifest  `json:"manifest"`
	Blobs     []dataBlobPlan      `json:"blobs"`
}

type dataDownloadPlanResponse struct {
	Workspace model.DataWorkspace `json:"workspace"`
	Snapshot  model.DataSnapshot  `json:"snapshot"`
	Transfer  model.DataTransfer  `json:"transfer"`
	Manifest  model.DataManifest  `json:"manifest"`
	Blobs     []dataBlobPlan      `json:"blobs"`
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
	Workspace model.DataWorkspace `json:"workspace"`
	Transfer  model.DataTransfer  `json:"transfer"`
	Blobs     []dataBlobPlan      `json:"blobs"`
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
	w       io.Writer
	enabled bool
	label   string
	total   int64
	done    int64
	last    time.Time
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
				for _, asset := range cfg.Assets {
					fmt.Fprintf(c.stdout, "  %-14s %s\n", asset.Name, asset.Path)
				}
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
			fmt.Fprintf(c.stdout, "Data workspace: %s\n\n", cfg.Workspace)
			fmt.Fprintln(c.stdout, "Tracked assets:")
			for _, asset := range cfg.Assets {
				fmt.Fprintf(c.stdout, "  %-14s %s\n", asset.Name, asset.Path)
			}
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
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show local data workspace status",
		Example: strings.TrimSpace(`
  fugue data status
  fugue data status --json
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			manifest, pathsByDigest, err := scanDataManifest(".", cfg, "")
			if err != nil {
				return err
			}
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
			}
			untrackedLargeDirs, err := findUntrackedLargeDataDirectories(".", cfg, dataUntrackedLargeDirThreshold())
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": workspace, "local_manifest": manifest, "latest_snapshot": latest, "untracked_large_directories": untrackedLargeDirs})
			}
			fmt.Fprintf(c.stdout, "Data workspace: %s\n", workspace.Name)
			if cfg.Project != "" {
				fmt.Fprintf(c.stdout, "Project: %s\n", cfg.Project)
			}
			if latest.ID != "" {
				fmt.Fprintf(c.stdout, "Latest version: %s\n", latest.Version)
			} else {
				fmt.Fprintln(c.stdout, "Latest version: none")
			}
			fmt.Fprintln(c.stdout, "\nASSET          LOCAL PATH        STATUS       FILES       SIZE")
			localByAsset := manifestStatsByAsset(manifest)
			remoteByAsset := manifestStatsByAsset(latest.Manifest)
			for _, asset := range cfg.Assets {
				stats := localByAsset[asset.Name]
				status := "new"
				if _, err := os.Stat(asset.Path); os.IsNotExist(err) {
					status = "missing"
				} else if latest.ID != "" {
					if manifestAssetDigest(manifest, asset.Name) == manifestAssetDigest(latest.Manifest, asset.Name) {
						status = "unchanged"
					} else if _, ok := remoteByAsset[asset.Name]; ok {
						status = "changed"
					}
				}
				fmt.Fprintf(c.stdout, "%-14s %-17s %-12s %-10d %s\n", asset.Name, asset.Path, status, stats.Files, formatBytes(stats.Bytes))
			}
			if len(untrackedLargeDirs) > 0 {
				fmt.Fprintln(c.stdout, "\nUntracked large directories:")
				for _, dir := range untrackedLargeDirs {
					fmt.Fprintf(c.stdout, "  %s  %s\n", dir.Path, formatBytes(dir.Bytes))
				}
				fmt.Fprintln(c.stdout, "Run `fugue data track <path>` if this directory is part of the training workspace.")
			}
			return nil
		},
	}
	return cmd
}

func (c *CLI) newDataPushCommand() *cobra.Command {
	var version, message, assetName string
	var dryRun, noResume, noProgress bool
	var concurrency int
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
			manifest, pathsByDigest, err := scanDataManifest(".", cfg, assetName)
			if err != nil {
				return err
			}
			if dryRun {
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"workspace": workspace, "manifest": manifest, "dry_run": true})
				}
				fmt.Fprintf(c.stdout, "Data workspace: %s\n\n", workspace.Name)
				fmt.Fprintf(c.stdout, "Planning upload:\n  assets: %d\n  files: %d\n  upload size: %s\n", len(cfg.Assets), manifest.FileCount, formatBytes(manifest.TotalBytes))
				return nil
			}
			manifestDigest := digestDataManifest(manifest)
			var plan dataUploadPlanResponse
			var resumed bool
			if !noResume {
				if state, ok, err := findDataTransferState(".", model.DataTransferDirectionUpload, workspace.ID, "", manifestDigest); err != nil {
					return err
				} else if ok {
					refresh, err := client.RefreshDataTransferAuthorization(state.TransferID)
					if err == nil {
						plan = dataUploadPlanResponse{
							Workspace: refresh.Workspace,
							Transfer:  refresh.Transfer,
							Manifest:  manifest,
							Blobs:     mergeDataPlanBlobsWithState(refresh.Blobs, state),
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
				fmt.Fprintf(c.stdout, "Data workspace: %s\n\n", workspace.Name)
				if resumed {
					fmt.Fprintf(c.stdout, "Resuming upload:\n  transfer: %s\n  files: %d\n  upload size: %s\n\n", plan.Transfer.ID, manifest.FileCount, formatBytes(manifest.TotalBytes))
				} else {
					fmt.Fprintf(c.stdout, "Planning upload:\n  transfer: %s\n  files: %d\n  upload size: %s\n\n", plan.Transfer.ID, manifest.FileCount, formatBytes(manifest.TotalBytes))
				}
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
			if err := c.uploadDataPlanBlobs(client, workspace.ID, plan.Transfer.ID, manifestDigest, plan.Blobs, pathsByDigest, !noResume, noProgress, concurrency); err != nil {
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
			fmt.Fprintf(c.stdout, "\nCreated version:\n  version: %s\n  id: %s\n  files: %d\n  size: %s\n", complete.Snapshot.Version, complete.Snapshot.ID, complete.Snapshot.FileCount, formatBytes(complete.Snapshot.TotalBytes))
			return nil
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "Version label. Defaults to latest")
	cmd.Flags().StringVar(&message, "message", "", "Version message")
	cmd.Flags().StringVar(&assetName, "asset", "", "Only push one asset")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show upload plan without uploading")
	cmd.Flags().IntVar(&concurrency, "concurrency", 8, "Transfer concurrency")
	cmd.Flags().BoolVar(&noResume, "no-resume", false, "Ignore local transfer resume state")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress rendering")
	return cmd
}

func (c *CLI) newDataPullCommand() *cobra.Command {
	var version, assetName, toPath string
	var verify, dryRun, keepLocal, overwrite, prune, confirm, noResume bool
	var concurrency int
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Download a data workspace version into the current project",
		Example: strings.TrimSpace(`
  fugue data pull
  fugue data pull --version before-provider-move
  fugue data pull --asset dataset
  fugue data pull --asset checkpoints --to /mnt/nvme/checkpoints
  fugue data pull --dry-run
  fugue data pull --overwrite
  fugue data pull --prune --confirm
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
			assets := []string{}
			if strings.TrimSpace(assetName) != "" {
				assets = append(assets, strings.TrimSpace(assetName))
			}
			planResp, err := client.PlanDataDownload(workspace.ID, version, assets)
			if err != nil {
				return err
			}
			targetCfg := cfg
			if toPath != "" && assetName != "" {
				for idx := range targetCfg.Assets {
					if targetCfg.Assets[idx].Name == assetName {
						targetCfg.Assets[idx].Path = toPath
					}
				}
			}
			pullPlan, err := buildPullPlan(".", targetCfg, planResp.Manifest, overwrite, keepLocal, prune)
			if err != nil {
				return err
			}
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
			_ = concurrency
			blobByDigest := map[string]dataBlobPlan{}
			for _, blob := range planResp.Blobs {
				blobByDigest[blob.SHA256] = blob
			}
			if err := validatePullBlobs(blobByDigest, pullPlan.Download); err != nil {
				return err
			}
			progress := newDataProgressRenderer(c.stdout, !c.wantsJSON(), "Download", totalManifestFileBytes(pullPlan.Download))
			var downloadedBytes int64
			var downloadedFiles int
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
			for _, entry := range pullPlan.Download {
				target, err := targetPathForEntry(".", targetCfg, entry)
				if err != nil {
					return err
				}
				if entry.Kind == model.DataManifestEntryKindDir {
					if err := os.MkdirAll(target, os.FileMode(entry.Mode)); err != nil {
						return err
					}
					continue
				}
				if entry.Kind == model.DataManifestEntryKindSymlink {
					if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
						return err
					}
					if overwrite {
						_ = os.Remove(target)
					}
					if err := os.Symlink(entry.LinkTarget, target); err != nil && !os.IsExist(err) {
						return err
					}
					continue
				}
				blob := blobByDigest[entry.SHA256]
				if blob.DownloadURL == "" {
					return fmt.Errorf("download url missing for %s", entry.SHA256)
				}
				if !c.wantsJSON() {
					fmt.Fprintf(c.stdout, "Downloading %s  %s\n", target, formatBytes(entry.Size))
				}
				if err := retryDataAuthorization(func() error {
					return client.DownloadDataBlob(blob.DownloadURL, target, entry.SHA256, entry.Size, !noResume, overwrite, concurrency)
				}, func() error {
					refresh, err := client.RefreshDataTransferAuthorization(planResp.Transfer.ID)
					if err != nil {
						return err
					}
					for _, refreshed := range refresh.Blobs {
						blobByDigest[refreshed.SHA256] = refreshed
					}
					refreshed, ok := blobByDigest[entry.SHA256]
					if !ok {
						return fmt.Errorf("refreshed transfer plan is missing blob %s", entry.SHA256)
					}
					blob = refreshed
					return nil
				}); err != nil {
					return err
				}
				downloadedBytes += entry.Size
				downloadedFiles++
				_ = client.CheckpointDataTransfer(planResp.Transfer.ID, downloadedBytes, downloadedFiles, nil)
				progress.advance(entry.Size)
			}
			progress.finish()
			if verify {
				if err := verifyPulledManifest(".", targetCfg, planResp.Manifest); err != nil {
					return err
				}
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
			fmt.Fprintf(c.stdout, "\nRestored version %s\n", planResp.Snapshot.Version)
			return nil
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "Version to pull. Defaults to latest")
	cmd.Flags().StringVar(&assetName, "asset", "", "Only pull one asset")
	cmd.Flags().StringVar(&toPath, "to", "", "Override target path for a single asset")
	cmd.Flags().BoolVar(&verify, "verify", false, "Verify restored files after download")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show pull plan without downloading")
	cmd.Flags().BoolVar(&keepLocal, "keep-local", false, "Skip conflicting local files")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite conflicting local files")
	cmd.Flags().BoolVar(&prune, "prune", false, "Delete local files absent from the version manifest")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Required with --prune to delete extra local files")
	cmd.Flags().BoolVar(&noResume, "no-resume", false, "Ignore local transfer resume state")
	cmd.Flags().IntVar(&concurrency, "concurrency", 8, "Transfer concurrency")
	return cmd
}

func (c *CLI) newDataCloneCommand() *cobra.Command {
	var toPath, version, assetName, grant string
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
				pullPlan, err := buildPullPlan(target, cfg, planResp.Manifest, false, false, false)
				if err != nil {
					return err
				}
				if len(pullPlan.Conflicts) > 0 {
					renderPullPreflight(c.stdout, workspaceResp.Workspace, planResp.Snapshot, pullPlan)
					return fmt.Errorf("pull preflight found conflicts")
				}
				blobByDigest := map[string]dataBlobPlan{}
				for _, blob := range planResp.Blobs {
					blobByDigest[blob.SHA256] = blob
				}
				if err := validatePullBlobs(blobByDigest, pullPlan.Download); err != nil {
					return err
				}
				for _, entry := range pullPlan.Download {
					targetPath, err := targetPathForEntry(target, cfg, entry)
					if err != nil {
						return err
					}
					if entry.Kind == model.DataManifestEntryKindDir {
						if err := os.MkdirAll(targetPath, os.FileMode(entry.Mode)); err != nil {
							return err
						}
						continue
					}
					if entry.Kind != model.DataManifestEntryKindFile {
						continue
					}
					blob := blobByDigest[entry.SHA256]
					if err := client.DownloadDataBlob(blob.DownloadURL, targetPath, entry.SHA256, entry.Size, true, false, 8); err != nil {
						return err
					}
				}
				if _, err := client.CompleteDataTransfer(planResp.Transfer.ID, map[string]any{"snapshot_id": planResp.Snapshot.ID, "bytes_done": planResp.Manifest.TotalBytes, "files_done": planResp.Manifest.FileCount}); err != nil {
					return err
				}
			} else if version != "" || assetName != "" {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workspace": workspaceResp.Workspace, "path": target})
			}
			fmt.Fprintf(c.stdout, "Cloned data workspace into %s\n", target)
			return nil
		},
	}
	cmd.Flags().StringVar(&toPath, "to", "", "Target directory")
	cmd.Flags().StringVar(&version, "version", "", "Version to pull after cloning")
	cmd.Flags().StringVar(&assetName, "asset", "", "Only pull one asset after cloning")
	cmd.Flags().StringVar(&grant, "grant", "", "Data grant secret")
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
	cmd.AddCommand(c.newDataWorkspaceListCommand(), c.newDataWorkspaceShowCommand(), c.newDataWorkspaceUseCommand(), c.newDataWorkspaceSetBackendCommand())
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
			for _, workspace := range workspaces {
				fmt.Fprintf(c.stdout, "%-24s %-18s %s\n", workspace.Name, workspace.StorageBackendID, formatBytes(workspace.UsedBytes))
			}
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
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			fmt.Fprintf(c.stdout, "Data workspace: %s\nBackend: %s\nUsed: %s\n", resp.Workspace.Name, resp.Workspace.StorageBackendID, formatBytes(resp.Workspace.UsedBytes))
			if len(resp.Workspace.Assets) > 0 {
				fmt.Fprintln(c.stdout, "\nAssets:")
				for _, asset := range resp.Workspace.Assets {
					fmt.Fprintf(c.stdout, "  %-14s %s\n", asset.Name, asset.Path)
				}
			}
			if resp.LatestSnapshot.ID != "" {
				fmt.Fprintf(c.stdout, "\nLatest version: %s\n", resp.LatestSnapshot.Version)
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
			fmt.Fprintf(c.stdout, "Bound current directory to data workspace %s\n", resp.Workspace.Name)
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
				for _, snapshot := range snapshots {
					fmt.Fprintf(c.stdout, "%-24s %-10d %-12s %s\n", snapshot.Version, snapshot.FileCount, formatBytes(snapshot.TotalBytes), snapshot.Message)
				}
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
				fmt.Fprintf(c.stdout, "Version: %s\nFiles: %d\nSize: %s\nDigest: %s\n", snapshot.Snapshot.Version, snapshot.Snapshot.FileCount, formatBytes(snapshot.Snapshot.TotalBytes), snapshot.Snapshot.ManifestDigest)
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
			fmt.Fprintf(c.stdout, "Added: %d\nRemoved: %d\nChanged: %d\n", len(diff["added"]), len(diff["removed"]), len(diff["changed"]))
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
			fmt.Fprintf(c.stdout, "Grant created. Secret: %s\n", grant["secret"])
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
			fmt.Fprintf(c.stdout, "Revoked %s\n", args[0])
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
				for _, transfer := range transfers {
					fmt.Fprintf(c.stdout, "%-24s %-10s %-12s %s\n", transfer.ID, transfer.Direction, transfer.Status, transfer.WorkspaceID)
				}
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
				fmt.Fprintf(c.stdout, "%s  %s  %.1f%%  %s/%s  files %d/%d\n", transfer.ID, transfer.Status, percent, formatBytes(transfer.BytesDone), formatBytes(transfer.BytesTotal), transfer.FilesDone, transfer.FilesTotal)
			}
			switch transfer.Status {
			case model.DataTransferStatusCompleted, model.DataTransferStatusFailed, model.DataTransferStatusCanceled:
				return nil
			}
			<-ticker.C
		}
	}}
	cmd.AddCommand(watch)
	cmd.AddCommand(&cobra.Command{Use: "resume <transfer-id>", Short: "Resume a data transfer", Example: strings.TrimSpace(`
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
		refresh, err := client.RefreshDataTransferAuthorization(args[0])
		if err != nil {
			return err
		}
		switch refresh.Transfer.Direction {
		case model.DataTransferDirectionUpload:
			manifest, pathsByDigest, err := scanDataManifest(".", cfg, "")
			if err != nil {
				return err
			}
			manifestDigest := digestDataManifest(manifest)
			if len(refresh.Transfer.Manifest.Entries) > 0 {
				manifestDigest = digestDataManifest(refresh.Transfer.Manifest)
			}
			if err := saveDataTransferState(".", dataTransferState{
				TransferID:     refresh.Transfer.ID,
				Direction:      model.DataTransferDirectionUpload,
				WorkspaceID:    refresh.Transfer.WorkspaceID,
				Version:        refresh.Transfer.Version,
				ManifestDigest: manifestDigest,
				Blobs:          refresh.Blobs,
			}); err != nil {
				return err
			}
			if err := c.uploadDataPlanBlobs(client, refresh.Transfer.WorkspaceID, refresh.Transfer.ID, manifestDigest, refresh.Blobs, pathsByDigest, true, false, 8); err != nil {
				return err
			}
			manifestToComplete := refresh.Transfer.Manifest
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
			fmt.Fprintf(c.stdout, "Resumed and completed upload transfer %s\n", refresh.Transfer.ID)
			return nil
		case model.DataTransferDirectionDownload:
			manifest := refresh.Transfer.Manifest
			if len(manifest.Entries) == 0 {
				return fmt.Errorf("transfer %s does not include a manifest to resume", refresh.Transfer.ID)
			}
			pullPlan, err := buildPullPlan(".", cfg, manifest, false, false, false)
			if err != nil {
				return err
			}
			if len(pullPlan.Conflicts) > 0 {
				renderPullPreflight(c.stdout, refresh.Workspace, model.DataSnapshot{Version: refresh.Transfer.Version, Manifest: manifest}, pullPlan)
				return fmt.Errorf("pull preflight found conflicts")
			}
			blobByDigest := map[string]dataBlobPlan{}
			for _, blob := range refresh.Blobs {
				blobByDigest[blob.SHA256] = blob
			}
			if err := validatePullBlobs(blobByDigest, pullPlan.Download); err != nil {
				return err
			}
			for _, entry := range pullPlan.Download {
				if entry.Kind != model.DataManifestEntryKindFile {
					continue
				}
				target, err := targetPathForEntry(".", cfg, entry)
				if err != nil {
					return err
				}
				blob := blobByDigest[entry.SHA256]
				if err := client.DownloadDataBlob(blob.DownloadURL, target, entry.SHA256, entry.Size, true, false, 8); err != nil {
					return err
				}
			}
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
			fmt.Fprintf(c.stdout, "Resumed and completed download transfer %s\n", refresh.Transfer.ID)
			return nil
		default:
			return fmt.Errorf("transfer %s direction %q is not resumable", refresh.Transfer.ID, refresh.Transfer.Direction)
		}
	}})
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
				for _, backend := range backends {
					fmt.Fprintf(c.stdout, "%-22s %-16s %s\n", backend.Name, backend.Provider, backend.Bucket)
				}
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
			fmt.Fprintf(c.stdout, "Rotated credentials for data backend %s\n", backend.Name)
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
			fmt.Fprintf(c.stdout, "Created data backend %s (%s)\n", backend.Name, backend.Provider)
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
			fmt.Fprintf(c.stdout, "Backend migration: %s -> %s\n", resp.Transfer.Source, resp.Transfer.Target)
			fmt.Fprintf(c.stdout, "Status: %s\nObjects: %d/%d\nBytes: %s/%s\n", resp.Transfer.Status, resp.Transfer.FilesDone, resp.Transfer.FilesTotal, formatBytes(resp.Transfer.BytesDone), formatBytes(resp.Transfer.BytesTotal))
			if cutover && resp.Workspace.StorageBackendID == args[1] {
				fmt.Fprintf(c.stdout, "Workspace backend: %s\n", resp.Workspace.StorageBackendID)
			}
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
			fmt.Fprintf(c.stdout, "Rolled back workspace backend: %s\n", resp.Workspace.StorageBackendID)
			fmt.Fprintf(c.stdout, "Rollback transfer: %s\n", resp.Transfer.ID)
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
			fmt.Fprintf(c.stdout, "GC sweep: %s\n", resp.Workspace.Name)
			if resp.GC.DryRun {
				fmt.Fprintln(c.stdout, "Mode: dry-run")
			} else {
				fmt.Fprintln(c.stdout, "Mode: delete")
			}
			fmt.Fprintf(c.stdout, "Candidates: %d\nDeleted: %d\nDeleted bytes: %s\n", len(resp.GC.Candidates), resp.GC.Deleted, formatBytes(resp.GC.DeletedBytes))
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
			fmt.Fprintf(c.stdout, "Data workspace: %s\n", cfg.Workspace)
			if len(warnings) == 0 {
				fmt.Fprintln(c.stdout, "Status: ok")
			} else {
				fmt.Fprintln(c.stdout, "Warnings:")
				for _, warning := range warnings {
					fmt.Fprintf(c.stdout, "  %s\n", warning)
				}
			}
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

func scanDataManifest(root string, cfg dataConfig, onlyAsset string) (model.DataManifest, map[string]string, error) {
	entries := []model.DataManifestEntry{}
	pathsByDigest := map[string]string{}
	for _, asset := range cfg.Assets {
		if onlyAsset != "" && asset.Name != onlyAsset {
			continue
		}
		assetPath := filepath.Join(root, filepath.FromSlash(asset.Path))
		info, err := os.Lstat(assetPath)
		if err != nil {
			if asset.Required {
				return model.DataManifest{}, nil, err
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
					sum, err := sha256LocalFile(current)
					if err != nil {
						return err
					}
					entry.SHA256 = sum
					entry.ObjectKey = model.DataObjectKey(sum)
					pathsByDigest[sum] = current
				} else {
					return nil
				}
				entries = append(entries, entry)
				return nil
			})
			if err != nil {
				return model.DataManifest{}, nil, err
			}
			continue
		}
		if info.Mode().IsRegular() {
			sum, err := sha256LocalFile(assetPath)
			if err != nil {
				return model.DataManifest{}, nil, err
			}
			entry := model.DataManifestEntry{AssetName: asset.Name, RelativePath: ".", Kind: model.DataManifestEntryKindFile, Size: info.Size(), Mode: int64(info.Mode()), MTime: info.ModTime().UTC(), SHA256: sum, ObjectKey: model.DataObjectKey(sum)}
			entries = append(entries, entry)
			pathsByDigest[sum] = assetPath
		}
	}
	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: entries})
	return manifest, pathsByDigest, nil
}

func shouldIgnoreDataPath(rel, base string, patterns []string) bool {
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
	var out []untrackedDataDirectory
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
			return nil, err
		}
		if bytes >= threshold {
			out = append(out, untrackedDataDirectory{Path: "./" + filepath.ToSlash(name), Bytes: bytes})
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

func sha256LocalFile(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func buildPullPlan(root string, cfg dataConfig, manifest model.DataManifest, overwrite, keepLocal, prune bool) (pullPlan, error) {
	var plan pullPlan
	expectedByAsset := map[string]map[string]struct{}{}
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
			localDigest, err := sha256LocalFile(target)
			if err != nil {
				return plan, err
			}
			if localDigest == entry.SHA256 {
				plan.Skip = append(plan.Skip, entry)
			} else if overwrite {
				plan.Download = append(plan.Download, entry)
			} else if keepLocal {
				plan.Warnings = append(plan.Warnings, pullConflict{"changed", target, "checksum differs from version manifest, keeping local file"})
			} else {
				plan.Conflicts = append(plan.Conflicts, pullConflict{"changed", target, "checksum differs from version manifest"})
			}
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
			got, err := sha256LocalFile(target)
			if err != nil {
				return err
			}
			if got != entry.SHA256 {
				return fmt.Errorf("verify %s: checksum mismatch", target)
			}
		}
	}
	return nil
}

func renderPullPreflight(w io.Writer, workspace model.DataWorkspace, snapshot model.DataSnapshot, plan pullPlan) {
	fmt.Fprintf(w, "Data workspace: %s\nVersion: %s\n\n", workspace.Name, snapshot.Version)
	if len(plan.Conflicts) > 0 {
		fmt.Fprintln(w, "Pull preflight found conflicts.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "CONFLICT   PATH                         REASON")
		for _, conflict := range plan.Conflicts {
			fmt.Fprintf(w, "%-10s %-28s %s\n", conflict.Kind, conflict.Path, conflict.Reason)
		}
	}
	if len(plan.Warnings) > 0 {
		fmt.Fprintln(w, "\nWARNING    PATH                         REASON")
		for _, warning := range plan.Warnings {
			fmt.Fprintf(w, "%-10s %-28s %s\n", warning.Kind, warning.Path, warning.Reason)
		}
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
	fmt.Fprintf(w, "Planning download:\n  files to download: %d\n  files to skip: %d\n", len(plan.Download), len(plan.Skip))
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
	fmt.Fprintf(c.stdout, "Data workspace: %s\nBackend: %s\n", workspace.Name, workspace.StorageBackendID)
	return nil
}

func (c *CLI) uploadDataPlanBlobs(client *Client, workspaceID, transferID, manifestDigest string, blobs []dataBlobPlan, pathsByDigest map[string]string, resume, noProgress bool, concurrency int) error {
	if concurrency <= 0 {
		concurrency = 1
	}
	progress := newDataProgressRenderer(c.stdout, !noProgress && !c.wantsJSON(), "Upload", totalDataBlobBytes(blobs))
	for _, blob := range blobs {
		if blob.Exists {
			progress.advance(blob.Size)
			continue
		}
		if blob.UploadMode != model.DataBlobUploadModeMultipart && strings.TrimSpace(blob.UploadURL) == "" {
			return fmt.Errorf("upload url missing for blob %s (mode=%q object_key=%q parts=%d exists=%t)", blob.SHA256, blob.UploadMode, blob.ObjectKey, len(blob.Parts), blob.Exists)
		}
		sourcePath := pathsByDigest[blob.SHA256]
		if sourcePath == "" {
			return fmt.Errorf("missing local source for blob %s", blob.SHA256)
		}
		if !noProgress && !c.wantsJSON() {
			mode := blob.UploadMode
			if mode == "" {
				mode = model.DataBlobUploadModeSingle
			}
			fmt.Fprintf(c.stdout, "Uploading %s  %s  %s\n", shortDataDigest(blob.SHA256), formatBytes(blob.Size), mode)
		}
		var state dataTransferState
		if resume {
			if loaded, ok, err := loadDataTransferState(".", transferID); err != nil {
				return err
			} else if ok {
				state = loaded
				blob = mergeDataBlobWithState(blob, state)
			}
		}
		if state.TransferID == "" {
			state = dataTransferState{TransferID: transferID, Direction: model.DataTransferDirectionUpload, WorkspaceID: workspaceID, ManifestDigest: manifestDigest}
		}
		if blob.UploadMode == model.DataBlobUploadModeMultipart {
			var completed []model.DataTransferPart
			err := retryDataAuthorization(func() error {
				var uploadErr error
				completed, uploadErr = c.uploadMultipartDataBlob(client, sourcePath, transferID, blob, resume, concurrency)
				return uploadErr
			}, func() error {
				refresh, err := client.RefreshDataTransferAuthorization(transferID)
				if err != nil {
					return err
				}
				refreshed, ok := dataPlanBlobByDigest(refresh.Blobs, blob.SHA256)
				if !ok {
					return fmt.Errorf("refreshed transfer plan is missing blob %s", blob.SHA256)
				}
				blob = mergeDataBlobWithState(refreshed, state)
				return nil
			})
			if err != nil {
				return err
			}
			if _, err := client.CompleteDataMultipartUpload(transferID, blob.SHA256, blob.UploadID, completed); err != nil {
				return err
			}
			completedBlob := markDataBlobUploaded(blob, completed)
			state = upsertDataTransferStateBlob(state, completedBlob)
			if resume {
				if err := saveDataTransferState(".", state); err != nil {
					return err
				}
			}
			_ = client.CheckpointDataTransfer(transferID, -1, -1, []dataBlobPlan{completedBlob})
			progress.advance(blob.Size)
			continue
		}
		if strings.TrimSpace(blob.UploadURL) == "" {
			continue
		}
		err := retryDataAuthorization(func() error {
			return client.PutDataBlob(blob.UploadURL, sourcePath)
		}, func() error {
			refresh, err := client.RefreshDataTransferAuthorization(transferID)
			if err != nil {
				return err
			}
			refreshed, ok := dataPlanBlobByDigest(refresh.Blobs, blob.SHA256)
			if !ok {
				return fmt.Errorf("refreshed transfer plan is missing blob %s", blob.SHA256)
			}
			blob = refreshed
			return nil
		})
		if err != nil {
			return err
		}
		completedBlob := markDataBlobUploaded(blob, nil)
		state = upsertDataTransferStateBlob(state, completedBlob)
		if resume {
			if err := saveDataTransferState(".", state); err != nil {
				return err
			}
		}
		_ = client.CheckpointDataTransfer(transferID, -1, -1, []dataBlobPlan{completedBlob})
		progress.advance(blob.Size)
	}
	progress.finish()
	return nil
}

func (c *CLI) uploadMultipartDataBlob(client *Client, sourcePath, transferID string, blob dataBlobPlan, resume bool, concurrency int) ([]model.DataTransferPart, error) {
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
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var mu sync.Mutex
	var wg sync.WaitGroup
	stateSave := func() error {
		if !resume {
			return nil
		}
		loaded, ok, err := loadDataTransferState(".", transferID)
		if err != nil {
			return err
		}
		if !ok {
			loaded = dataTransferState{TransferID: transferID, Direction: model.DataTransferDirectionUpload}
		}
		current := blob
		current.Parts = append([]model.DataTransferPart(nil), parts...)
		loaded = upsertDataTransferStateBlob(loaded, current)
		if err := saveDataTransferState(".", loaded); err != nil {
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
			etag, err := client.UploadDataBlobPart(part.UploadURL, sourcePath, part.Offset, part.Size)
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
	return &dataProgressRenderer{w: w, enabled: enabled, label: label, total: total}
}

func (p *dataProgressRenderer) advance(delta int64) {
	if p == nil || !p.enabled || delta <= 0 {
		return
	}
	p.done += delta
	now := time.Now()
	if !p.last.IsZero() && now.Sub(p.last) < 500*time.Millisecond && p.done < p.total {
		return
	}
	p.last = now
	if p.total > 0 {
		percent := float64(p.done) / float64(p.total) * 100
		fmt.Fprintf(p.w, "%s progress: %.1f%%  %s/%s\n", p.label, percent, formatBytes(p.done), formatBytes(p.total))
		return
	}
	fmt.Fprintf(p.w, "%s progress: %s\n", p.label, formatBytes(p.done))
}

func (p *dataProgressRenderer) finish() {
	if p == nil || !p.enabled || p.total <= 0 || p.done >= p.total {
		return
	}
	p.done = p.total
	fmt.Fprintf(p.w, "%s progress: 100.0%%  %s/%s\n", p.label, formatBytes(p.done), formatBytes(p.total))
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

func dataTransferStatePath(root, transferID string) string {
	return filepath.Join(root, dataTransferStateDir, strings.TrimSpace(transferID)+".json")
}

func saveDataTransferState(root string, state dataTransferState) error {
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
	return os.WriteFile(dataTransferStatePath(root, state.TransferID), raw, dataTransferStatePerm)
}

func loadDataTransferState(root, transferID string) (dataTransferState, bool, error) {
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

func findDataTransferState(root, direction, workspaceID, snapshotID, manifestDigest string) (dataTransferState, bool, error) {
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
			return dataTransferState{}, false, fmt.Errorf("read transfer state %s: %w", entry.Name(), err)
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
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/workspaces", workspaceID, "transfers/plan-upload"), map[string]any{"version": version, "message": message, "manifest": manifest}, &resp); err != nil {
		return dataUploadPlanResponse{}, err
	}
	return resp, nil
}

func (c *Client) PlanDataDownload(workspaceID, version string, assets []string) (dataDownloadPlanResponse, error) {
	var resp dataDownloadPlanResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/workspaces", workspaceID, "transfers/plan-download"), map[string]any{"version": version, "assets": assets}, &resp); err != nil {
		return dataDownloadPlanResponse{}, err
	}
	return resp, nil
}

func (c *Client) CompleteDataTransfer(transferID string, req map[string]any) (dataTransferCompleteResponse, error) {
	var resp dataTransferCompleteResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/transfers", transferID, "complete"), req, &resp); err != nil {
		return dataTransferCompleteResponse{}, err
	}
	return resp, nil
}

func (c *Client) RefreshDataTransferAuthorization(transferID string) (dataTransferAuthorizationResponse, error) {
	var resp dataTransferAuthorizationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/data/transfers", transferID, "refresh"), nil, &resp); err != nil {
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
	return c.doJSON(http.MethodPost, path.Join("/v1/data/transfers", transferID, "checkpoint"), req, &resp)
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
	file, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer file.Close()
	req, err := http.NewRequest(http.MethodPut, uploadURL, file)
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
	body := io.NewSectionReader(file, offset, size)
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
	if size > dataDownloadPartSize && concurrency > 1 {
		return c.downloadDataBlobMultipart(downloadURL, targetPath, expectedSHA, size, resume, overwrite, concurrency, dataDownloadPartSize)
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
				return os.Rename(tmpPath, targetPath)
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
	resp, err := c.httpClient.Do(req)
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
		offset = 0
	}
	if offset > 0 && resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return c.DownloadDataBlob(downloadURL, targetPath, expectedSHA, size, false, overwrite, concurrency)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusBadRequest {
		tmp.Close()
		return errDataAuthorizationExpired
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		tmp.Close()
		return fmt.Errorf("download failed: status=%d", resp.StatusCode)
	}
	if _, err := io.Copy(tmp, resp.Body); err != nil {
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

func (c *Client) downloadDataBlobMultipart(downloadURL, targetPath, expectedSHA string, size int64, resume, overwrite bool, concurrency int, partSize int64) error {
	if size <= 0 {
		return c.DownloadDataBlob(downloadURL, targetPath, expectedSHA, size, resume, overwrite, 1)
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
			if err := c.downloadDataBlobRangePart(downloadURL, part.path, part.offset, part.size, resume); err != nil {
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

func (c *Client) downloadDataBlobRangePart(downloadURL, partPath string, offset, size int64, resume bool) error {
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
			return nil
		}
		if existing > size {
			if err := os.Remove(partPath); err != nil {
				return err
			}
			existing = 0
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
	resp, err := c.httpClient.Do(req)
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
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if trimmed := strings.TrimSpace(string(body)); trimmed != "" {
			return fmt.Errorf("range download failed: status=%d body=%s", resp.StatusCode, trimmed)
		}
		return fmt.Errorf("range download failed: status=%d", resp.StatusCode)
	}
	if _, err := io.Copy(file, resp.Body); err != nil {
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
	if isFugueManagedDataBlobURL(req.URL.String()) && strings.TrimSpace(req.Header.Get("Authorization")) == "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
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
		if trimmed := strings.TrimSpace(string(body)); trimmed != "" {
			return nil, fmt.Errorf("object transfer failed: status=%d body=%s", resp.StatusCode, trimmed)
		}
		return nil, fmt.Errorf("object transfer failed: status=%d", resp.StatusCode)
	}
	return resp, nil
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
	if err := c.doJSON(http.MethodGet, path.Join("/v1/data/transfers", id), nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) GetDataTransferModel(id string) (model.DataTransfer, error) {
	var resp struct {
		Transfer model.DataTransfer `json:"transfer"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/data/transfers", id), nil, &resp); err != nil {
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
