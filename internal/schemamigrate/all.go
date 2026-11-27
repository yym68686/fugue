package schemamigrate

import (
	"context"
	"fmt"
)

// MigrateAll applies the independently owned schema migrations in release order.
func MigrateAll(ctx context.Context, databaseURL string) error {
	if err := MigrateStorageMaintenance(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateBoundedContent(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate bounded content stores: %w", err)
	}
	if err := MigratePlatformArtifactContent(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate artifact content references: %w", err)
	}
	if err := MigrateCompilerInputCodec(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate compiler input codec: %w", err)
	}
	if err := MigrateCompilerInputShared(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate shared compiler input: %w", err)
	}
	if err := MigrateCompilerInputSeparated(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate separated compiler input: %w", err)
	}
	if err := MigrateCompilerInputArchive(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate compiler input archive ledger: %w", err)
	}
	if err := MigrateAppReleaseWorkload(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateAppReleaseRetirement(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateObjectStorage(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateSSHIdentity(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateSourceUploadSessions(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigratePlatformShadowConsumers(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateHostedProposalGuards(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateCompilerContract(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigratePlatformState(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate platform-state schema: %w", err)
	}
	if err := MigratePlatformConsumerRetirements(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigratePlatformArtifactReadIndex(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate platform-artifact read index: %w", err)
	}
	if err := MigratePlatformHistoryFKIndexes(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate platform history foreign-key indexes: %w", err)
	}
	if err := MigrateCompilerInputHotIndex(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate compiler input dependency index: %w", err)
	}
	if err := MigrateNodeUpdateTaskScheduler(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigratePlatformRuntimeFactIndexes(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate runtime fact read indexes: %w", err)
	}
	if err := MigrateImageRetirement(ctx, databaseURL); err != nil {
		return err
	}
	if err := MigrateImageCacheManifestGraph(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate image-cache manifest graph schema: %w", err)
	}
	if err := MigrateEdgeInstanceFencing(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate edge-instance fencing schema: %w", err)
	}
	if err := MigrateAppMail(ctx, databaseURL); err != nil {
		return err
	}
	return nil
}
