package api

import (
	"context"

	"fugue/internal/backupusage"
	"fugue/internal/backupusage/inventoryscan"
	"fugue/internal/model"
	"fugue/internal/store"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BackupInventoryConfig = inventoryscan.Config

func BackupInventoryConfigFromEnv() BackupInventoryConfig { return inventoryscan.ConfigFromEnv() }

func (s *Server) StartBackgroundBackupInventory(ctx context.Context) {
	s.backupInventory.Run(ctx, func(err error) {
		if s.log != nil {
			s.log.Printf("backup inventory checkpoint refresh failed: %v", err)
		}
	})
}

func newBackupInventory(db *store.Store, cfg BackupInventoryConfig) *inventoryscan.Service {
	return inventoryscan.New(backupInventoryStore{db}, openBackupInventory, backupArtifactObjectKeysForDeletion, cfg, backupFailedRunGCGrace)
}

func backupRepositoryPrefix(artifact model.BackupArtifact, backend model.DataBackend) string {
	return inventoryscan.RepositoryPrefix(artifact, backend)
}

type backupInventoryStore struct{ db *store.Store }

func (s backupInventoryStore) Backends() ([]model.BackupBackend, error) {
	return s.db.ListBackupBackends("", true)
}

func (s backupInventoryStore) BackendForInventory(id string) (model.BackupBackend, error) {
	return s.db.GetBackupBackendForUse(id, "", true)
}

func (s backupInventoryStore) Artifacts() ([]model.BackupArtifact, error) {
	records, err := s.db.ListBackupUsageArtifacts("", true)
	if err != nil {
		return nil, err
	}
	artifacts := make([]model.BackupArtifact, len(records))
	for i, record := range records {
		artifacts[i] = record.Artifact
	}
	return artifacts, nil
}

func (s backupInventoryStore) ReadPages(ctx context.Context, key string) (inventoryscan.Pages, []byte, error) {
	pages, raw, err := s.db.ReadObservationPages(ctx, key)
	return inventoryscan.Pages(pages), raw, err
}

func (s backupInventoryStore) UpdatePages(ctx context.Context, key string, update func(inventoryscan.Pages, []byte) (inventoryscan.Pages, error), legacy func([]byte) ([]byte, error)) (bool, error) {
	return s.db.UpdateObservationPages(ctx, key, func(pages store.ObservationPages, raw []byte) (store.ObservationPages, error) {
		result, err := update(inventoryscan.Pages(pages), raw)
		return store.ObservationPages(result), err
	}, legacy)
}

func openBackupInventory(record model.BackupBackend) (inventoryscan.ObjectReader, error) {
	backend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(record))
	if err != nil {
		return inventoryscan.ObjectReader{}, err
	}
	return inventoryscan.ObjectReader{
		Namespace: backupusage.NamespaceOf(backend.backend),
		ListPage: func(ctx context.Context, cursor string) (inventoryscan.Page, error) {
			var token *string
			if cursor != "" {
				token = aws.String(cursor)
			}
			response, err := backend.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(backend.backend.Bucket), Prefix: aws.String(backend.objectKey("")),
				ContinuationToken: token, MaxKeys: aws.Int32(1000),
			})
			if err != nil {
				return inventoryscan.Page{}, err
			}
			page := inventoryscan.Page{Cursor: aws.ToString(response.NextContinuationToken), More: aws.ToBool(response.IsTruncated)}
			page.Objects = make([]backupusage.InventoryObject, 0, len(response.Contents))
			for _, o := range response.Contents {
				page.Objects = append(page.Objects, backupusage.InventoryObject{
					Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), LastModified: aws.ToTime(o.LastModified),
				})
			}
			return page, nil
		},
	}, nil
}
