package sourceimport

import "context"

type UploadTopologyInspectRequest struct {
	ArchiveFilename  string
	ArchiveSHA256    string
	ArchiveSizeBytes int64
	ArchiveData      []byte
	AppName          string
}

func (i *Importer) InspectUploadedImportableTopology(_ context.Context, req UploadTopologyInspectRequest) (NormalizedTopology, error) {
	src, err := i.extractUploadedArchive(UploadSourceImportRequest{
		UploadID:         "topology-inspect",
		ArchiveFilename:  req.ArchiveFilename,
		ArchiveSHA256:    req.ArchiveSHA256,
		ArchiveSizeBytes: req.ArchiveSizeBytes,
		ArchiveData:      req.ArchiveData,
		AppName:          req.AppName,
	})
	if err != nil {
		return NormalizedTopology{}, err
	}
	defer releaseExtractedUploadSource(src)

	return inspectImportableTopologyFromRepo(clonedGitHubRepo{
		RepoDir:        src.RootDir,
		CommitSHA:      src.ArchiveSHA256,
		DefaultAppName: src.DefaultAppName,
	})
}
