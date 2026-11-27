package api

import (
	"context"

	"fugue/internal/appimages"
)

// The HTTP composition keeps only the registry operations required by its
// handlers; the image domain owns remote access and the inspection cache.
type appImageRegistry interface {
	InspectImage(ctx context.Context, imageRef string) (appImageRegistryInspectResult, error)
	DeleteImage(ctx context.Context, imageRef string) (appImageRegistryDeleteResult, error)
}

type appImageRegistryInspectResult = appimages.Inspection
type appImageRegistryDeleteResult = appimages.DeleteResult

func newRemoteAppImageRegistry() appImageRegistry {
	return appimages.NewRemoteRegistry()
}
