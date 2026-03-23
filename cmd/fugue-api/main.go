package main

import (
	"log"
	"net/http"

	"fugue/internal/api"
	"fugue/internal/auth"
	"fugue/internal/config"
	"fugue/internal/store"
)

func main() {
	cfg := config.APIFromEnv()
	logger := log.Default()
	store := store.New(cfg.StorePath, cfg.DatabaseURL)
	if err := store.Init(); err != nil {
		logger.Fatalf("init store: %v", err)
	}

	server := api.NewServer(store, auth.New(store, cfg.BootstrapAdminKey), logger, api.ServerConfig{
		AppBaseDomain:    cfg.AppBaseDomain,
		APIPublicDomain:  cfg.APIPublicDomain,
		RegistryPushBase: cfg.RegistryPushBase,
		ImportWorkDir:    cfg.ImportWorkDir,
	})
	logger.Printf("fugue-api listening on %s", cfg.BindAddr)
	if err := http.ListenAndServe(cfg.BindAddr, server.Handler()); err != nil {
		logger.Fatalf("listen and serve: %v", err)
	}
}
