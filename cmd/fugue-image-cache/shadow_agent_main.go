//go:build imageplaneagent

package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	lifecycle, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runImageCachePlatformPlanAgent(lifecycle); err != nil {
		log.Fatalf("serve image-plane shadow agent: %v", err)
	}
}
