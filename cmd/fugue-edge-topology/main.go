package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"fugue/internal/edgetopology"
)

func main() {
	topologyPath := flag.String("topology", "deploy/edge/topology.json", "edge topology intent")
	discoveryURL := flag.String("discovery", "", "optional read-only DiscoveryBundle URL")
	flag.Parse()
	file, err := os.Open(*topologyPath)
	if err != nil {
		fail(err)
	}
	intent, err := edgetopology.Decode(file)
	_ = file.Close()
	if err != nil {
		fail(err)
	}
	if *discoveryURL == "" {
		fmt.Printf("validated %d cells, %d pools, %d edges\n", len(intent.Cells), len(intent.Pools), len(intent.Edges))
		return
	}
	if len(*discoveryURL) < len("https://") || (*discoveryURL)[:len("https://")] != "https://" {
		fail(errors.New("discovery audit requires HTTPS"))
	}
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Get(*discoveryURL)
	if err != nil {
		fail(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fail(fmt.Errorf("discovery returned HTTP %d", response.StatusCode))
	}
	var snapshot struct {
		EdgeNodes []struct {
			ID          string `json:"id"`
			EdgeGroupID string `json:"edge_group_id"`
		} `json:"edge_nodes"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&snapshot); err != nil {
		fail(err)
	}
	observed := make([]edgetopology.ObservedEdge, 0, len(snapshot.EdgeNodes))
	for _, node := range snapshot.EdgeNodes {
		observed = append(observed, edgetopology.ObservedEdge{ID: node.ID, LegacyGroupID: node.EdgeGroupID})
	}
	audit := intent.Audit(observed)
	if err := json.NewEncoder(os.Stdout).Encode(audit); err != nil {
		fail(err)
	}
	if len(audit.UnknownEdges) != 0 || len(audit.MismatchedGroups) != 0 {
		os.Exit(1)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
