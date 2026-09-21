package main

import (
	"bufio"
	"bytes"
	"sort"
	"strings"
)

// Stack paths preserve caller relationships that cumulative tables cannot show.
// A shared frame dictionary keeps repeated wrappers small on the report wire.
type stackPathSummary struct {
	Frames          []string    `json:"frames"`
	Paths           []stackPath `json:"paths"`
	ObservedSamples int         `json:"observed_samples"`
	OmittedSamples  int         `json:"omitted_samples"`
	Truncated       bool        `json:"truncated"`
	Order           string      `json:"frame_order"`
}
type stackPath struct {
	Frames  []int `json:"frames"`
	Samples int   `json:"samples"`
}

func summarizeStackPaths(raw []byte) stackPathSummary {
	result := stackPathSummary{Frames: []string{}, Paths: []stackPath{}, Order: "leaf-to-root"}
	frameIDs := map[string]int{}
	pathIDs := map[string]int{}
	var current []string
	frameBytes := 0
	flush := func() {
		if len(current) == 0 {
			return
		}
		result.ObservedSamples++
		key := strings.Join(current, "\x00")
		if id, ok := pathIDs[key]; ok {
			result.Paths[id].Samples++
			current = nil
			return
		}
		newFrames, newBytes := 0, 0
		seen := map[string]bool{}
		for _, frame := range current {
			if _, ok := frameIDs[frame]; !ok && !seen[frame] {
				seen[frame] = true
				newFrames++
				newBytes += len(frame)
			}
		}
		if len(result.Paths) >= 512 || len(result.Frames)+newFrames > 4096 || frameBytes+newBytes > 256<<10 || len(current) > 64 {
			result.OmittedSamples++
			result.Truncated = true
			current = nil
			return
		}
		ids := make([]int, 0, len(current))
		for _, frame := range current {
			id, ok := frameIDs[frame]
			if !ok {
				id = len(result.Frames)
				frameIDs[frame] = id
				result.Frames = append(result.Frames, frame)
				frameBytes += len(frame)
			}
			ids = append(ids, id)
		}
		pathIDs[key] = len(result.Paths)
		result.Paths = append(result.Paths, stackPath{Frames: ids, Samples: 1})
		current = nil
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			flush()
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || !isHexAddress(fields[0]) {
			continue
		}
		frame := strings.TrimSpace(strings.TrimPrefix(line, fields[0]))
		if frame == "" {
			frame = "0x" + fields[0] + " [unknown]"
		}
		current = append(current, frame)
	}
	flush()
	if scanner.Err() != nil {
		result.Truncated = true
	}
	sort.SliceStable(result.Paths, func(i, j int) bool { return result.Paths[i].Samples > result.Paths[j].Samples })
	return result
}
