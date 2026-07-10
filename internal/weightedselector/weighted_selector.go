package weightedselector

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
)

type Candidate struct {
	ID     string
	Weight int
	Active bool
}

type Selection struct {
	Index       int
	Bucket      int
	TotalWeight int
	SelectedID  string
}

func Select(candidates []Candidate, stickyKey string) (Selection, bool) {
	total := 0
	activeIndexes := make([]int, 0, len(candidates))
	for index, candidate := range candidates {
		if !candidate.Active || candidate.Weight <= 0 {
			continue
		}
		activeIndexes = append(activeIndexes, index)
		total += candidate.Weight
	}
	if len(activeIndexes) == 0 || total <= 0 {
		return Selection{}, false
	}
	bucket := Bucket(stickyKey, total)
	running := 0
	selectedIndex := activeIndexes[len(activeIndexes)-1]
	for _, index := range activeIndexes {
		running += candidates[index].Weight
		if bucket < running {
			selectedIndex = index
			break
		}
	}
	return Selection{
		Index:       selectedIndex,
		Bucket:      bucket,
		TotalWeight: total,
		SelectedID:  strings.TrimSpace(candidates[selectedIndex].ID),
	}, true
}

func Bucket(key string, total int) int {
	if total <= 0 {
		return 0
	}
	sum := sha256.Sum256([]byte(key))
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(total))
}
