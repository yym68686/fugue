package localpvsafety

import "time"

const (
	DefaultInventoryInterval = 15 * time.Minute
	DefaultInventoryTTL      = time.Hour
	MinimumFreeBytes         = int64(5 << 30)
	MinimumFreePercent       = int64(10)
	MinimumFilesystemPercent = int64(95)
	MaximumFutureClockSkew   = 5 * time.Minute
)

func RequiredFreeBytes(capacityBytes int64) int64 {
	if capacityBytes <= 0 {
		return MinimumFreeBytes
	}
	percentBytes := (capacityBytes/100)*MinimumFreePercent +
		((capacityBytes%100)*MinimumFreePercent+99)/100
	if percentBytes > MinimumFreeBytes {
		return percentBytes
	}
	return MinimumFreeBytes
}

func HasCapacityHeadroom(capacityBytes, freeBytes int64) bool {
	if capacityBytes <= 0 || freeBytes < 0 || freeBytes > capacityBytes {
		return false
	}
	return freeBytes >= RequiredFreeBytes(capacityBytes)
}

func MinimumFilesystemCapacityBytes(provisionedBytes int64) int64 {
	if provisionedBytes <= 0 {
		return 0
	}
	return (provisionedBytes/100)*MinimumFilesystemPercent +
		((provisionedBytes%100)*MinimumFilesystemPercent+99)/100
}

func FilesystemCapacityConverged(capacityBytes, provisionedBytes int64) bool {
	return capacityBytes > 0 &&
		provisionedBytes > 0 &&
		capacityBytes >= MinimumFilesystemCapacityBytes(provisionedBytes)
}

func IsFresh(observedAt, now time.Time, ttl time.Duration) bool {
	if observedAt.IsZero() {
		return false
	}
	if ttl <= 0 {
		ttl = DefaultInventoryTTL
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if observedAt.After(now.Add(MaximumFutureClockSkew)) {
		return false
	}
	return !observedAt.Before(now.Add(-ttl))
}
