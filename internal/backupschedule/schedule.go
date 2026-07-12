package backupschedule

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	hourlyShortcut = "@hourly"
	hourlySchedule = "0 * * * *"
)

type cronField struct {
	values   []bool
	min      int
	max      int
	wildcard bool
}

type cronSchedule struct {
	minute     cronField
	hour       cronField
	dayOfMonth cronField
	month      cronField
	dayOfWeek  cronField
}

// Validate verifies the backup scheduling contract: a numeric five-field cron
// expression evaluated in UTC, plus the historical @hourly shortcut.
func Validate(schedule string) error {
	_, err := Next(schedule, time.Now().UTC())
	return err
}

// Next returns the first scheduled minute strictly after the supplied time.
func Next(schedule string, after time.Time) (time.Time, error) {
	parsed, err := parse(schedule)
	if err != nil {
		return time.Time{}, err
	}
	if after.IsZero() {
		after = time.Now().UTC()
	}
	after = after.UTC()
	candidate := after.Truncate(time.Minute).Add(time.Minute)
	limit := after.AddDate(5, 0, 0)
	for !candidate.After(limit) {
		if !parsed.month.values[int(candidate.Month())] {
			candidate = time.Date(candidate.Year(), candidate.Month()+1, 1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if !parsed.dayMatches(candidate) {
			candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day()+1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if !parsed.hour.values[candidate.Hour()] {
			hour, wrapped := parsed.hour.nextAfter(candidate.Hour())
			dayOffset := 0
			if wrapped {
				dayOffset = 1
			}
			candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day()+dayOffset, hour, 0, 0, 0, time.UTC)
			continue
		}
		if !parsed.minute.values[candidate.Minute()] {
			minute, wrapped := parsed.minute.nextAfter(candidate.Minute())
			if wrapped {
				candidate = candidate.Truncate(time.Hour).Add(time.Hour).Add(time.Duration(minute) * time.Minute)
			} else {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(), candidate.Hour(), minute, 0, 0, time.UTC)
			}
			continue
		}
		return candidate, nil
	}
	return time.Time{}, fmt.Errorf("schedule has no occurrence within five years")
}

func parse(schedule string) (cronSchedule, error) {
	schedule = strings.TrimSpace(schedule)
	if schedule == hourlyShortcut {
		schedule = hourlySchedule
	}
	if schedule == "" {
		return cronSchedule{}, fmt.Errorf("schedule is required")
	}
	if strings.HasPrefix(schedule, "TZ=") || strings.HasPrefix(schedule, "CRON_TZ=") || strings.HasPrefix(schedule, "@") {
		return cronSchedule{}, fmt.Errorf("use a numeric five-field UTC cron expression or @hourly")
	}
	parts := strings.Fields(schedule)
	if len(parts) != 5 {
		return cronSchedule{}, fmt.Errorf("expected exactly five cron fields, found %d", len(parts))
	}
	minute, err := parseField("minute", parts[0], 0, 59, false)
	if err != nil {
		return cronSchedule{}, err
	}
	hour, err := parseField("hour", parts[1], 0, 23, false)
	if err != nil {
		return cronSchedule{}, err
	}
	dayOfMonth, err := parseField("day-of-month", parts[2], 1, 31, false)
	if err != nil {
		return cronSchedule{}, err
	}
	month, err := parseField("month", parts[3], 1, 12, false)
	if err != nil {
		return cronSchedule{}, err
	}
	dayOfWeek, err := parseField("day-of-week", parts[4], 0, 7, true)
	if err != nil {
		return cronSchedule{}, err
	}
	return cronSchedule{minute: minute, hour: hour, dayOfMonth: dayOfMonth, month: month, dayOfWeek: dayOfWeek}, nil
}

func parseField(name, raw string, min, max int, normalizeSunday bool) (cronField, error) {
	field := cronField{values: make([]bool, max+1), min: min, max: max}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return cronField{}, fmt.Errorf("invalid %s field %q", name, raw)
		}
		base := item
		step := 1
		hasStep := strings.Contains(item, "/")
		if hasStep {
			pieces := strings.Split(item, "/")
			if len(pieces) != 2 || pieces[0] == "" || pieces[1] == "" {
				return cronField{}, fmt.Errorf("invalid %s step %q", name, item)
			}
			base = pieces[0]
			var err error
			step, err = parseNumber(pieces[1])
			if err != nil || step <= 0 {
				return cronField{}, fmt.Errorf("invalid %s step %q", name, pieces[1])
			}
			if step > max-min+1 {
				return cronField{}, fmt.Errorf("%s step %d exceeds field width", name, step)
			}
		}

		start, end := 0, 0
		switch {
		case base == "*":
			start, end = min, max
			field.wildcard = true
		case strings.Contains(base, "-"):
			bounds := strings.Split(base, "-")
			if len(bounds) != 2 {
				return cronField{}, fmt.Errorf("invalid %s range %q", name, base)
			}
			var err error
			start, err = parseNumber(bounds[0])
			if err != nil {
				return cronField{}, fmt.Errorf("invalid %s range %q", name, base)
			}
			end, err = parseNumber(bounds[1])
			if err != nil {
				return cronField{}, fmt.Errorf("invalid %s range %q", name, base)
			}
		default:
			value, err := parseNumber(base)
			if err != nil {
				return cronField{}, fmt.Errorf("invalid %s value %q", name, base)
			}
			start, end = value, value
			if hasStep {
				end = max
			}
		}
		if start < min || start > max || end < min || end > max || start > end {
			return cronField{}, fmt.Errorf("%s range %d-%d is outside %d-%d", name, start, end, min, max)
		}
		for value := start; value <= end; value += step {
			index := value
			if normalizeSunday && value == 7 {
				index = 0
			}
			field.values[index] = true
		}
	}
	return field, nil
}

func parseNumber(raw string) (int, error) {
	if raw == "" || strings.Trim(raw, "0123456789") != "" {
		return 0, fmt.Errorf("invalid number %q", raw)
	}
	return strconv.Atoi(raw)
}

func (schedule cronSchedule) dayMatches(candidate time.Time) bool {
	dayOfMonthMatches := schedule.dayOfMonth.values[candidate.Day()]
	dayOfWeekMatches := schedule.dayOfWeek.values[int(candidate.Weekday())]
	if schedule.dayOfMonth.wildcard || schedule.dayOfWeek.wildcard {
		return dayOfMonthMatches && dayOfWeekMatches
	}
	return dayOfMonthMatches || dayOfWeekMatches
}

func (field cronField) nextAfter(current int) (int, bool) {
	for value := current + 1; value <= field.max; value++ {
		if field.values[value] {
			return value, false
		}
	}
	for value := field.min; value <= field.max; value++ {
		if field.values[value] {
			return value, true
		}
	}
	return field.min, true
}
