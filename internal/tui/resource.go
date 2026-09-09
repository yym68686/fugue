package tui

import "time"

func (m *Model) togglePause() {
	m.paused = !m.paused
	if m.paused {
		m.pausedAt = m.now
	}
}
func (m *Model) chartNow() time.Time {
	if m.paused && !m.pausedAt.IsZero() {
		return m.pausedAt
	}
	return m.now
}

// Histories are retained only for a bounded set of rows in the current view.
// The selected row is always included. Refreshing/sorting never mixes nodes.
func (m *Model) mergeResourceHistory(next *Snapshot) {
	old := map[string]*ResourceDetail{}
	for _, table := range m.snapshot.Tables {
		for _, row := range table.Rows {
			if row.Detail != nil {
				old[table.ID+"\x00"+row.ID] = row.Detail
			}
		}
	}
	selected := m.selectedRow()
	retained := 0
	for i := range next.Tables {
		table := &next.Tables[i]
		for j := range table.Rows {
			row := &table.Rows[j]
			if row.Detail == nil {
				continue
			}
			previous := old[table.ID+"\x00"+row.ID]
			keep := retained < 128 || (selected != nil && selected.ID == row.ID)
			for k, series := range row.Detail.Series {
				var history Series
				if keep && previous != nil {
					for _, candidate := range previous.Series {
						if candidate.ID == series.ID {
							history = candidate
							break
						}
					}
				}
				row.Detail.Series[k] = MergeSeries(history, series, m.now)
			}
			retained++
		}
	}
}

func (m *Model) selectedRow() *Row {
	rows := m.rows()
	if m.selected < 0 || m.selected >= len(rows) {
		return nil
	}
	return &rows[m.selected]
}

func (m *Model) chartSeries() []Series {
	series := m.snapshot.Series
	if row := m.selectedRow(); !m.overviewCharts && row != nil && row.Detail != nil && len(row.Detail.Series) > 0 {
		series = row.Detail.Series
	}
	out := make([]Series, 0, len(series))
	for _, s := range series {
		// Unsupported metrics belong in the availability summary, not a large
		// empty chart. Collecting series with real observations stay visible.
		if len(s.Points) > 0 || s.State == "" || s.State == "collecting" || s.State == "available" {
			out = append(out, s)
		}
	}
	return out
}

func (m *Model) chartScope() string {
	if row := m.selectedRow(); !m.overviewCharts && row != nil && row.Detail != nil && len(row.Detail.Series) > 0 {
		return "Selected · " + cell(*row, 0)
	}
	if m.request.Target.Kind == "cluster" {
		return "Cluster · per-node peaks"
	}
	return first(m.snapshot.Title, m.request.Target.Name, "Overview")
}
