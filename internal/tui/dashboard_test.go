package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

func clusterFixture(now time.Time) Snapshot {
	s := Snapshot{Target: Target{Kind: "cluster"}, Title: "Cluster", Status: "ready", ObservedAt: now,
		Fields:             []Field{{Label: "Nodes ready", Value: "8 / 8"}, {Label: "Workloads", Value: "64"}, {Label: "Control plane", Value: "ready"}, {Label: "Policy drift", Value: "0"}, {Label: "Health blocked", Value: "0"}, {Label: "Topology", Value: "multi-zone"}},
		Sources:            []Source{{ID: "nodes", State: "available", ObservedAt: now}, {ID: "policies", State: "available", ObservedAt: now}, {ID: "components", State: "available", ObservedAt: now}},
		UnavailableScreens: map[string]string{"logs": "Open an application"},
		Events:             []Event{{At: now.Add(-time.Minute), Severity: "success", Message: "Rollout completed · 3 replicas ready"}},
	}
	table := Table{ID: "nodes", Title: "Nodes", Columns: []string{"Node", "Status", "CPU", "Memory", "Disk", "Workloads", "Region"}}
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("worker-%02d", i+1)
		d := &ResourceDetail{ObservedAt: now, ItemsTitle: "Workloads", Fields: []Field{{Label: "Region", Value: "region A / zone 2"}, {Label: "OS", Value: "Linux"}, {Label: "Kubelet", Value: "v1.34.2"}, {Label: "Container runtime", Value: "containerd"}, {Label: "CPU schedulable", Value: "2.50 cores"}, {Label: "Memory schedulable", Value: "5.2 GiB"}}, Capacity: []Field{{Label: "CPU", Value: "1.64 / 8.00 cores"}, {Label: "Memory", Value: "12.4 / 32.0 GiB"}, {Label: "Disk", Value: "192.0 / 500.0 GiB"}}}
		values := []float64{20 + float64(i*8), 38 + float64(i*4), 38 + float64(i*7)}
		d.Capacity = []Field{{Label: "CPU", Value: fmt.Sprintf("%.2f / 8.00 cores", values[0]*8/100)}, {Label: "Memory", Value: fmt.Sprintf("%.2f / 32.0 GiB", values[1]*32/100)}, {Label: "Disk", Value: fmt.Sprintf("%.1f / 500.0 GiB", values[2]*5)}}
		for j, id := range []string{"cpu", "memory", "disk"} {
			series := Series{ID: id, Label: []string{"CPU", "Memory", "Disk"}[j], Unit: "%", Source: "synthetic node telemetry", State: "available", Interval: 30 * time.Second}
			for k := 0; k < 20; k++ {
				value := values[j] + float64((k*7)%17) - 8
				if k == 19 {
					value = values[j]
				}
				series.Points = append(series.Points, chartSample(now.Add(time.Duration(k-19)*30*time.Second), value))
			}
			d.Series = append(d.Series, series)
		}
		for k := 0; k < 8; k++ {
			d.Items = append(d.Items, Field{Label: fmt.Sprintf("service-%02d", k+1), Value: "app · 2 pods"})
		}
		target := Target{Kind: "node", ID: name, Name: name}
		row := Row{ID: name, Target: &target, Detail: d, Cells: []string{name, "ready", fmt.Sprintf("%.1f%%", values[0]), fmt.Sprintf("%.1f%%", values[1]), fmt.Sprintf("%.1f%%", values[2]), "8", "region A"}, Numbers: map[int]float64{2: values[0], 3: values[1], 4: values[2]}}
		table.Rows = append(table.Rows, row)
	}
	s.Tables = []Table{table}
	for _, series := range table.Rows[7].Detail.Series {
		series.Label = "Peak node " + series.Label
		series.Subject = table.Rows[7].ID
		s.Series = append(s.Series, series)
	}
	s.Series = append(s.Series, Series{ID: "network", Label: "Network", State: "unavailable", Source: "not exported"})
	return s
}

func TestSelectionLinksChartsWithoutRequests(t *testing.T) {
	m := New(&testProvider{}, Request{Target: Target{Kind: "cluster"}}, Options{Preferences: DefaultPreferences()})
	m.accept("overview", clusterFixture(m.now))
	m.width, m.height = 200, 50
	first := m.chartSeries()[2]
	if m.chartScope() != "Selected · worker-01" {
		t.Fatal(m.chartScope())
	}
	m.move(1)
	if m.chartScope() != "Selected · worker-02" || *m.chartSeries()[2].Points[0].Value == *first.Points[0].Value {
		t.Fatal("selection did not switch the series")
	}
	if cmd := press(m, "o"); cmd != nil {
		t.Fatal("chart scope started a request")
	}
	if m.chartScope() != "Cluster · per-node peaks" || m.chartSeries()[2].Subject != "worker-08" {
		t.Fatal("overview scope lost peak attribution")
	}
	press(m, "o")
	r := m.layout().table
	m.click(tea.MouseClickMsg{X: r.Min.X + 3, Y: r.Min.Y + 4, Button: tea.MouseLeft})
	if m.chartScope() != "Selected · worker-03" {
		t.Fatalf("mouse selected %s", m.chartScope())
	}
	if len(m.fetches) != 0 {
		t.Fatal("selection created background requests")
	}
}

func TestResourceHistoriesKeepIdentityAndSanitize(t *testing.T) {
	m := New(&testProvider{}, Request{Target: Target{Kind: "cluster"}}, Options{Preferences: DefaultPreferences()})
	initial := clusterFixture(m.now)
	m.accept("overview", initial)
	m.now = m.now.Add(30 * time.Second)
	next := clusterFixture(m.now)
	for i := range next.Tables[0].Rows {
		for j := range next.Tables[0].Rows[i].Detail.Series {
			s := &next.Tables[0].Rows[i].Detail.Series[j]
			s.Points = s.Points[len(s.Points)-1:]
		}
	}
	next.Tables[0].Rows[0], next.Tables[0].Rows[1] = next.Tables[0].Rows[1], next.Tables[0].Rows[0]
	next.Tables[0].Rows[0].Detail.Fields[0].Value = "region\x1b]52;c;YWJj\a"
	m.accept("overview", next)
	if m.selectedRow().ID != "worker-01" || len(m.chartSeries()[0].Points) != 21 {
		t.Fatal("refresh/reorder lost selection or history")
	}
	if strings.Contains(m.snapshot.Tables[0].Rows[0].Detail.Fields[0].Value, "\x1b") {
		t.Fatal("detail control sequence survived")
	}
}

func TestDenseDashboardGeometryAndPercentageScale(t *testing.T) {
	m := New(&testProvider{}, Request{Target: Target{Kind: "cluster"}}, Options{Preferences: DefaultPreferences()})
	m.accept("overview", clusterFixture(m.now))
	m.width, m.height = 200, 60
	l := m.layout()
	if len(l.charts) != 3 || l.capacity.Empty() || l.inspector.Empty() || l.summary.Empty() {
		t.Fatal("dense dashboard panels missing")
	}
	if l.table.Dy() > len(m.rows())+3 {
		t.Fatal("sparse table occupies unused vertical space")
	}
	view := m.renderChart(m.chartSeries()[0], 60, 12, 0)
	if !strings.Contains(view, "100%") || !strings.Contains(view, "50%") {
		t.Fatal("percentage graph is not on a 0–100 axis")
	}
	for _, size := range [][2]int{{60, 18}, {80, 30}, {100, 30}, {140, 40}, {200, 50}, {250, 72}} {
		m.width, m.height = size[0], size[1]
		for _, line := range strings.Split(m.View().Content, "\n") {
			if displayWidth(line) > m.width {
				t.Fatalf("%v overflow", size)
			}
		}
	}
}

func TestPausedChartsFreezePendingResultsAndTime(t *testing.T) {
	m := New(&testProvider{}, Request{Target: Target{Kind: "cluster"}}, Options{Preferences: DefaultPreferences()})
	m.accept("overview", clusterFixture(m.now))
	m.width, m.height = 200, 50
	press(m, "p")
	before := m.renderChart(m.chartSeries()[0], 80, 15, 0)
	m.Update(tickMsg(m.now.Add(2 * time.Minute)))
	m.Update(fetchedMsg{epoch: m.epoch, section: "overview", value: clusterFixture(m.now)})
	after := m.renderChart(m.chartSeries()[0], 80, 15, 0)
	if before != after {
		t.Fatal("paused chart moved or accepted an in-flight result")
	}
	press(m, "p")
	if m.renderChart(m.chartSeries()[0], 80, 15, 0) == before {
		t.Fatal("resuming did not restore wall time")
	}
}

func TestFreshResponseRendersBeforeNextTick(t *testing.T) {
	m := New(&testProvider{}, Request{Target: Target{Kind: "cluster"}}, Options{Preferences: DefaultPreferences()})
	m.now = m.now.Add(-time.Minute)
	now := time.Now()
	m.Update(fetchedMsg{epoch: m.epoch, section: "overview", value: clusterFixture(now)})
	if m.now.Before(now) || !strings.Contains(m.renderChart(m.chartSeries()[0], 80, 12, 0), "20.0%") {
		t.Fatal("newly received data was hidden until the next tick")
	}
}

func BenchmarkDashboardColor(b *testing.B) {
	m := testModel()
	m.accept("overview", fixture(1000))
	m.width, m.height, m.opts.Color = 200, 50, true
	b.ReportAllocs()
	for b.Loop() {
		_ = m.View()
	}
}
