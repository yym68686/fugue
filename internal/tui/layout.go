package tui

import (
	"image"
	"strings"

	"github.com/charmbracelet/x/ansi"
)

type hit struct {
	id, label string
	bounds    image.Rectangle
}
type layoutState struct {
	body, table, summary, events image.Rectangle
	capacity, inspector, context image.Rectangle
	charts                       []image.Rectangle
	hits                         []hit
}

// Layout is the single geometry authority for both paint and input. No control
// is assigned an interactive rectangle that differs from its visible label.
func (m *Model) layout() layoutState {
	w, h := m.width, m.height
	l := layoutState{body: image.Rect(1, 3, max(1, w-1), max(3, h-2))}
	x := 1
	for i, name := range []string{"dashboard", "resources", "events", "logs", "tasks", "details"} {
		if m.snapshot.UnavailableScreens[name] != "" {
			continue
		}
		label := strings.ToUpper(name[:1]) + name[1:]
		if w < 90 {
			label = string(rune('1'+i)) + " " + name[:3]
		}
		size := ansi.StringWidth(label) + 2
		if x+size > w-1 {
			break
		}
		l.hits = append(l.hits, hit{"screen:" + name, label, image.Rect(x, 2, x+size, 3)})
		x += size + 1
	}
	x = w - 1
	for _, control := range [][2]string{{"palette", ": menu"}, {"refresh", "r refresh"}, {"pause", "p pause"}, {"search", "/ search"}, {"actions", "a actions"}, {"window", m.prefs.Window}} {
		size := ansi.StringWidth(control[1]) + 2
		if x-size < 0 {
			break
		}
		l.hits = append(l.hits, hit{control[0], control[1], image.Rect(x-size, max(0, h-1), x, h)})
		x -= size
	}
	if w < 30 || h < 10 {
		return l
	}
	if m.screen != "dashboard" {
		if m.screen == "resources" {
			l.table = l.body
			addTableHits(&l)
		}
		return l
	}
	series := m.chartSeries()
	count := len(series)
	if m.prefs.panelHidden("metrics") {
		count = 0
	}
	body := l.body
	rows := len(m.rows())
	inset := func(r image.Rectangle) image.Rectangle { return r.Inset(1) }
	if w >= 110 && h >= 30 {
		if count == 0 {
			split := body.Min.X + body.Dx()*3/5
			l.table = inset(image.Rect(body.Min.X, body.Min.Y, split, body.Max.Y))
			middle := body.Min.Y + body.Dy()/2
			l.inspector = inset(image.Rect(split+1, body.Min.Y, body.Max.X, middle))
			l.summary = inset(image.Rect(split+1, middle, body.Max.X, body.Max.Y))
		} else {
			top := body.Min.Y + min(18, max(10, body.Dy()/3))
			split := body.Min.X + body.Dx()*2/3
			l.charts = append(l.charts, inset(image.Rect(body.Min.X, body.Min.Y, split, top)))
			l.capacity = inset(image.Rect(split+1, body.Min.Y, body.Max.X, top))
			left := body.Min.X + body.Dx()*43/100
			remaining := body.Max.Y - top
			secondaryEnd := top + remaining/2
			if count == 1 {
				secondaryEnd = top
			}
			if count > 1 {
				columns := min(2, count-1)
				for i := 0; i < columns; i++ {
					x1 := body.Min.X + i*(left-body.Min.X)/columns
					x2 := body.Min.X + (i+1)*(left-body.Min.X)/columns
					l.charts = append(l.charts, inset(image.Rect(x1, top, x2, secondaryEnd)))
				}
			}
			if count > 3 {
				columns := min(2, count-3)
				for i := 0; i < columns; i++ {
					x1 := body.Min.X + i*(left-body.Min.X)/columns
					x2 := body.Min.X + (i+1)*(left-body.Min.X)/columns
					l.charts = append(l.charts, inset(image.Rect(x1, secondaryEnd, x2, body.Max.Y)))
				}
			} else {
				l.summary = inset(image.Rect(body.Min.X, secondaryEnd, left, body.Max.Y))
			}
			tableEnd := top + min(max(8, rows+5), max(8, remaining*3/5))
			l.table = inset(image.Rect(left+1, top, body.Max.X, tableEnd))
			l.inspector = inset(image.Rect(left+1, tableEnd, body.Max.X, body.Max.Y))
			row := m.selectedRow()
			if row != nil && row.Detail != nil && (len(row.Detail.Items) > 0 || len(m.snapshot.Events) > 0) && l.inspector.Dx() >= 90 && l.inspector.Dy() >= 12 {
				middle := l.inspector.Min.X + l.inspector.Dx()/2
				l.context = image.Rect(middle+1, l.inspector.Min.Y, l.inspector.Max.X, l.inspector.Max.Y)
				l.inspector.Max.X = middle - 1
			}
		}
	} else {
		below := body.Min.Y
		if count > 0 {
			columns := 1
			if w >= 72 {
				columns = min(2, count)
			}
			chartHeight := min(12, max(6, body.Dy()/2))
			for i := 0; i < columns; i++ {
				x1 := body.Min.X + i*body.Dx()/columns
				x2 := body.Min.X + (i+1)*body.Dx()/columns
				l.charts = append(l.charts, inset(image.Rect(x1, body.Min.Y, x2, body.Min.Y+chartHeight)))
			}
			below += chartHeight
		}
		l.table = inset(image.Rect(body.Min.X, below, body.Max.X, body.Max.Y))
	}
	if m.prefs.panelHidden("summary") {
		l.summary = image.Rectangle{}
	}
	if row := m.selectedRow(); row != nil && row.Detail != nil && len(row.Detail.Series) > 0 {
		label := "o overview"
		if m.overviewCharts {
			label = "o selection"
		}
		size := ansi.StringWidth(label) + 2
		if w >= 110 {
			l.hits = append(l.hits, hit{"chart-scope", label, image.Rect(w-size-1, 2, w-1, 3)})
		}
	}
	addTableHits(&l)
	return l
}
func (m *Model) modalBounds() image.Rectangle {
	w := min(76, max(1, m.width-4))
	height := min(max(1, m.height-4), max(9, len(m.modalItems())+5))
	if m.modal == "confirm" {
		height = min(max(1, m.height-4), 16)
	}
	x, y := (m.width-w)/2, (m.height-height)/2
	return image.Rect(x, y, x+w, y+height)
}

func addTableHits(l *layoutState) {
	if l.table.Empty() {
		return
	}
	l.hits = append(l.hits, hit{"table", "", image.Rect(l.table.Min.X, l.table.Min.Y, l.table.Max.X-8, l.table.Min.Y+1)}, hit{"sort", "s sort", image.Rect(l.table.Max.X-8, l.table.Min.Y, l.table.Max.X, l.table.Min.Y+1)})
}
