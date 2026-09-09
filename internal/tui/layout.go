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
	charts                       []image.Rectangle
	hits                         []hit
}

// Layout is the single geometry authority for both paint and input. No control
// is assigned an interactive rectangle that differs from its visible label.
func (m *Model) layout() layoutState {
	w, h := m.width, m.height
	l := layoutState{body: image.Rect(1, 4, max(1, w-1), max(4, h-2))}
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
	chartHeight := max(6, min(10, l.body.Dy()/2))
	columns := 1
	if w >= 180 {
		columns = 5
	} else if w >= 140 {
		columns = 4
	} else if w >= 100 {
		columns = 3
	} else if w >= 72 {
		columns = 2
	}
	count := min(len(m.snapshot.Series), columns)
	if count == 0 {
		count = 1
	}
	chartWidth := (l.body.Dx() - (count - 1)) / count
	for i := 0; i < count; i++ {
		right := l.body.Min.X + (i+1)*(chartWidth+1) - 1
		if i == count-1 {
			right = l.body.Max.X
		}
		l.charts = append(l.charts, image.Rect(l.body.Min.X+i*(chartWidth+1), l.body.Min.Y, right, l.body.Min.Y+chartHeight))
	}
	below := l.body.Min.Y + chartHeight + 1
	if m.prefs.panelHidden("metrics") {
		l.charts = nil
		below = l.body.Min.Y
	}
	l.table = image.Rect(l.body.Min.X, below, l.body.Max.X, l.body.Max.Y)
	if w >= 110 && l.table.Dy() >= 7 && !m.prefs.panelHidden("summary") {
		left := l.body.Min.X + l.body.Dx()*2/3
		l.table.Max.X = left - 1
		l.summary = image.Rect(left, below, l.body.Max.X, l.body.Max.Y)
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
