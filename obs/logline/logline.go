// Package logline renders each published obs.Snapshot as a single bounded
// key=value summary line suitable for terminal or structured process logs.
package logline

import (
	"bytes"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/nickstrad/distributed-systems-algorithms/obs"
)

const (
	AdapterName   = "log"
	MaxLineBytes  = 1024
	TruncatedMark = " truncated=1"
)

// Adapter implements obs.Adapter, rendering one bounded log line per publish.
type Adapter struct {
	w    io.Writer
	last []byte
}

// New creates a logline Adapter writing each rendered line to w (if non-nil).
func New(w io.Writer) *Adapter { return &Adapter{w: w} }

// Name returns AdapterName ("log").
func (a *Adapter) Name() string { return AdapterName }

// Export renders s, saves the line for Last, and writes it to w if non-nil.
func (a *Adapter) Export(s obs.Snapshot) error {
	a.last = Render(s)
	if a.w == nil {
		return nil
	}
	_, err := a.w.Write(a.last)
	return err
}

// Last returns a copy of the most recently rendered log line.
func (a *Adapter) Last() []byte { return slices.Clone(a.last) }

// Render formats s as one newline-terminated line of at most MaxLineBytes bytes.
func Render(s obs.Snapshot) []byte {
	var b bytes.Buffer
	b.Grow(256)
	b.WriteString("obs.publish")
	appendField(&b, "seq", strconv.FormatInt(s.Seq, 10))
	appendField(&b, "t_ns", strconv.FormatInt(s.TimeNs, 10))
	if s.Instance != "" {
		appendField(&b, "instance", s.Instance)
	}
	appendField(&b, "points", strconv.Itoa(len(s.Points)))
	appendField(&b, "histograms", strconv.Itoa(len(s.Hists)))

	budget := MaxLineBytes - 1 // reserve trailing '\n'
	var numBuf [20]byte
	for _, p := range s.Points {
		if len(p.Labels) > 0 {
			continue // keep headline line concise with unlabelled scalars
		}
		val := strconv.AppendInt(numBuf[:0], p.Value, 10)
		entryLen := 1 + len(p.Component) + 1 + len(p.Metric) + 1 + len(val)
		if b.Len()+entryLen+len(TruncatedMark) > budget {
			b.WriteString(TruncatedMark)
			break
		}
		b.WriteByte(' ')
		b.WriteString(p.Component)
		b.WriteByte('.')
		b.WriteString(p.Metric)
		b.WriteByte('=')
		b.Write(val)
	}
	b.WriteByte('\n')
	return b.Bytes()
}

func appendField(b *bytes.Buffer, k, v string) {
	b.WriteByte(' ')
	b.WriteString(k)
	b.WriteByte('=')
	if v == "" || strings.ContainsAny(v, " \t\r\n=\"") {
		b.WriteString(strconv.Quote(v))
		return
	}
	b.WriteString(v)
}
