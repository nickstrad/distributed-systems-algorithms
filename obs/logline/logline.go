// Package logline renders each published obs.Snapshot as a single bounded
// key=value summary line suitable for terminal or structured process logs.
package logline

import (
	"bytes"
	"fmt"
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
	n, err := a.w.Write(a.last)
	if err == nil && n != len(a.last) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("logline: write snapshot %d: %w", s.Seq, err)
	}
	return nil
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
	// Reserve space for the summary fields and the truncation marker before
	// adding the optional instance. Escaping can expand a short string.
	var counts bytes.Buffer
	appendField(&counts, "points", strconv.Itoa(len(s.Points)))
	appendField(&counts, "histograms", strconv.Itoa(len(s.Hists)))
	budget := MaxLineBytes - 1 - len(TruncatedMark)
	truncated := false
	if s.Instance != "" {
		start := b.Len()
		if len(s.Instance) <= budget {
			appendField(&b, "instance", s.Instance)
		}
		if len(s.Instance) > budget || b.Len()+counts.Len() > budget {
			b.Truncate(start)
			truncated = true
		}
	}
	b.Write(counts.Bytes())

	var numBuf [20]byte
	for _, p := range s.Points {
		if len(p.Labels) > 0 {
			continue // keep headline line concise with unlabelled scalars
		}
		if strings.ContainsAny(p.Component, " \t\r\n=\"") || strings.ContainsAny(p.Metric, " \t\r\n=\"") {
			truncated = true
			continue
		}
		val := strconv.AppendInt(numBuf[:0], p.Value, 10)
		entryLen := 1 + len(p.Component) + 1 + len(p.Metric) + 1 + len(val)
		if b.Len()+entryLen > budget {
			truncated = true
			break
		}
		b.WriteByte(' ')
		b.WriteString(p.Component)
		b.WriteByte('.')
		b.WriteString(p.Metric)
		b.WriteByte('=')
		b.Write(val)
	}
	if truncated {
		b.WriteString(TruncatedMark)
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
