package sim

import "github.com/nickstrad/distributed-systems-algorithms/obs"

type simStats struct {
	crashes    obs.Counter
	recoveries obs.Counter
	liveNodes  obs.Gauge
}

type networkStats struct {
	sent       obs.Counter
	dropped    obs.Counter
	duplicated obs.Counter
	delivered  obs.Counter
	latency    *obs.Histogram
}

// MetricsSource returns an obs.Source exporting deterministic simulator and
// network counters, gauges, and delivery-latency histograms.
func (s *Sim) MetricsSource() obs.Source {
	return obs.SourceFunc{
		Name: "sim",
		Fn: func(e *obs.Emitter) {
			e.Counter("steps", "count", "Total simulator events executed", s.steps)
			e.Counter("messages_sent", "count", "Total messages submitted to simulated network", s.net.stats.sent.Value())
			e.Counter("messages_dropped", "count", "Total messages dropped by loss or partition", s.net.stats.dropped.Value())
			e.Counter("messages_duplicated", "count", "Total extra duplicate message copies scheduled", s.net.stats.duplicated.Value())
			e.Counter("messages_delivered", "count", "Total messages delivered to live nodes", s.net.stats.delivered.Value())
			e.Counter("node_crashes", "count", "Total simulated node crashes", s.stats.crashes.Value())
			e.Counter("node_recoveries", "count", "Total simulated node recoveries", s.stats.recoveries.Value())
			e.Gauge("nodes_live", "count", "Current number of live simulated nodes", s.stats.liveNodes.Value())
			e.Gauge("nodes_live_peak", "count", "Peak number of live simulated nodes", s.stats.liveNodes.Peak())
			e.Histogram("net_delivery_latency", "ns", "Simulated message delivery latency", s.net.stats.latency)
		},
	}
}
