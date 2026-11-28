# Module observability

A module owns its counters, gauges, and histograms. It updates them where the state changes and exposes an `obs.Source` for collection. The scenario or runtime chooses when to sample and where to send the snapshot. This keeps protocol updates integer-only, allocation-free, and independent of export I/O.

## Instrument a module

The buffered job transformer is the working example. Read its [state transitions](../job/buffered.go), [metrics helper](../job/buffered_stats.go), and [source registration](../scenario/builtins.go) in that order. The [job handler helpers](../job/handler_stats.go) share the counters common to both handler types. The [simulator source](../sim/stats.go) combines node and transport measurements at collection time.

1. Give the component a private stats helper with typed `obs.Counter`, `obs.Gauge`, and, where needed, `obs.Histogram` fields. A counter records a cumulative event count, a gauge records a current level, and a histogram records a distribution such as delivery latency. The gauge's `Peak()` can answer a high-water question even when the final level is zero.
2. Update the helper at the transition that owns the event. For example, count a rejection when the queued error indication runs and update queue depth on enqueue and dequeue. Keep names, units, help strings, and bounded labels in the source emitter. Avoid per-job identifiers as labels.
3. Expose the helper through the component's `MetricsSource` and register it with the scenario's `obs.Collector`. Call `Collect` to read a snapshot in a test or `Publish` to send a snapshot to the registered adapters. The collector sorts points and labels and copies snapshot storage for adapters. Live component state belongs to its owning execution loop.

The [object-store node helper](https://github.com/nickstrad/object-store/blob/31f9edcc18c927098eb5f21245ccdd73798dabc7/internal/node/metrics.go) shows how the same ownership works with bounded operation/outcome labels and an off-by-default configuration. It predefines series, uses a nil helper when disabled, and lets the node expose related component sources at composition time. Its [telemetry runner](https://github.com/nickstrad/object-store/blob/31f9edcc18c927098eb5f21245ccdd73798dabc7/internal/telemetry/runner.go) owns periodic collection, sequence and process identity, deadlines, and adapter publication. In this repository, typed values and deterministic source emission provide the module boundary. The scenario CLI publishes once at the end of each seed, using simulated time.

## Inspect a run with CSV and DuckDB

Run this from the repository root with DuckDB available on your path.

```sh
make build
out=$(mktemp -d)
./bin/dsasim run --config configs/job-buffered.json \
  --csv-dir "$out" --prom-file "$out/job-buffered.prom"
./bin/dsasim run --config configs/sim-ping.json --csv-dir "$out"
./bin/dsasim report "$out/job-buffered-seed-1"
cd "$out"
```

Each seed gets a directory containing schema-version-1 `metrics.csv` with scalar readings, `histograms.csv` with bucket counts, and `run.csv` with run metadata. The writer creates `run.csv` last as the completion marker. Use a fresh output directory for each invocation. Within a run, `flush_seq` identifies a snapshot and `t_ns` is elapsed simulation time in nanoseconds. The CLI produces one flush, sequence zero.

Check whether all submitted jobs were accepted or rejected. The query takes the latest sample of each series before checking the conservation equation.

```sh
duckdb <<'SQL'
WITH latest AS (
    SELECT component, metric, value
    FROM read_csv_auto('job-buffered-seed-1/metrics.csv')
    WHERE component = 'job_buffered' AND kind = 'counter'
      AND metric IN ('submitted', 'confirmed', 'errored')
    QUALIFY row_number() OVER (
        PARTITION BY run_id, instance, component, metric, labels_json
        ORDER BY flush_seq DESC
    ) = 1
)
SELECT
    max(value) FILTER (WHERE metric = 'submitted') AS submitted,
    max(value) FILTER (WHERE metric = 'confirmed') AS confirmed,
    max(value) FILTER (WHERE metric = 'errored') AS errored,
    submitted = confirmed + errored AS conserved
FROM latest;
SQL
```

The starter config yields 24 submitted, 23 confirmed, and 1 rejected. A gauge query can read `buffered_peak` from the same file to measure the highest queue occupancy.

Estimate the p95 simulated network delivery time from the final histogram. Each CSV row holds the count for one disjoint bucket. Sorting by upper bound and accumulating the counts finds the bucket containing the p95 observation.

```sh
duckdb <<'SQL'
WITH buckets AS (
    SELECT bucket_lo, bucket_hi,
           sum(count) OVER (ORDER BY bucket_hi ROWS UNBOUNDED PRECEDING) AS cumulative,
           sum(count) OVER () AS observations
    FROM read_csv_auto('sim-ping-seed-1/histograms.csv')
    WHERE component = 'sim' AND metric = 'net_delivery_latency' AND unit = 'ns'
      AND flush_seq = 0
)
SELECT observations, bucket_lo AS p95_lower_ns, bucket_hi AS p95_upper_ns
FROM buckets
WHERE cumulative * 100 >= observations * 95
ORDER BY bucket_hi
LIMIT 1;
SQL
```

The starter config records 42 deliveries. The p95 falls in the bucket from 13,434,880 to 13,500,415 ns.

For periodic sampling, scope time-series queries by run/process identity, component, metric, and labels, and order samples by sequence. Use the difference from the preceding sample within the same identity for interval counts. An initial retained sample has no preceding sample. Object-store's DuckDB views apply this pattern to finalized per-snapshot segments and partition by boot so a restart starts a new series. [Object-store CSV views](https://github.com/nickstrad/object-store/blob/31f9edcc18c927098eb5f21245ccdd73798dabc7/deploy/observability/views.sql)

## Prometheus and OpenTelemetry

**Available here.** `--prom-file` writes the final snapshot in Prometheus text exposition format 0.0.4. For example, inspect the total with `rg '^dsa_job_buffered_submitted_total' job-buffered.prom` from the output directory. `obs/promtext.Adapter.Handler()` can be mounted at `/metrics` by a host process. It reads the latest rendered body via an atomic pointer and returns HTTP 503 until the first snapshot is published. The current simulator CLI writes files and exits. Histogram export rebuckets onto fixed cumulative bounds and exposes `_bucket` and `_count`.

**Runtime integration path.** A future live runtime can schedule collection on the component's owning loop, then hand the captured snapshot to a publisher outside that loop. Direct Prometheus scraping uses the existing handler with that runtime wiring. An OpenTelemetry adapter would consume the same snapshot outside the deterministic core, add process identity and real timestamps at the runtime boundary, and map counters to cumulative monotonic OTLP sums and gauges to OTLP gauges. Histogram support requires an explicit mapping of bucket bounds, counts, units, and temporality. There is currently no OTLP adapter, periodic runtime publisher, or Collector deployment in this repository.

Object-store already demonstrates both live paths. Its direct Prometheus adapter serves cached exposition. Its OTLP adapter sends sums and gauges as OTLP/HTTP JSON to an OpenTelemetry Collector, which exposes a Prometheus scrape endpoint. In that checkout, `make metrics-up` starts three nodes, the Collector, and Prometheus, `make load` generates traffic, `make test-otel` checks the integration, and `make metrics-down` stops the stack. [Object-store observability guide](https://github.com/nickstrad/object-store/blob/31f9edcc18c927098eb5f21245ccdd73798dabc7/docs/observability.md)
