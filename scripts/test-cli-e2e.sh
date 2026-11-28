#!/bin/sh
set -eu

# Run from the repository root after building bin/dsasim.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

expect_line() {
    if ! grep -Eq "$1" "$2"; then
        printf 'Missing expected pattern %s in %s\n' "$1" "$2" >&2
        cat "$2" >&2
        exit 1
    fi
}

bin/dsasim run --config configs/job-buffered.json --check-determinism \
    --story --csv-dir "$work" --prom-file "$work/metrics.prom" > "$work/story"
expect_line 'OVERALL: PASS' "$work/story"
bin/dsasim report "$work/job-buffered-seed-1" > "$work/report"
expect_line 'scenario=job-buffered[[:space:]]+seed=1' "$work/report"
# Seed 1 generates 24 submissions across the eight bursts.
expect_line 'job_buffered\.submitted[[:space:]]+24[[:space:]]' "$work/report"
expect_line '^dsa_job_buffered_submitted_total\{instance="job-buffered"\} 24$' "$work/metrics.prom"

if bin/dsasim run missing-suite > "$work/output" 2> "$work/error"; then
    echo 'Expected an unknown suite to exit unsuccessfully' >&2
    exit 1
fi
expect_line 'unknown suite "missing-suite"' "$work/error"
