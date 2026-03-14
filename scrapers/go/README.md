# Go Scraper Experiments

This directory contains Go ports of the Python scrapers, for performance benchmarking.

## Why test Go?

| Metric | Python + httpx | Go |
|---|---|---|
| Startup time | ~300 ms | ~5 ms |
| HTTP throughput | Good | Excellent |
| Memory | ~50 MB | ~8 MB |
| Concurrency | asyncio (single thread) | goroutines (OS threads) |
| Binary | Requires Python + venv | Single static binary |

## Scrapers

### `kicksonfire/`

Port of `fetch_release_kicksonfire.py`. Uses `net/http` + `goquery` (jQuery-like
HTML parsing for Go).

**Run:**
```bash
cd scrapers/go/kicksonfire
go mod tidy          # downloads goquery dependency
go run . --days 35 --output ../../../data/fallback_kicksonfire_go.json
```

**Benchmark vs Python:**
```bash
# Python (with httpx fast path)
time python fetch_release_kicksonfire.py --days 35 --output /tmp/kof_py.json

# Go
time go run ./scrapers/go/kicksonfire --days 35 --output /tmp/kof_go.json

# Compare output
python -c "
import json
py = json.load(open('/tmp/kof_py.json'))
go = json.load(open('/tmp/kof_go.json'))
print(f'Python: {len(py)} releases')
print(f'Go:     {len(go)} releases')
"
```

## Integration strategy (if Go wins)

If the Go scraper is faster AND produces equivalent output, it can be
integrated into the GitHub Actions workflow by:

1. Installing Go in the workflow: `uses: actions/setup-go@v5`
2. Building the binary: `go build -o kicksonfire ./scrapers/go/kicksonfire`
3. Replacing the Python call: `./kicksonfire --days 35 --output data/fallback_kicksonfire.json`

The output JSON schema is identical, so `merge_and_compare.py` needs no changes.

## Branch policy

This is the `experiment/go-scrapers` branch.
- **DO NOT merge to main without benchmarking first.**
- If Go wins: open a PR to the main working branch.
- If Python + httpx is fast enough: abandon this branch.
