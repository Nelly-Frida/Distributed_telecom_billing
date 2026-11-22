# Power BI Dashboard

## Data sources
All CSVs are written to `powerbi/dashboard_data/`:
    transactions.csv   - one row per request (now includes a run_tag column)
    node_stats.csv     - per-node counters
    coordination.csv   - Week 4 election/failure events
    system.csv         - CPU + memory samples

In Power BI Desktop:
    Get Data -> Text/CSV -> point at each file.

Or via Apache:
    http://127.0.0.1:80/data/transactions.csv   (or just http://127.0.0.1/data/transactions.csv)

## Large-scale runs (e.g. 100,000 requests)
Use `experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag <name>`
instead of the Week 3 script for big runs — it uses a thread pool to fire
many requests at once (a sequential run at 100k requests would take
20-30+ minutes; concurrency brings this down substantially, see its
--concurrency flag). Every row it writes gets tagged in the run_tag
column, so you can:
- Filter the whole report by a single run using a run_tag slicer.
- Run once tagged "baseline" and once tagged "proposed" (see brief
  section 6 — Baseline vs Proposed System) and compare them side by
  side with the same visuals, just swapping the slicer.

After a large run finishes, transactions.csv and node_stats.csv are
flushed automatically (the script calls the API's /flush endpoint) so
what you see in Power BI is complete and not missing the last batch
still sitting in memory.

## Tips for 100k+ rows in Power BI
    In Power BI: right-click the timestamp column -> Change Type -> Date/Time,
    since raw Unix timestamps won't group nicely on a time axis by default.
    Use "Import" mode (not DirectQuery) for local CSV files this size -
    100k rows imports in seconds and every visual then responds instantly.
    Add a run_tag slicer near the top of the report so every visual can
    be filtered to one run (or "All") without rebuilding anything.

## Recommended visuals
    Card  Total Requests   = COUNTROWS(transactions)
    Card  Total Revenue    = SUM(transactions[charge])
    Card  Avg Latency      = AVERAGE(transactions[latency_ms])
    Bar   Revenue by service
    Bar   Requests per rating node
    Line  Latency over time
    Table coordination.csv
    Slicer run_tag         (filters every visual above to one experiment run)
