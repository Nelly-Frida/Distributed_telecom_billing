# Power BI Dashboard

## Data sources
All CSVs are written to `powerbi/dashboard_data/`:
    transactions.csv   - one row per request
    node_stats.csv     - per-node counters
    coordination.csv   - Week 4 election/failure events
    system.csv         - CPU + memory samples

In Power BI Desktop:
    Get Data -> Text/CSV -> point at each file.

Or via Apache:
    http://127.0.0.1:8080/data/transactions.csv

## Recommended visuals
    Card  Total Requests   = COUNTROWS(transactions)
    Card  Total Revenue    = SUM(transactions[charge])
    Card  Avg Latency      = AVERAGE(transactions[latency_ms])
    Bar   Revenue by service
    Bar   Requests per rating node
    Line  Latency over time
    Table coordination.csv
