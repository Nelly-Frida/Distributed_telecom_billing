# Power BI — Weeks 9 and 10 (no slicers across tables)

Every visual uses ONE table, and filtering is done with the **Filters** pane of that visual, so no relationships are needed.

## Load the tables
Spark folders (pick the `part-00000-….csv` inside each): `week9_request_metrics` → `Week9_RequestMetrics`, `week9_error_timeline` → `Week9_ErrorTimeline`, `week9_overhead` → `Week9_Overhead`,
`week10_resolution_metrics` → `Week10_Resolution`, `week10_latency_by_messages` → `Week10_ByMessages`.
Plain CSVs from `results\`: `week9_summary.csv` → `Week9_Summary`, `week9_requests.csv` → `Week9_Requests`, `week10_summary.csv` → `Week10_Summary`.

**Step A: Make the summary `value` column numeric (Week9_Summary and Week10_Summary)**
1. Select **Transform data**.
2. Select `Week9_Summary`, then the `value` column.
3. Select **Transform → Data Type → Decimal Number**.
4. With the column still selected, select **Transform → Replace Values → Replace Errors**, enter `null` and select **OK**. True/False rows become empty and all numbers stay numbers.
5. Repeat for `Week10_Summary` and select **Close & Apply**.

## Week 9 page

**Step 1: Create the migration story (the key chart)**
1. Select **Line chart**.
2. From `Week9_ErrorTimeline`, drag `second` → **X-axis**, `error_pct` → **Y-axis** (**Average**) and `variant` → **Legend**.
3. In the **Filters** pane add `experiment` → `migration`. The direct line jumps to 100 % when the partition moves and the transparent line stays at 0.
4. Copy the chart and change the filter to `failure`. The direct line is high only during the crash outage.

**Step 2: Create the "who produced the errors" donut**
1. Select **Donut chart**.
2. From `Week9_RequestMetrics`, drag `variant` → **Legend** and `errors` → **Values** (**Sum**).
3. Filter `experiment` → `failure`. Only the direct client has a slice; copy it and use `migration`.

**Step 3: Create the overhead charts**
1. Select **Clustered bar chart**. From `Week9_Overhead`, drag `variant` → **Y-axis**, `mean_ms` → **X-axis** (**Average**) and `op` → **Legend**. It shows direct, gateway R=1, R=2, R=3 for reads and writes.
2. Select **Clustered bar chart** again with `p95_ms` → **X-axis** instead.

**Step 4: Create the replication chart**
1. Select **Clustered column chart**.
2. From `Week9_Summary`, drag `variant` → **X-axis**, `value` → **Y-axis** (**Average**) and `metric` → **Legend**.
3. Filter `experiment` → `replication` and `metric` → `write_mean_ms` and `read_mean_ms`. Writes grow with R, reads much less.

**Step 5: Create the "reads are shared" donut**
1. Select **Donut chart**.
2. From `Week9_Requests`, drag `served_by` → **Legend** and `latency_ms` → **Values** (**Count**).
3. Filter `experiment` → `replication`, `op` → `get`, `ok` → `True`. The slices are the replicas serving reads.

**Step 6: Create the location and cost cards**
1. Select **Clustered bar chart**. From `Week9_Summary`: `variant` → **Y-axis**, `value` → **X-axis**; filter `experiment` → `location` and `metric` → `mean_resolve_ms`.
2. Select **Card**. From `Week9_Summary`: `value` → **Fields** (**Maximum**); filter `experiment` → `migration`, `variant` → `transparent`, `metric` → `freeze_ms`. Title it "Migration freeze (ms)".
3. Add two more cards for `metric` → `lost_writes` (**Sum**, title "Lost writes") and `experiment` → `access`, `metric` → `identical_results_pct`.

## Week 10 page

**Step 7: Create the strategy comparison**
1. Select **Clustered bar chart**. From `Week10_Resolution`: `strategy` → **Y-axis**, `mean_ms` → **X-axis** (**Average**). Select **Format → X-axis → Scale type → Log**, because the values span three orders of magnitude.
2. Select **Clustered column chart**: `strategy` → **X-axis**, `messages_per_lookup` → **Y-axis** (**Average**).

**Step 8: Create the "how many messages did lookups cost" donut**
1. Select **Donut chart**.
2. From `Week10_ByMessages`, drag `messages` → **Legend** and `lookups` → **Values** (**Sum**).
3. The slices show the share of lookups answered with 0, 1, 2 or 4 messages across all strategies.

**Step 9: Create the staleness and outage charts**
1. Select **Clustered bar chart**. From `Week10_Summary`: `variant` → **Y-axis**, `value` → **X-axis** (**Sum**); filter `experiment` → `staleness` and `metric` → `wrong_lookups_before_correct`.
2. Select **Clustered bar chart**. Same fields; filter `experiment` → `ns_outage` and `metric` → `resolved_during_outage_pct`.

**Step 10: Create the scalability charts**
1. Select **Clustered column chart**. From `Week10_Summary`: `variant` → **X-axis**, `value` → **Y-axis** (**Average**); filter `experiment` → `scalability_nodes` and `metric` → `mean_ms`. Broadcast grows with N.
2. Select **Clustered column chart**. Same fields; filter `experiment` → `scalability_clients` and `metric` → `throughput_lookups_s`. Use a log scale on the Y-axis.
3. Select **Clustered column chart**. Same fields; filter `experiment` → `scalability_names` and `metric` → `prefix_table_load_ms`.

**Step 11: Create the process charts**
1. Select **Clustered bar chart**. From `Week10_Summary`: `variant` → **Y-axis**, `value` → **X-axis** (**Average**); filter `experiment` → `scheduling` and `metric` → `makespan_s_mean`.
2. Select **Line chart**. `variant` → **X-axis**, `value` → **Y-axis** (**Average**); filter `experiment` → `coordination` and `metric` → `speedup`.
3. Select **Donut chart**. Filter `experiment` → `scheduling` and `metric` → `max_node_records`, with `variant` → **Legend** and `value` → **Values** (**Sum**): the share of work the busiest node carries per policy.

**Step 12: Create the recovery cards**
1. Select **Card**. From `Week10_Summary` use `value` → **Fields** (**Maximum**), one card for each filter set and give each a title:
   - `experiment` → `recovery`, `metric` → `first_reschedule_s` ("First task rescheduled (s)")
   - `experiment` → `migration_process`, `metric` → `downtime_ms` ("Process migration downtime (ms)")
   - `experiment` → `lifecycle`, `metric` → `names_left` ("Names left behind")
   - `experiment` → `lifecycle`, `metric` → `tasks_left_on_nodes` ("Tasks left behind")

**Step 13: Arrange the pages**
Put the cards at the top, the key line chart (Step 1 on the Week 9 page, Step 7 on the Week 10 page) beside them, the donuts in a row and the bar charts at the bottom, like your earlier pages.
