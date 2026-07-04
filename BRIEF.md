# Tessera — Project Brief

## Business context

Tessera is a fictional consumer-facing analytics product sitting on top of
New York City's High-Volume For-Hire-Vehicle data — the trip records that
Uber, Lyft, and Via file with the city every month for every ride. The
product is sold as a self-serve web app: a fleet manager, a city
transportation planner, or a curious journalist signs in, picks a question
from a small menu of canned views — surge hotspots, wait-time percentiles,
shared-ride match rates, driver-pay share, borough-pair duration shift —
clicks a zone or a borough or a month, and gets an answer back. The screen
has to feel **interactive**. If the operator has to wait three seconds to
see the next bar of a chart, they stop using the product and the
subscription churns. The customer-facing promise, written into every sales
deck, is the same line: *"every answer in under a second."*

Tessera doesn't generate the data. The city publishes a new monthly parquet
file for HV-FHV trips a few weeks after the month closes, dropped into a
shared bucket alongside the previous months' files and a small zone-lookup
CSV. Each file is ~470 MB on disk, ~20 M rows, and over the four months
2024-09 .. 2024-12 you're looking at roughly **80 million raw trip
records** — Manhattan, the outer boroughs, Newark, JFK and LGA, the airport
runs at 4 AM, the Friday-night spikes, the Sunday-morning lulls. Eighty
million rows is small for a warehouse, big enough to be slow for a naïve
`SELECT … GROUP BY` against the raw files, and exactly the wrong size for
"just run DuckDB on the parquet" to be the whole answer when the customer
expects sub-second.

Your team's mandate is to build that serving system. The product surface is
the five business questions in the DOD below — they're a fair stand-in for
the catalogue of canned views the product would ship. The pressure on the
system is not ingestion correctness, not throughput, not freshness in the
streaming-100-ms sense. **The pressure is query latency.** Every one of the
five questions must return in **under one second, query-only, warm
process**. That is the SLO. You have three weeks before you defend the
system live in class.

## Data source

The upstream data provider is encapsulated as a sidecar in this scaffold —
a MinIO bucket called `tlc-drops`, populated by a `tlc-publisher` service
that drips each month's parquet into the bucket as its simulated
publication date arrives. The publisher reads cached copies of the real
NYC TLC files (downloaded from CloudFront once by `data-init` on first
`make run`); from your team's point of view it behaves exactly like a real
public S3 provider would. The replay clock is configurable
(`REPLAY_SECONDS_PER_MONTH` in `.env`, default 75) so the four-month
window completes in roughly five wall-clock minutes — short enough to demo
end-to-end during your defence slot.

**Endpoint and credentials.** MinIO listens on `http://localhost:18900`
(S3 API). Access key `tessera`, secret key `tessera-secret`. The bucket is
`tlc-drops`. A web console is exposed at `http://localhost:18901` if you
want to eyeball the bucket contents during development.

**Discovery contract — `S3 LIST` only.** There is no manifest endpoint, no
push notification, no webhook. To know what's in the bucket you `LIST`
it. To know whether a file you've already processed has changed since, you
compare ETags. The idempotency key for a drop is **`(filename, etag)`** —
two `LIST` responses with the same filename and same ETag describe the
same bytes; a different ETag means the file has been re-published with
different bytes, and your pipeline must reprocess it.

**Object names.** Monthly trip files are named
`fhvhv_tripdata_YYYY-MM.parquet`. The zone-lookup CSV is
`taxi_zone_lookup.csv`. Nothing else lives in the bucket; you don't need
to filter prefixes.

**HV-FHV file schema.** Stable across the chosen window
(2024-09 .. 2024-12). The columns you'll care about for the five questions
are at minimum:

- `hvfhs_license_num` (string) — `HV0003` is Uber, `HV0005` is Lyft,
  `HV0004` is Via.
- `request_datetime`, `on_scene_datetime`, `pickup_datetime`,
  `dropoff_datetime` (microsecond timestamps; treat as UTC for simplicity).
- `PULocationID`, `DOLocationID` (int) — join keys against
  `taxi_zone_lookup.csv`.
- `trip_miles` (double), `trip_time` (long, seconds).
- `base_passenger_fare`, `tolls`, `bcf`, `sales_tax`,
  `congestion_surcharge`, `airport_fee`, `tips`, `driver_pay` (double).
- `shared_request_flag`, `shared_match_flag` (string `Y`/`N`).

The zone-lookup CSV is the standard four-column file
(`LocationID,Borough,Zone,service_zone`). Join `PULocationID` to get
`pickup_borough` / `pickup_zone`; join `DOLocationID` to get
`dropoff_borough` / `dropoff_zone`.

**Resolving "now".** The publisher exposes a `/simulated_now` endpoint
returning the current simulated wall-clock as an ISO-8601 UTC timestamp.
Questions like "the most recent complete month" and "the trailing 30 days"
must resolve against this value **at query time**, not at ingest time. A
small wrapper script that reads `/simulated_now` and templates it into the
SQL is fine; baking a fixed cutoff into a materialised view is not — the
answer would go stale as the replay advances.

**Things to expect from the provider.** This scaffold simulates exactly
one failure mode, on demand (see `make vendor-chaos`); your system must
cope without operator intervention.

- **Late corrections.** A previously-published month's parquet file will
  occasionally be re-PUT with a small mutation — a handful of rows have
  their `base_passenger_fare` nudged by a seeded RNG. The mutation is
  real (the parquet bytes are different and MinIO returns a new ETag), so
  your ETag-based change detection will see it. Your serving layer must
  reflect the corrected values, not the originals. The default rate is
  zero; `make vendor-chaos` raises it to one-in-two on each new-month
  publication roll.

That's the only failure mode. There are no partial drops, no schema
drifts, no outages. The single chaos surface is deliberately narrow — the
pressure on this project is the serving layer, not the ingestion-trust
problem the other capstones explore.

## Definition of done

Tessera defines success around one concrete deliverable plus an
operational baseline they expect of any production-grade system you hand
them.

### The five business questions, each under one second

The serving surface is the five questions below. The customer's promise is
sub-second response on every one of them, and your defence is exactly
that: at your slot you will run each query live, measure its latency, and
the bar is **< 1 second, query-only, warm process**. "Query-only" means
the time we count starts when the query is sent and ends when the rows are
returned; cold-start one-off costs (process boot, connection pool warm-up,
buffer cache from a previous run) are excluded. "Warm process" means you
have a persistent SQL client running and the relevant pages or cube
fragments have been touched once before — exactly the state a customer
clicking around in the product would be in.

The questions resolve against the publisher's `/simulated_now` at query
time.

1. **Surge-proxy hotspots.** Find the top 100
   `(pickup_zone, hour_of_day, day_of_week)` cells with the highest
   **median dollars-per-mile** — defined as
   `base_passenger_fare / NULLIF(trip_miles, 0)` — over the **most recent
   complete simulated month**, restricted to cells with at least 200
   completed trips. Return zone, hour, day-of-week, the median, and the
   trip count.

2. **Wait-time percentiles by zone and hour.** Over the **trailing 30
   simulated days**, compute for each `(pickup_zone, hour_of_day)` pair
   the p50 and p90 of the wait between `request_datetime` and
   `pickup_datetime`, in seconds. Return the 25 zones with the **worst
   p90 wait at 8 AM**, with zone, p50, p90, and the sample size at 8 AM.

3. **Shared-ride match rate.** Over the **trailing 90 simulated days**,
   identify the 20 `(pickup_zone, dropoff_zone)` pairs with the highest
   match rate, defined as the count of trips with
   `shared_match_flag = 'Y'` divided by the count of trips with
   `shared_request_flag = 'Y'`. Restrict to pairs with at least 500
   shared requests. Return both zones, the ratio, and the denominator.

4. **Driver-pay share, monthly.** For each licensee (Uber `HV0003`, Lyft
   `HV0005`, Via `HV0004`) and each month in the replay window, return
   the total gross fare (`base_passenger_fare + tolls + bcf + sales_tax +
   congestion_surcharge + airport_fee + tips`), the total `driver_pay`,
   and the percentage of gross paid to drivers, ordered by month then
   licensee.

5. **Borough-pair rush-hour duration shift.** For weekday rush hours
   (07:00-10:00 and 16:00-19:00, by local hour-of-day on
   `pickup_datetime`), compute the median trip duration (`trip_time`) per
   `(pickup_borough, dropoff_borough)` pair, separately for the **first
   half** and the **second half** of the replay window (the cut point is
   the midpoint of the configured window). Return the 10 pairs with the
   largest percentage change in median duration between halves, with both
   medians, the percentage change, and the sample sizes.

Each question must return its answer in **under 1 second, query-only,
warm process**, over the full four-month dataset (~80 M rows). After a
late-correction round, the same queries — re-run — must reflect the
corrected `base_passenger_fare` values; **still under 1 second**.

How you get there is open. The obvious path is Postgres with materialised
views, hand-rolled indexes on the cube grain you actually query, and a
small wrapper that resolves `simulated_now` into the SQL at run time.
Equally valid: Redis hot cubes keyed by `(zone, hour, day_of_week)`,
DuckDB on pre-aggregated parquet cubes, per-cell sketches (HLL, t-digest)
maintained incrementally, or a hybrid where the heavy aggregations live
in Postgres and the latest-month cube lives in Redis for the surge-proxy
question. We will ask you to defend the choice, not vote on it.

A few things follow from the SLO and shouldn't catch you out:

- **Eighty million rows is too many to scan at query time.** A naïve
  `SELECT … FROM raw_trips GROUP BY zone, hour, day_of_week` over the
  full window will not return in under a second on a laptop, regardless
  of how much RAM you throw at it. You will need pre-aggregated state.
- **Pre-aggregated state must be kept current.** When a new month's file
  lands in the bucket, your cubes / materialised views / hot keys must
  reflect it before your defence runs the question against it. When a
  late correction lands, the same applies.
- **The `simulated_now` resolution is part of the query.** You don't get
  to bake "September 2024 is the most recent complete month" into the
  surge-proxy view. The view (or cube, or key range) must be addressable
  by month, and the query layer picks the right one for the current
  `simulated_now`.

### Operational baseline

The system must come up with a single command on a fresh laptop —
`make run` in your project root — and survive a `docker compose restart`
of any one service without losing data, dropping a drop, or requiring a
human to babysit it back to a healthy state. The publisher persists its
simulated clock; your pipeline should persist whatever cursor / ETag
table / cube state it needs. Assume the grader will `docker compose
restart` something during your defence.

### Defence — what we'll do at your slot

Your slot is 10-15 minutes. We will:

1. Watch `make run` come up on the project as you hand it over (or on
   the state you've already left it in).
2. Verify `simulated_now` is advancing and that the publisher has
   produced at least the first one or two months' files in the bucket.
3. Ask you to run each of the five questions live and **measure the
   latency** (e.g. `\timing on` in psql, a small wrapper script that
   wraps the SQL in `time.perf_counter()`, a Spark stage UI, whatever's
   honest and visible). Each must return in under a second, query-only,
   warm process.
4. Run `make vendor-chaos` and wait for one or two correction rounds to
   land (you'll see them in `/stats` under `chaos_mutations`). Re-run
   the relevant question and confirm the corrected values appear —
   still under a second.
5. Ask you to walk us through the serving-layer design: why this storage
   choice, why those indexes / cubes / keys, what fails first under
   load, and how the late-correction path actually updates the
   pre-aggregated state.

Bring the queries written and the wrapper scripts (if any) prepared.
Bring a one-page diagram of your data flow so we can read it while you
talk. The grade is on the design and the latency, not on prose, and not
on a polished UI — there is no UI to build.
