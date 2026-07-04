# Tessera — low-latency NYC mobility analytics (capstone B)

Your job is to design the system. **This scaffold only ships the upstream
data provider and the dataset bootstrap.** Everything else — ingest, storage,
serving layer, materialised views, cubes, indexes — is yours.

**Start here:** [`BRIEF.md`](./BRIEF.md).

## Bring it up

```bash
make run
```

The first run downloads ~2 GB of NYC TLC HV-FHV monthly parquet (four months,
2024-09 .. 2024-12) plus the taxi-zone lookup CSV from CloudFront into a
docker volume (`tlc-cache`). Expect **~5-10 min** end to end on a decent
connection. Watch progress:

```bash
docker compose logs -f data-init
```

Subsequent runs reuse the volume and start in seconds. `make reset` wipes
both volumes and forces a re-download.

### Host-cache passthrough (skip the re-download)

If you `make reset` often (e.g. while iterating on the publisher or trying
chaos rolls from a clean slate), set `HVFHV_CACHE_DIR` in `.env` to a host
directory that holds the parquet files and zone-lookup CSV. `data-init`
copies from there instead of re-downloading ~2 GB:

```bash
mkdir -p ~/tlc-cache
# After the first successful run, populate it from the docker volume:
docker run --rm -v tessera_tlc-cache:/src -v ~/tlc-cache:/dst alpine \
  sh -c "cp /src/fhvhv_tripdata_*.parquet /src/taxi_zone_lookup.csv /dst/"
# Then in .env:
#   HVFHV_CACHE_DIR=/Users/you/tlc-cache
```

Filenames must match exactly (`fhvhv_tripdata_YYYY-MM.parquet`,
`taxi_zone_lookup.csv`). Files of insufficient size are ignored and
re-downloaded.

## What's running

| Service         | Purpose                                                      | URL                                |
|-----------------|--------------------------------------------------------------|------------------------------------|
| `data-init`     | One-shot: download HV-FHV parquet + zone-lookup CSV.         | (no port; exits when done)         |
| `minio`         | S3-compatible object store. Bucket `tlc-drops`.              | http://localhost:18900 (API), http://localhost:18901 (console) |
| `tlc-publisher` | FastAPI service that PUTs each month's parquet into MinIO    | http://localhost:18910             |
|                 | as its simulated publication date arrives.                   |                                    |

MinIO root credentials (used as S3 access-key / secret-key): `tessera` /
`tessera-secret`. Override in `.env` if you need to.

Useful endpoints on `tlc-publisher`:

- `GET /healthz` — `{ready, simulated_now, published_months}`
- `GET /simulated_now` — current simulated wall-clock (UTC)
- `GET /stats` — PUT counters, chaos round counters, mutation log
- `GET /docs` — OpenAPI / Swagger UI

### Sample calls

```bash
# Publisher health
curl -s http://localhost:18910/healthz | jq

# Current simulated time (resolves "most recent complete month" etc.)
curl -s http://localhost:18910/simulated_now | jq
```

## The S3 contract — what you integrate against

The publisher writes to MinIO at `s3://tlc-drops/`. **Discovery is `LIST`.**
There is no manifest endpoint. The idempotency key for each file is
`(filename, etag)` — if a file's ETag has changed since you last processed
it, the bytes are different and you must reprocess.

### Boto3 from the host

```python
import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:18900",
    aws_access_key_id="tessera",
    aws_secret_access_key="tessera-secret",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# List drops
resp = s3.list_objects_v2(Bucket="tlc-drops")
for obj in resp.get("Contents", []):
    print(obj["Key"], obj["ETag"], obj["Size"])

# Download one file
s3.download_file("tlc-drops", "fhvhv_tripdata_2024-09.parquet", "/tmp/sep.parquet")
```

### Boto3 from inside a compose service

Use the in-network endpoint `http://minio:9000` (not `localhost`):

```python
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="tessera",
    aws_secret_access_key="tessera-secret",
    ...
)
```

### PySpark `s3a://` from the host

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("tessera")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:18900")
    .config("spark.hadoop.fs.s3a.access.key", "tessera")
    .config("spark.hadoop.fs.s3a.secret.key", "tessera-secret")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.read.parquet("s3a://tlc-drops/fhvhv_tripdata_2024-09.parquet")
df.printSchema()
```

(From a compose service swap `http://localhost:18900` → `http://minio:9000`.)

## Chaos toggle

The publisher simulates the one failure mode named in `BRIEF.md`:
**late corrections** — a previously-published file is re-PUT with a small
mutation and a new ETag.

```bash
make vendor-chaos   # late-correction rate 0.5 (one-in-two new-month rolls)
make vendor-calm    # off
```

When a correction fires, the publisher's `/stats` endpoint records the
month, row count, and new ETag under `chaos_mutations`. Your pipeline must
detect the new ETag and reflect the corrected rows in your serving layer.

## Replay speed

Default `REPLAY_SECONDS_PER_MONTH=75` means one simulated month per 75
wall-clock seconds — so the four-month window drips into the bucket over
~5 wall-minutes total. Raise the value in `.env` (e.g. `300`) to slow it
down for debugging; lower it for a faster sanity loop. Restart
`tlc-publisher` after editing `.env`:

```bash
docker compose restart tlc-publisher
```

The publisher persists its simulated clock and the list of already-published
months to `_publisher_state.json` inside the `tlc-cache` volume, so a
restart resumes where it left off — `simulated_now` does not snap back to
the window start.

## Adding your own services

The scaffold ships a thin `compose.yml`. Add services as you need them —
copy snippets from the course's `labs/streaming/compose.yml`,
`labs/spark/compose.yml`, `labs/monitoring/compose.yml`, etc. There's no
opinionated module layout under `src/` — design as a team and make your
choices defensible.

## Editing in VS Code

The course already has a remote-development pattern (see
`scaffolds/streaming-clickstream/README.md`). Add your own
`.devcontainer/devcontainer.json` pointing at whichever container you spin
up for development.

## Data notes (read me before defending)

- **HV-FHV schema.** The HV-FHV parquet files share a stable schema across
  the chosen window: `hvfhs_license_num` (HV0002 Juno [Lyft], HV0003 Uber,
  HV0004 Via, HV0005 Lyft), `dispatching_base_num`, `originating_base_num`,
  `request_datetime`, `on_scene_datetime`, `pickup_datetime`,
  `dropoff_datetime`, `PULocationID`, `DOLocationID`, `trip_miles`,
  `trip_time`, `base_passenger_fare`, `tolls`, `bcf`, `sales_tax`,
  `congestion_surcharge`, `airport_fee`, `tips`, `driver_pay`,
  `shared_request_flag`, `shared_match_flag`, `access_a_ride_flag`,
  `wav_request_flag`, `wav_match_flag`. (Names verified live against
  CloudFront on the 2024 monthly drops.)
- **Zone lookup.** `taxi_zone_lookup.csv` is a small file
  (`LocationID,Borough,Zone,service_zone`) that you'll want to join against
  the `PULocationID` / `DOLocationID` columns for borough/zone names.
- **Late corrections affect `base_passenger_fare`.** The chaos mutation
  changes a small number of rows' `base_passenger_fare` by a deterministic
  seeded RNG. The new file is genuinely different bytes (and ETag).
- **Window.** Four months, 2024-09 .. 2024-12. ~80 M rows total.
