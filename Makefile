# ─── Tessera capstone project ───────────────────────────────────────────────
#
# Run from the project root. The scaffold ships only the upstream:
#   * data-init      — one-shot sidecar that downloads four HV-FHV monthly
#                      parquet files + the taxi-zone lookup CSV into a named
#                      docker volume. ~5-10 min on first run (~2 GB total).
#                      Idempotent — exits in <2s after that.
#   * minio          — single-node S3 server. Bucket tlc-drops.
#   * tlc-publisher  — FastAPI service that PUTs the cached parquet files
#                      into the bucket as their simulated publication date
#                      arrives.
#
# Everything else (ingest, storage, materialised views, indexes, cubes) is
# yours to design. Add services to compose.yml as you need them.
# ────────────────────────────────────────────────────────────────────────────

.PHONY: run stop reset logs vendor-chaos vendor-calm help

help:
	@echo ""
	@echo "  make run            Build vendor image, run data-init, start MinIO + publisher"
	@echo "  make stop           Stop containers (keeps the tlc-cache + minio-data volumes)"
	@echo "  make reset          Stop + wipe volumes (next run re-downloads ~2 GB)"
	@echo "  make logs           Tail tlc-publisher logs"
	@echo "  make vendor-chaos   Restart tlc-publisher with late-correction chaos on"
	@echo "  make vendor-calm    Restart tlc-publisher with chaos disabled"
	@echo ""
	@echo "  MinIO S3 API:    http://localhost:18900"
	@echo "  MinIO console:   http://localhost:18901  (login: tessera / tessera-secret)"
	@echo "  Publisher API:   http://localhost:18910/docs"
	@echo "  Healthcheck:     http://localhost:18910/healthz"
	@echo ""

run:
	docker compose up -d --build
	@echo ""
	@echo "=============================================================="
	@echo " Tessera vendor mock is starting."
	@echo "   First run downloads ~2 GB (5-10 min). Watch progress:"
	@echo "     docker compose logs -f data-init"
	@echo "   Once tlc-publisher is healthy:"
	@echo "     curl http://localhost:18910/healthz"
	@echo "     open http://localhost:18901  (MinIO console)"
	@echo "=============================================================="

stop:
	docker compose down --remove-orphans

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f tlc-publisher

vendor-chaos:
	VENDOR_LATE_CORRECTION_RATE=0.5 \
	docker compose up -d --no-deps --force-recreate tlc-publisher
	@echo "[chaos] tlc-publisher restarted with late-correction rate 0.5."

vendor-calm:
	VENDOR_LATE_CORRECTION_RATE=0 \
	docker compose up -d --no-deps --force-recreate tlc-publisher
	@echo "[calm] tlc-publisher restarted with chaos disabled."
