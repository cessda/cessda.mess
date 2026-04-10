# MESS — Metadata Enrichment Semantic Service

Enriches research dataset metadata by discovering related digital objects (publications, software, datasets) via Scholexplorer, OpenAIRE, and OpenAlex. Caches results in PostgreSQL, serves SKG-IF JSON-LD, and materializes a SPARQL-queryable knowledge graph.

---

## Quick start and test

### 1. Configure and start

```bash
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD and MESS_ADMIN_KEY at minimum
bash scripts/start.sh
```

Wait for the API to become healthy:

```bash
docker compose ps          # mess-api should show "(healthy)"
curl http://localhost:8000/health
# {"status": "ok", "postgres": "ok", "sparql": "ok"}
```

> `sparql` will show `"error"` until you build the QLever index (step 3).

### 2. Enrich datasets — web UI

Open **http://localhost:8000** in your browser. Paste each DOI into the search box and press Enter:

| DOI | Dataset |
|-----|---------|
| `10.4232/1.13571` | ALLBUS 2018 (GESIS) |
| `10.4232/1.11449` | ALLBUS 2014 (GESIS) |
| `10.5255/ukda-sn-7480-1` | UK Household Longitudinal Study (UK Data Service) |
| `10.17026/dans-xby-5dhs` | DANS dataset (DANS-KNAW) |

Each enrichment fetches related publications, datasets, and software from Scholexplorer, OpenAIRE, and OpenAlex, then returns the result as SKG-IF JSON-LD.

To browse all cached objects: **http://localhost:8000/browse.html**

### 3. Build the QLever knowledge graph

```bash
bash scripts/rebuild_qlever.sh
```

Once done, `curl http://localhost:8000/health` should return `"sparql": "ok"`.

### 4. Explore — web UI

- **Graph explorer**: http://localhost:8000/graph.html — visual link graph for any enriched DOI
- **SPARQL interface**: http://localhost:8000/sparql.html — run queries against the knowledge graph, with built-in example queries

---

## Container management

The core stack (PostgreSQL + API) and the QLever SPARQL store are managed **separately**. QLever requires a pre-built index and cannot start without one — always run `rebuild_qlever.sh` before expecting SPARQL to work.

### Core stack (PostgreSQL + API)

```bash
# Start (preserves data)
bash scripts/start.sh

# Stop (preserves data)
bash scripts/stop.sh

# Also stop QLever at the same time
bash scripts/stop.sh --qlever

# Restart
bash scripts/restart.sh

# Also restart QLever (does NOT rebuild the index)
bash scripts/restart.sh --qlever

# Wipe ALL data and start fresh — PostgreSQL, export volume, and QLever index
bash scripts/clean.sh
```

### QLever (SPARQL store)

QLever must be started separately and requires an index to exist first. The index must be rebuilt whenever enriched data changes.

```bash
# Build index and start QLever — run this on first setup, after clean.sh, or after new enrichments
bash scripts/rebuild_qlever.sh
```

**First-time setup order:**
1. `bash scripts/start.sh` — start PostgreSQL + API
2. Enrich at least one dataset via `GET /enrich?pid=...`
3. `bash scripts/rebuild_qlever.sh` — export triples, build index, start QLever

---

## Logs

```bash
# Follow all containers
docker-compose logs -f

# Follow API only (most useful)
docker-compose logs -f mess-api

# Last 100 lines + follow
docker-compose logs -f --tail=100 mess-api

# Human-readable (requires jq)
docker-compose logs -f mess-api | jq -r '"\(.asctime) \(.levelname) \(.name): \(.message)"'
```

Set `LOG_LEVEL=DEBUG` in `.env` and restart for verbose output.

---

## API — curl examples

### Health check
```bash
curl http://localhost:8000/health
```
```json
{"status": "ok", "postgres": "ok", "sparql": "ok"}
```

### Enrich a dataset by PID
```bash
curl "http://localhost:8000/enrich?pid=10.1234/my-dataset"
```
Returns SKG-IF JSON-LD with the source dataset and all related digital objects.

Supported PID formats: DOI (`10.xxx/...`), Handle (`NNN/...`), URN:NBN (`urn:nbn:xx:...`), ARK (`ark:/NNNNN/...`)

### Cache status (no enrichment triggered)
```bash
curl "http://localhost:8000/status?pid=10.1234/my-dataset"
```
```json
{"pid": "10.1234/my-dataset", "found": true, "fresh": true, "last_checked": "2026-03-23T10:00:00Z"}
```

### Admin stats
```bash
curl http://localhost:8000/admin/stats \
  -H "X-Admin-Key: your-admin-key"
```

### Trigger ETL (PostgreSQL → SPARQL store)
```bash
curl -X POST http://localhost:8000/admin/etl \
  -H "X-Admin-Key: your-admin-key"
```
Returns `202 Accepted` immediately; ETL runs in the background.

Or use the script:
```bash
MESS_ADMIN_KEY=your-admin-key ./scripts/etl.sh
```

---

## Knowledge graph (QLever)

QLever requires a pre-built index. Rebuild it whenever you want the SPARQL store to reflect the latest enriched data:

```bash
# Full rebuild in one command (stops QLever, exports triples, builds index, restarts QLever)
bash scripts/rebuild_qlever.sh
```

Or run the steps manually:

```bash
# 1. Export triples from PostgreSQL to a shared volume
docker compose run --rm mess-api python scripts/export_triples.py

# 2. Build the QLever index
docker compose -f docker-compose.qlever.yml run --rm indexer

# 3. Start (or restart) the QLever server
docker compose -f docker-compose.qlever.yml up -d qlever
```

> The main `docker-compose.yml` does **not** start QLever — it must be managed separately via `docker-compose.qlever.yml`.

---

## SPARQL queries

The SPARQL endpoint is available at two levels:

- **Via MESS API proxy** (rate-limited, logged): `http://localhost:8000/sparql?query=...`
- **Directly on QLever**: `http://localhost:7001/?query=...`

### All objects in the graph
```bash
curl -G "http://localhost:8000/sparql" \
  --data-urlencode "query=SELECT * WHERE { ?s ?p ?o } LIMIT 10"
```

### All datasets
```bash
curl -G "http://localhost:8000/sparql" \
  --data-urlencode "query=
    PREFIX skg: <https://w3id.org/skg-if/ontology/>
    SELECT ?dataset ?title WHERE {
      ?dataset a skg:Dataset ;
               <http://purl.org/dc/terms/title> ?title .
    } LIMIT 20"
```

### All objects citing a dataset
```bash
curl -G "http://localhost:8000/sparql" \
  --data-urlencode "query=
    PREFIX skg: <https://w3id.org/skg-if/ontology/>
    SELECT ?pub ?title WHERE {
      <https://doi.org/10.1234/my-dataset> skg:cites ?pub .
      OPTIONAL { ?pub <http://purl.org/dc/terms/title> ?title }
    }"
```

### Citation counts for all objects
```bash
curl -G "http://localhost:8000/sparql" \
  --data-urlencode "query=
    PREFIX skg: <https://w3id.org/skg-if/ontology/>
    SELECT ?obj ?citations WHERE {
      ?obj skg:citationCount ?citations .
    } ORDER BY DESC(?citations) LIMIT 20"
```

### All relationships by type
```bash
curl -G "http://localhost:8000/sparql" \
  --data-urlencode "query=
    SELECT ?type (COUNT(*) AS ?count) WHERE {
      ?s ?type ?o .
      FILTER(STRSTARTS(STR(?type), 'https://w3id.org/skg-if/ontology/'))
    } GROUP BY ?type ORDER BY DESC(?count)"
```

---

## Running tests

```bash
# Install dependencies (requires Docker for PostgreSQL test container)
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"

# Run full test suite
uv run pytest

# Unit tests only (no Docker needed)
uv run pytest tests/unit/

# With verbose output
uv run pytest -v --tb=short
```

---

## Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MESS_SERVICE_NAME` | Display name | `CESSDA MESS` |
| `MESS_SOURCE_ENDPOINT` | SKG-IF products API | CESSDA staging |
| `MESS_SOURCE_PID_FILTER` | Query parameter pattern | `filter=identifiers.id:{pid}` |
| `MESS_CACHE_TTL_HOURS` | Cache freshness window | `48` |
| `SCHOLEXPLORER_API_URL` | Scholexplorer v3 endpoint | _(default set)_ |
| `OPENAIRE_API_URL` | OpenAIRE Graph API base | _(default set)_ |
| `OPENALEX_API_URL` | OpenAlex API base | _(default set)_ |
| `OPENAIRE_ACCESS_TOKEN` | OAuth 2.0 token | _(empty = 60 req/hr)_ |
| `OPENALEX_API_KEY` | OpenAlex API key | _(empty = 100 credits/day)_ |
| `POSTGRES_PASSWORD` | **Required** | — |
| `SPARQL_STORE_URL` | SPARQL store URL | `http://sparql:7080` |
| `MESS_ADMIN_KEY` | **Required** for admin endpoints | — |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING` | `INFO` |

See `.env.example` for all variables with placeholders.
