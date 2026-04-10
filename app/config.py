"""Central configuration — all settings come from environment variables (12-factor).

Pydantic-settings reads variables from the process environment and, optionally, a `.env`
file.  A singleton `settings` object is imported throughout the app; no config values
should be hardcoded anywhere else.

Env-var naming convention: the field name is used verbatim as the env var name (upper-cased
by pydantic-settings).  E.g. `sparql_query_url` → env var `SPARQL_QUERY_URL`.

See `.env.example` for all variables with placeholder values.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Required at startup (no usable default):
      - POSTGRES_PASSWORD
      - MESS_ADMIN_KEY

    Everything else has a working default suitable for local Docker Compose development.
    For production, review every field — especially API keys and base URLs.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Service identity ────────────────────────────────────────────────────────
    mess_service_name: str = "CESSDA MESS"
    mess_service_base_url: str = "https://mess.cessda.eu"

    # ── Source SKG-IF endpoint ─────────────────────────────────────────────────
    # The upstream service that provides research product metadata as SKG-IF JSON-LD.
    # Any SKG-IF-compliant endpoint can be plugged in here.
    mess_source_endpoint: str = "https://skg-if-staging.cessda.eu/api/products"
    # Query-string pattern used to look up a product by PID; `{pid}` is substituted.
    mess_source_pid_filter: str = "filter=identifiers.id:{pid}"

    # ── External enrichment APIs ───────────────────────────────────────────────
    # Scholexplorer v3 — discovers dataset↔publication links (Scholix 3.0 format).
    scholexplorer_api_url: str = "https://api-beta.scholexplorer.openaire.eu/v3/Links"
    # OpenAIRE Graph API — adds funder/project metadata and access-rights info.
    openaire_api_url: str = "https://api.openaire.eu/graph/v2"
    # OpenAlex — adds citation counts, FWCI, topics, and open-access URLs.
    openalex_api_url: str = "https://api.openalex.org"
    # OAuth 2.0 bearer token for OpenAIRE (empty = anonymous, limited to 60 req/hr).
    openaire_access_token: str = ""
    # OpenAlex API key (empty = polite pool, limited to ~100 credits/day per IP).
    openalex_api_key: str = ""

    # ── Cache freshness ────────────────────────────────────────────────────────
    # Objects whose `last_checked` timestamp is within this window are served from
    # the PostgreSQL cache without triggering new external API calls.
    mess_cache_ttl_hours: int = 48

    # ── PostgreSQL connection ──────────────────────────────────────────────────
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "mess"
    postgres_user: str = "mess"
    postgres_password: str = ""  # REQUIRED — set via env var, never hardcode

    # ── SPARQL triple store (QLever) ───────────────────────────────────────────
    # Path where scripts/export_triples.py writes the N-Triples file.
    # Must match the mount point of the shared Docker volume in docker-compose.yml.
    sparql_export_path: str = "/export/graph.nt"
    # Base URL of the QLever SPARQL server.  Optional — only needed when QLever is running.
    # Used by the /sparql proxy endpoint and the /health SPARQL check.
    # May be unreachable when QLever has not yet been started or the index not yet built.
    sparql_query_url: str = "http://qlever:7001"

    # ── Admin API ─────────────────────────────────────────────────────────────
    # Sent as the `X-Admin-Key` header to reach /admin/* endpoints.
    # REQUIRED in production — set a long random string.
    mess_admin_key: str = ""

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"  # DEBUG | INFO | WARNING | ERROR

    # ── Computed DB URLs ──────────────────────────────────────────────────────

    @property
    def database_url(self) -> str:
        """asyncpg DSN used by SQLAlchemy's async engine (FastAPI runtime)."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def database_url_sync(self) -> str:
        """psycopg2 DSN used by Alembic migrations (synchronous, CLI only)."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
