FROM python:3.12.8-slim

WORKDIR /app

# Install system deps (psycopg2 build deps for Alembic sync driver)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.5.14 /uv /usr/local/bin/uv

# Copy application source
COPY . .

# Install all dependencies including the project
RUN uv pip install --system ".[dev]"

# Non-root user for security
RUN useradd -m appuser && chown -R appuser:appuser /app

# Create the export directory owned by appuser so the Docker volume is initialised
# with the right permissions when first mounted (Docker copies dir perms on first use).
RUN mkdir /export && chown appuser:appuser /export

USER appuser

EXPOSE 8000

# Run migrations then start server
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
