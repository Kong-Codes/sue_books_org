FROM python:3.13-slim AS builder

ENV UV_LINK_MODE=copy \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Install uv (no flags needed)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copy dependency manifests first for better layer caching
COPY pyproject.toml uv.lock ./

# Create virtualenv and install deps with uv
RUN uv sync --frozen --no-dev

# Copy the application code
COPY . .

# Re-run sync in case of optional extras or local modules
RUN uv sync --frozen --no-dev


FROM python:3.13-slim AS runtime

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

# Runtime libs for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Bring in the synced venv and app
COPY --from=builder /app /app

# Activate project venv on PATH
ENV PATH="/app/.venv/bin:${PATH}"

EXPOSE 8000

# Endpoints app entrypoint
# CMD ["uvicorn", "endpoints:get_app", "--host", "0.0.0.0", "--port", "8000"]


