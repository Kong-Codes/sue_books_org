  # ---------- API builder ----------
  FROM python:3.13-slim-bookworm AS api-builder
  ENV UV_LINK_MODE=copy \
      PIP_DISABLE_PIP_VERSION_CHECK=1 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONUNBUFFERED=1
  RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libpq-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
  # install uv
  RUN curl -LsSf https://astral.sh/uv/install.sh | sh
  ENV PATH="/root/.local/bin:${PATH}"
  WORKDIR /app
  # deps first for cache
  COPY pyproject.toml uv.lock ./
  RUN uv sync --frozen --no-dev
  # app code
  COPY . .
  RUN uv sync --frozen --no-dev
  
  # ---------- API runtime ----------
  FROM python:3.13-slim-bookworm AS api-runtime
  ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONUNBUFFERED=1 \
      PORT=8000
  RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
  WORKDIR /app
  COPY --from=api-builder /app /app
  ENV PATH="/app/.venv/bin:${PATH}"
  EXPOSE 8000
  # Uncomment if you want the image to be runnable standalone:
  # CMD ["uvicorn","endpoints:app","--host","0.0.0.0","--port","8000"]
  