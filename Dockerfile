FROM python:3.12-slim

# ── System deps + Node.js 20 ──────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        ca-certificates \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Node: install playwright npm package ─────────────────────────────────────
COPY package.json ./
RUN npm install

# ── Playwright: install Chromium + all system dependencies ───────────────────
RUN npx playwright install --with-deps chromium

# ── Python: install dependencies ──────────────────────────────────────────────
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ── App source ────────────────────────────────────────────────────────────────
COPY . .

# ── Runtime configuration ────────────────────────────────────────────────────
ENV DATA_DIR=/var/data
ENV PAGE_PAUSE=0.3
ENV PHASE0_PAUSE=1.0
ENV BATCH_SIZE=30
ENV PARALLEL_WORKERS=6
ENV START_YEAR=1940
ENV END_YEAR=2005
ENV TARGET_PEOPLE_DEFAULT=10400000
ENV HEARTBEAT_STALE_SECONDS=300
ENV AUTO_RESUME=true

RUN mkdir -p /var/data

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
