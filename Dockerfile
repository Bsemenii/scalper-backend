FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# Убедись, что SQLAlchemy там есть
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Дефолтный конфиг + папки
COPY docker/default_settings.json /app/docker/default_settings.json
RUN mkdir -p /app/data /app/config /app/logs

# HEALTHCHECK: проверяет /healthz
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 \
  CMD curl -fsS http://localhost:8010/healthz || exit 1

# ENTRYPOINT — наш bootstrap
COPY docker/entrypoint.sh /app/docker/entrypoint.sh
ENTRYPOINT ["/app/docker/entrypoint.sh"]
