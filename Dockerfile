FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# зависимости
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# код бэкенда
COPY . /app

# дефолтный конфиг + папки
COPY docker/default_settings.json /app/docker/default_settings.json
RUN mkdir -p /app/data /app/config /app/logs

EXPOSE 8000

# HEALTHCHECK: /healthz
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 \
  CMD curl -fsS http://localhost:8000/healthz || exit 1

# entrypoint
COPY docker/entrypoint.sh /app/docker/entrypoint.sh
RUN chmod +x /app/docker/entrypoint.sh
ENTRYPOINT ["/app/docker/entrypoint.sh"]
