FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/kestra-io/plugin-dlt"
LABEL org.opencontainers.image.description="dlt runtime"

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gcc build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir "dlt[duckdb]" requests

ENTRYPOINT ["/bin/sh","-c"]
CMD ["python --version"]
