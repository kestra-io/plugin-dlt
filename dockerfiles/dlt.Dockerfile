FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/kestra-io/plugin-dlt"
LABEL org.opencontainers.image.description="dlt runtime"

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GIT_PYTHON_REFRESH=quiet

# Install system deps (includes git)
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl gcc build-essential \
 && rm -rf /var/lib/apt/lists/*

RUN pip install uv && uv pip install --system "dlt[duckdb,cli,workspace]==1.16.0" websockets>=14.2.0 requests


ENTRYPOINT ["/bin/sh","-c"]
CMD ["python --version"]
