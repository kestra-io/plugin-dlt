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

# Run as a non-root user (fixed UID/GID 1000) with a writable HOME (dlt stores state in ~/.dlt).
# site-packages and bin are owned by this user so `pip install` / `uv pip install --system`
# in beforeCommands keep working at runtime.
RUN groupadd --gid 1000 kestra \
 && useradd --uid 1000 --gid kestra --create-home --home-dir /home/kestra --shell /bin/sh kestra \
 && chown -R kestra:kestra /usr/local/lib/python3.11/site-packages /usr/local/bin

USER kestra
ENV HOME=/home/kestra
WORKDIR /home/kestra

RUN pip install uv && uv pip install --system "dlt[duckdb,cli]" \
    requests \
    pandas \
    pyarrow \
    sqlalchemy \
    fastparquet

ENTRYPOINT ["/bin/sh","-c"]
CMD ["python --version"]
