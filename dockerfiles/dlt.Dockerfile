FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/kestra-io/plugin-dlt"
LABEL org.opencontainers.image.description="dlt runtime"

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GIT_PYTHON_REFRESH=quiet

# Install system deps (includes git)
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
      git ca-certificates curl gcc build-essential \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip setuptools wheel uv && uv pip install --system "dlt[duckdb,cli]" \
    requests \
    pandas \
    pyarrow \
    sqlalchemy \
    fastparquet

# Unprivileged user (UID/GID 1000) that task code runs as, with a writable HOME (dlt stores state in ~/.dlt).
# site-packages and bin are owned by it so `pip install` / `uv pip install --system` keep working in beforeCommands.
RUN groupadd --gid 1000 kestra \
 && useradd --uid 1000 --gid kestra --create-home --home-dir /home/kestra --shell /bin/sh kestra \
 && chown -R kestra:kestra /usr/local/lib/python3.11/site-packages /usr/local/bin

# The container starts as root on purpose: Kestra's Docker task runner creates the task working directory as root,
# so the entrypoint chowns it and then drops to `kestra` before running the task command.
COPY --chmod=755 dockerfiles/dlt-entrypoint.sh /usr/local/bin/kestra-entrypoint.sh

# nosemgrep: dockerfile.security.missing-user-entrypoint.missing-user-entrypoint
ENTRYPOINT ["/bin/sh","-c"]
# nosemgrep: dockerfile.security.missing-user.missing-user
CMD ["python --version"]
