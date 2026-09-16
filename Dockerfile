FROM python:3.13-slim

WORKDIR /app

# Non-root by default. These services read session tokens and write to Postgres
# and MinIO; there is no reason for any of them to run as root, and postmark's
# parse-worker already sets the precedent.
RUN useradd -m appuser

# git is needed because cortex-utils is a git dependency pinned to a SHA.
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install uv

COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

RUN uv sync --frozen --no-dev

RUN chown -R appuser:appuser /app
USER appuser

# No VOLUME for secrets. All three services share this image and only two of
# them mount /app/secrets -- a VOLUME here would make Docker create an
# anonymous, root-owned volume in the third instead, leaking a fresh one on
# every recreate (`docker compose down` does not remove anonymous volumes
# without -v). Compose declares the real mount where it is needed.

# Overridden per service in docker-compose. The poller is the default because it
# is the one that must always be running: the parse and media workers can catch
# up from the archive, but a gap in the poll is a gap in the record.
CMD ["uv", "run", "python", "-m", "cortex_slack.services.run_poller"]
