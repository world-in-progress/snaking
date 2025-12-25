FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

RUN uv init && uv add fastdb4py
FROM python:3.12-slim-bookworm

WORKDIR /app

COPY --from=builder /app/.venv /opt/venv

COPY preprocessor.py .

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

CMD ["python", "preprocessor.py"]
