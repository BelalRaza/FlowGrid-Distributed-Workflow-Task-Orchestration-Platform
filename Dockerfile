FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /code

# Install dependencies first so this layer is cached when only source changes.
COPY pyproject.toml README.md ./
COPY app/__init__.py app/__init__.py
RUN pip install ".[dev]"

COPY . .
RUN pip install --no-deps -e .

# Run as an unprivileged user.
RUN useradd --create-home --uid 1000 flowgrid && chown -R flowgrid /code
USER flowgrid

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
