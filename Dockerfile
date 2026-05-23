# Reproducible container for the topVenues artifact.
#
# Build:
#   docker build -t topvenues .
#
# Run the web interface (default):
#   docker run --rm -p 8501:8501 topvenues
#   → open http://localhost:8501
#
# Run any CLI command:
#   docker run --rm topvenues python -m src.cli stats
#   docker run --rm topvenues python -m src.cli search --title "intrusion"
#
# Run the test suite:
#   docker run --rm topvenues python -m pytest -q

FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first so changes to source do not invalidate the layer.
COPY requirements.txt requirements-web.txt ./
RUN pip install -r requirements.txt -r requirements-web.txt

# Copy only what the artifact needs at runtime.
COPY src/ ./src/
COPY web/ ./web/
COPY tests/ ./tests/
COPY config.yaml README.md LICENSE ./
COPY data/dataset/papers.db.gz ./data/dataset/
COPY reproduce.sh ./

EXPOSE 8501

# Healthcheck: the Streamlit endpoint must return 200 on its homepage.
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health').read()" || exit 1

CMD ["streamlit", "run", "web/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true", "--browser.gatherUsageStats=false"]
