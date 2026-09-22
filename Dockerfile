# Reproducible container for the TopVenues artifact.
#
# Build:
#   docker build -t topvenues .
#
# Run the web interface (default):
#   docker run --rm -p 8501:8501 topvenues
#   → open http://localhost:8501
#
# Run any CLI command:
#   docker run --rm topvenues python -m src.cli --profile security-20-v4 stats
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

# The same pinned, hash-checked set the reproduction script installs. Installing
# the open ranges from requirements.txt here gave the container different
# versions from the ones a reviewer running reproduce.sh receives.
COPY requirements-frozen.txt ./
RUN pip install --require-hashes -r requirements-frozen.txt

# Copy only what the artifact needs at runtime.
COPY src/ ./src/
COPY web/ ./web/
COPY tests/ ./tests/
COPY scripts/ ./scripts/
COPY profiles/ ./profiles/
COPY config.yaml README.md README.pt-BR.md LICENSE pyproject.toml uv.lock ./
COPY requirements.txt requirements-web.txt requirements-frozen.txt ./
COPY data/profiles/ ./data/profiles/
COPY data/adjudication/ ./data/adjudication/
# The Evidence page renders the manual-audit summary and the v3-to-v4 transfer.
COPY evaluation/ ./evaluation/
# The Hugging Face export copies these into the dataset card it builds.
COPY docs/assets/topvenues-abstract-search.png docs/assets/topvenues-abstract-search.pdf ./docs/assets/
# The interface draws its logo and favicon from the brand masters.
COPY docs/brand/ ./docs/brand/
# Theme and font configuration; without it the image ran on Streamlit defaults.
COPY .streamlit/ ./.streamlit/
COPY data/awards/ ./data/awards/
COPY reproduce.sh ./

EXPOSE 8501

# Healthcheck: the Streamlit endpoint must return 200 on its homepage.
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health').read()" || exit 1

CMD ["python", "-m", "streamlit", "run", "web/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true", "--browser.gatherUsageStats=false"]
