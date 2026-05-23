.PHONY: help install install-web test stats web reproduce docker docker-test snapshot clean

help:
	@echo "Targets:"
	@echo "  install     create venv and install dependencies"
	@echo "  install-web install optional Streamlit dependencies"
	@echo "  test        run pytest"
	@echo "  stats       print corpus statistics"
	@echo "  web         launch the Streamlit interface on :8501"
	@echo "  reproduce   verify every headline claim end to end"
	@echo "  docker      build the Docker image"
	@echo "  docker-test run pytest inside Docker"
	@echo "  snapshot    rewrite data/dataset/papers.db.gz from papers.db"
	@echo "  clean       remove caches and the materialised papers.db"

install:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

install-web: install
	. .venv/bin/activate && pip install -r requirements-web.txt

test:
	python -m pytest -q

stats:
	python -m src.cli stats

web:
	python -m streamlit run web/app.py

reproduce:
	bash reproduce.sh

docker:
	docker build -t topvenues .

docker-test:
	docker run --rm topvenues python -m pytest -q

snapshot:
	python -m src.cli write-snapshot

clean:
	rm -rf .pytest_cache __pycache__ */__pycache__ */*/__pycache__
	rm -f data/dataset/papers.db data/dataset/papers.db.sync-id
