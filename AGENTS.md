# Repository Guidelines

## Project Structure & Module Organization
Core research logic lives in phase-oriented Python packages: `data/` for ingestion and cleaning, `indicators/` for technical studies, `scanners/` for VCP and sniper screens, `sectors/` for sector intelligence, `models/` and `features/` for ranking logic, and `pipelines/` for orchestrating end-to-end runs. The FastAPI dashboard lives under `web/`, with routes in `web/routes/`, service code in `web/services/`, templates in `web/templates/`, and static assets in `web/static/`. Tests are grouped by delivery phase in `tests/phase1`, `tests/phase2`, and `tests/phase3`. Treat `output/` and generated artifacts in `web/static/` as derived data, not hand-edited source.

## Build, Test, and Development Commands
Install dependencies with `pip install -r requirements.txt`. Run the full research pipeline from the repo root with `python pipelines/run_pipeline.py`; this executes ingestion, indicators, scanners, sector scoring, fusion, and Monte Carlo generation in order. Start the web app locally with `uvicorn web.app:app --reload`. Run the full test suite with `pytest tests -q`, or scope by phase, for example `pytest tests/phase2 -q`.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, `snake_case` for modules, functions, and variables, and small focused modules named for the domain they implement, such as `sector_rotation.py` or `stock_filtering.py`. Keep imports explicit and keep pipeline entrypoints in `pipelines/`. There is no committed formatter or linter config in this repo, so default to PEP 8-compatible formatting and keep docstrings/comments brief and functional.

## Testing Guidelines
Tests use `pytest` and are named `test_*.py`. Add new tests to the matching phase directory for the logic you change. Prefer deterministic unit tests with small `pandas.Series` or frame fixtures over network-bound tests. For pipeline or scanner changes, add both a success-path test and at least one guard-rail failure case.

## Commit & Pull Request Guidelines
Recent commits use short, plain-language subjects such as `updated sector rotation` and `updating css file`. Keep commit titles concise, scoped to one change, and written in the imperative or simple past tense. Pull requests should describe the affected phase or web surface, summarize test coverage, link the relevant issue if one exists, and include screenshots for template or CSS changes.

## Security & Configuration Tips
Secrets are environment-driven. Keep `MONGODB_URI`, `SESSION_SECRET`, and any Cloudinary credentials in local environment variables or a non-committed `.env` file. Do not commit credentials, downloaded raw market data, or cache artifacts such as `__pycache__`.
