# AGENTS.md

Use this guide when making changes in this repo.

## Repository snapshot
- Python 3.12 project with dependencies in `pyproject.toml`.
- `uv.lock` is present; prefer uv for dependency management.
- Source code lives under `src/` (Spark ETL jobs + utils).
- Bootstrap scripts under `scripts/bootstrap/`.
- Notebooks under `notebooks/` for exploration.

## Setup
- Create/activate a virtualenv (repo uses `.venv/`).
- Prefer `uv sync` to install deps from `pyproject.toml`/`uv.lock`.
- If uv is unavailable, use `python -m venv .venv` then `pip install -e .`.
- Keep `uv.lock` updated if dependencies change.

## Build
- No build step is configured; this is a pure Python/Spark repo.
- Spark jobs are run as scripts (see examples below).

## Lint & format
- Lint: `ruff check .`
- Format: `ruff format .`
- VS Code uses Ruff as the default formatter (see `.vscode/settings.json`).

## Tests
- No test framework is configured yet.
- If pytest is added, run all tests with `python -m pytest`.
- Single test pattern: `python -m pytest path/to/test_file.py::test_name`.

## Common run commands
- Bootstrap NYC TLC parquet data: `python scripts/bootstrap/initial_load.py`.
- Spark ETL job (yellow trips staging): `python src/etl/jobs/nyc_tlc_stg_yellow_tripdata.py`.
- S3 connectivity check (uses `.env`): `python test.py`.

## Cursor/Copilot rules
- No `.cursor/rules`, `.cursorrules`, or `.github/copilot-instructions.md` found.

## Code style (Python)
- Use 4-space indentation; keep lines readable (Ruff format default is 88).
- Prefer explicit imports; avoid wildcard imports.
- Group imports as: standard library, third-party, local modules.
- Keep top-level imports minimal in Spark jobs to reduce executor overhead.
- Use `functions as f` alias for `pyspark.sql.functions` (existing pattern).
- Prefer f-strings for formatting.
- Keep functions small and single-purpose; factor reusable helpers into `src/utils/`.

## Types
- Use type hints for public functions.
- Use `typing.cast` for Spark JVM bridges when needed.
- Prefer `Optional[...]` or `| None` for nullable values.
- Avoid `Any` unless interacting with Spark JVM or external dynamic APIs.

## Naming
- Modules/files: `snake_case.py`.
- Functions/variables: `snake_case`.
- Classes: `PascalCase`.
- Constants: `UPPER_SNAKE_CASE`.
- Spark columns: lower_snake_case; maintain consistency with rename maps.

## Formatting & organization
- Keep Spark config grouped near session creation.
- Keep transformation chains readable; break long chains into blocks.
- Prefer `.withColumn` chains for derived columns; avoid side effects.
- Use `repartition`/`partitionBy` consciously; document why if non-obvious.

## Error handling
- Use `response.raise_for_status()` for HTTP requests.
- Handle known error conditions explicitly (e.g., 403 in TLC data).
- Avoid bare `except`; always catch specific exceptions.
- Provide context in error messages (year/month, dataset, etc.).

## Logging
- Use Spark/log4j for job logs where possible.
- Keep logging noisy but informative for data pipelines (progress, counts).
- Avoid printing large dataframes; prefer schema/summary info.

## Data & storage
- Data lands under `data/` for local bootstrap; it is gitignored.
- S3 paths use `s3a://`; keep bucket/path config out of code if possible.
- Avoid collecting large datasets to the driver.
- Cast types deliberately (see money/decimal columns pattern).
- Prefer deterministic partitioning columns (year/month) for lakehouse paths.

## Environment & secrets
- Do not commit `.env`, credentials, or secrets.
- `.env` is used by `test.py` via `python-dotenv`.
- Prefer environment variables for AWS endpoints/keys and bucket names.

## Spark conventions
- Do not hardcode the Spark master in code; rely on `spark-submit`.
- Stop Spark sessions explicitly (`spark.stop()`).
- Enable adaptive execution only where needed (currently set in job).
- Use `mergeSchema` only when required due to schema drift.
- Keep schema decisions close to the read step.

## Notebooks
- Keep notebooks in `notebooks/` for exploration only.
- Migrate production logic into `src/` scripts.
- Clean outputs before committing notebooks when possible.

## Packaging & imports
- Repo uses a `src/` layout; ensure import paths work in scripts.
- If you add a package config, prefer editable installs for local dev.
- Keep `__init__.py` files present for packages.
- Avoid relative imports that traverse upward (prefer absolute package imports).

## Dependency changes
- Add new dependencies to `pyproject.toml` with explicit versions.
- Update `uv.lock` after dependency changes.
- Avoid adding heavy libraries unless required for the pipeline.

## CLI scripts
- Keep script entry points under `scripts/` or `src/etl/jobs/`.
- Use `if __name__ == "__main__":` guards for runnable scripts.
- Add argparse for new scripts with user-facing options.

## Data transformations
- Keep rename maps adjacent to schema handling.
- Prefer `select` with explicit columns to avoid accidental schema drift.
- Validate assumptions (e.g., passenger counts) before filtering.

## Performance
- Avoid `collect()` and `toPandas()` on large datasets.
- Use `cache()` only when reused and memory is sufficient.
- Prefer vectorized Spark operations over Python UDFs.

## Adding tests (recommended pattern)
- Create `tests/` and use `test_*.py` naming.
- Use pytest fixtures for Spark sessions and S3 mocks.
- Keep unit tests small; integration tests can be marked and skipped by default.
- Document any required local services (MinIO, Spark) in test docs.

## File hygiene
- Do not commit data files, large artifacts, or `.venv/`.
- Keep `uv.lock` and `pyproject.toml` in sync.
- Avoid editing generated files under `build/`.
- Keep notebooks lightweight; strip outputs when possible.

## PRs/commits (if you create them)
- Keep commits focused and descriptive.
- Mention data/schema changes in commit messages.
- Note any new runtime requirements (Spark configs, S3 paths).

## When unsure
- Search existing scripts for patterns before introducing new ones.
- Follow Ruff/formatter output; do not hand-format.
- Ask for clarification if a change affects data contracts or S3 layout.
- Prefer making small, reviewable changes over large rewrites.
