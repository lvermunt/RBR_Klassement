![](https://github.com/lvermunt/RBR_Klassement/workflows/Test%20package/badge.svg)
![](https://img.shields.io/github/license/lvermunt/RBR_Klassement)

Small package to produce the general classification of the Dutch Run-Bike-Run Series 2024.

This project uses a modern Python, uv-first workflow and a Polars-based data pipeline.

## Setup

```bash
git clone https://github.com/lvermunt/RBR_Klassement.git
cd RBR_Klassement
uv sync --group dev
```

Use Python 3.13+ via uv automatically:

```bash
uv run python --version
```

## Run checks

```bash
uv run ruff check .
uv run ty check .
uv run pytest -q
```

## Build

```bash
uv build
```
