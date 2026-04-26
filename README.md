# cybulde-data-preparation

A **well-architected, cloud-native data preprocessing pipeline** for cyberbullying and toxic content detection. The system ingests three heterogeneous text datasets, applies a composable NLP cleaning chain via Dask distributed computing, and outputs standardized, ML-ready Parquet files — deployable to Google Cloud Platform at the push of a button.

---

## Overview

Training a good toxic-content classifier starts long before the model. This pipeline handles everything upstream of training: pulling raw data from versioned storage, normalizing it across sources, cleaning text with a configurable sequence of operations, and writing stratified train/dev/test splits to cloud storage. Infrastructure and hyperparameters live entirely in YAML configuration files — no hardcoded flags, no manual steps.

```
┌──────────────────────────────────────────────────────────────┐
│                       Data Sources (DVC)                     │
│   GHC (TSV)    Jigsaw Toxic Comments (CSV)   Twitter (CSV)   │
└──────────────────────┬───────────────────────────────────────┘
                       │
              DatasetReaderManager
                       │  merge + repartition
                       ▼
         ┌─────────────────────────┐
         │  Dask Distributed       │
         │  Cluster                │
         │  (local or GCP VMs)     │
         └────────────┬────────────┘
                      │  map_partitions
                      ▼
         ┌─────────────────────────┐
         │  DatasetCleanerManager  │
         │  (10 composable steps)  │
         └────────────┬────────────┘
                      │  filter + split
                      ▼
         train.parquet  dev.parquet  test.parquet
              (Google Cloud Storage)
```

---

## Key Features

- **Three-dataset ingestion** — Ghent Hate Comments, Jigsaw Toxic Comments, and a Twitter cyberbullying dataset are unified into a single labeled corpus.
- **Composable text cleaning** — Ten independently configurable cleaning operations (URL removal, punctuation stripping, stopword removal, spell correction, and more) are chained via a manager class.
- **Dask-powered parallelism** — `map_partitions` distributes cleaning across workers; smart repartitioning respects available memory per worker.
- **Two deployment targets** — Run locally with a 12-worker `LocalCluster` or scale out to GCP VMs provisioned on-demand via `dask-cloudprovider`.
- **Configuration-as-code (Hydra)** — Every tuneable parameter — dataset paths, cleaning steps, cluster topology, GCP region — lives in composable YAML files backed by typed Pydantic dataclasses.
- **Reproducible runs** — The final composed config is pickled alongside the output, and Docker image metadata (`docker_info.yaml`) is written with every run so any result can be traced back to an exact image + config.
- **Cloud-native I/O** — A unified `open_file()` abstraction treats `gs://` and local paths identically via `fsspec`/`gcsfs`.
- **Secrets out of code** — GitHub access tokens for DVC are fetched at runtime from GCP Secret Manager; no credentials appear in configs or environment files.
- **Stratified splits** — Train/dev/test partitioning preserves label distribution via `dask-ml`.

---

## Architecture

### Configuration Layer (Hydra + Pydantic)

All behavior is driven by structured YAML configs in `cybulde/configs/`. Hydra composes them at startup; Pydantic dataclasses enforce types. A dedicated script (`generate_final_config.py`) merges defaults with any CLI overrides and persists the result as a pickle, which the main script then loads — decoupling config construction from execution.

```
cybulde/configs/
├── data_processing_config.yaml       # root config (entry point)
├── dataset_reader_manager/           # which datasets to load
│   └── dataset_reader/               # per-dataset paths & schemas
├── dataset_cleaner_manager/          # which cleaning steps to apply
├── dask_cluster/
│   ├── local_dask_cluster.yaml       # 12 workers, 1 GB/worker
│   └── gcp_dask_cluster.yaml         # n1-standard-1 VMs, us-east4-a
└── running_mode/
    ├── local.yaml
    └── n1-standard-1.yaml            # 3.75 GB memory budget
```

### Dataset Readers

An abstract `DatasetReader` defines the contract; three concrete implementations handle format-specific parsing (TSV vs CSV, column remapping, label normalization). `DatasetReaderManager` instantiates all configured readers, merges their outputs into a single Dask DataFrame, and applies repartitioning before passing data downstream.

### Text Cleaning Chain

`DatasetCleanerManager` wraps up to twelve `DatasetCleaner` implementations in order:

| Step | Operation |
|------|-----------|
| 1 | Lowercase conversion |
| 2 | URL removal |
| 3 | `@mention` removal |
| 4 | Retweet (`RT`) marker removal |
| 5 | Punctuation stripping |
| 6 | Non-letter character removal |
| 7 | Non-ASCII filtering |
| 8 | Newline normalization |
| 9 | English stopword removal (NLTK) |
| 10 | Spell correction (SymSpell) |
| 11 | Character-limit truncation |
| 12 | Minimum word-count filtering (post-clean) |

Each cleaner is a callable that operates on a pandas Series; the manager applies them via Dask's `map_partitions` for zero-copy distributed execution.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10 |
| Config management | [Hydra](https://hydra.cc/) 1.3 + Pydantic 1.10 |
| Distributed compute | [Dask](https://www.dask.org/) 2023.5 + Distributed |
| Cloud provisioning | dask-cloudprovider (GCP) |
| Data versioning | [DVC](https://dvc.org/) 2.56 (GCS + GDrive backends) |
| NLP utilities | NLTK 3.8 (stopwords, tokenization) |
| Spell correction | SymSpellPy 6.7 |
| File I/O | fsspec + gcsfs (unified local/GCS abstraction) |
| Serialization | fastparquet |
| Secrets | GCP Secret Manager |
| Containerization | Docker (linux/amd64, python:3.10-slim) |
| Package management | Poetry 1.4.2 |
| Type checking | mypy (strict) |
| Linting/formatting | Black, isort, flake8 |

---

## Prerequisites

- Docker and Docker Compose
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud`) authenticated to your project
- A GCP service account key placed at `./creds/<project-id>-<key-id>.json`
- GCP Secret Manager secret `cybulde-data-github-access-token` containing a GitHub PAT with read access to the raw-data repository
- DVC-tracked raw datasets stored in GCS or GDrive

---

## Quickstart

### 1. Clone and configure

```bash
git clone https://github.com/ajohnson114/cybulde-data-preparation.git
cd cybulde-data-preparation
```

Update `cybulde/configs/data_processing_config.yaml` (or override on the command line) with your:
- GCP project ID
- GCS bucket paths
- Secret Manager secret name

### 2. Start the development environment

```bash
make build      # build the Docker image
make up         # start the container (mounts project root + gcloud config)
make exec-in    # open a shell inside the running container
```

### 3. Generate the final config

```bash
make generate-final-data-processing-config \
    CONFIG_NAME=data_processing_config \
    OVERRIDES="infrastructure.project_id=my-project ..."
```

This composes all YAML layers into a single pickle at `cybulde/configs/automatically_generated/data_processing_config.pickle`.

### 4. Run the pipeline

**Locally (Dask LocalCluster, 12 workers):**

```bash
make local-process-data
```

**On GCP (dask-cloudprovider, VMs auto-provisioned):**

```bash
make process-data
```

This also builds a fresh Docker image, pushes it to GCP Artifact Registry, and embeds the image tag in the output metadata.

---

## Makefile Reference

| Target | Description |
|--------|-------------|
| `make build` | Build the Docker image |
| `make up` / `make down` | Start / stop the Docker Compose stack |
| `make exec-in` | Shell into the running container |
| `make process-data` | Build image → push to Artifact Registry → run pipeline on GCP |
| `make local-process-data` | Run pipeline locally with Dask LocalCluster |
| `make generate-final-data-processing-config` | Compose and pickle the final Hydra config |
| `make notebook` | Launch JupyterLab on port 8888 |
| `make lint` | Run flake8 |
| `make format` | Run Black + isort |
| `make check-type-annotations` | Run mypy |
| `make test` | Run pytest |
| `make push` | Push Docker image to GCP Artifact Registry |
| `make lock-dependencies` | Regenerate `poetry.lock` |

---

## Project Structure

```
cybulde-data-preparation/
├── cybulde/
│   ├── configs/                        # Hydra YAML config tree
│   │   ├── automatically_generated/    # pickled final configs (git-ignored)
│   │   ├── dask_cluster/
│   │   ├── dataset_cleaner_manager/
│   │   ├── dataset_reader_manager/
│   │   └── running_mode/
│   ├── data_processing/
│   │   ├── dataset_cleaners.py         # 10+ cleaner implementations + manager
│   │   └── dataset_readers.py          # 3 dataset readers + manager
│   ├── utils/
│   │   ├── config_utils.py             # Hydra config loading/saving helpers
│   │   ├── data_utils.py               # DVC, repartitioning, split filtering
│   │   ├── gcp_utils.py                # Secret Manager client
│   │   ├── io_utils.py                 # Unified local/GCS file I/O
│   │   └── utils.py                    # Logging, shell execution, spell init
│   ├── generate_final_config.py        # Config composition entry point
│   └── process_data.py                 # Pipeline orchestration entry point
├── docker/
│   └── Dockerfile                      # linux/amd64 production image
├── notebooks/                          # Exploratory analysis
├── docker-compose.yaml
├── Makefile
└── pyproject.toml
```

---

## Outputs

Each pipeline run writes to `gs://<bucket>/data/processed/<run_tag>/`:

| File | Description |
|------|-------------|
| `train.parquet` | Training split |
| `dev.parquet` | Validation split |
| `test.parquet` | Test split |
| `docker_info.yaml` | Image name + tag used for this run |

All Parquet files share the schema `{text, label, split, dataset_name}` with binary labels (0 = benign, 1 = toxic/cyberbullying).

---

## Design Decisions

**Why Hydra for config?** The pipeline has three orthogonal axes of variation: which datasets to load, which cleaning steps to apply, and where to run. Hydra's config composition handles all three cleanly without any conditional logic in Python code. Pydantic dataclasses catch misconfiguration at startup rather than mid-run.

**Why pickle the composed config?** The final config pickle is a single deterministic artifact that fully describes a run. The main script loads it via a decorator, so `process_data.py` has no knowledge of Hydra internals and can be invoked directly in tests.

**Why Dask instead of Spark?** Dask integrates natively with pandas, deploys to GCP VMs without a cluster manager, and gives a live browser dashboard (`bokeh`) out of the box — appropriate for single-node-equivalent workloads that still benefit from parallelism.

**Why DVC for raw data?** Raw datasets are large binary files that don't belong in Git. DVC provides content-addressable versioning on top of GCS/GDrive, so the exact dataset version used in any run is recorded in the commit history alongside the code.

---

## License

[MIT](LICENSE)
