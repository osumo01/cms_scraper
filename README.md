# CMS Metastore Extractor

A robust, fault-tolerant Python ETL pipeline for extracting, transforming, and loading (ETL) healthcare datasets from the CMS (Centers for Medicare & Medicaid Services) Data API based on configurable themes.

---
### Business Logic Configuration (`scripts/config/meta_store.yaml`)

Configure which datasets to download by keyword/theme:
```yaml
theme_filter:
  - "Hospitals"
  - "Nursing Homes"  # Add more themes as needed
```


## Setup & Installation

### Prerequisites
- Python 3.12+ 
- `pip` (Python package manager)
- Docker & Docker Compose (optional, for containerized execution)

### Local Installation

1. **Clone the repository**:
   ```bash
   git clone <repo-url>
   cd cms_scraper
   ```

2. **Create a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment**:
   Copy the example configuration and edit it:
   ```bash
   cp .env.example .env
   nano .env
   ```

### Running the Scraper

**Option A: Local Python (Virtual Env)**
```bash
python scripts/python_scripts/cms_metastore_extractor.py
```

**Option B: Docker (Portable)**
Build and run the container. The script starts immediately, processes data, and saves files to your local `working_directory/` before exiting.
```bash
# Build
docker build -t cms_scraper .

# Run
docker run --rm \
  --env-file .env \
  -v $(pwd)/working_directory:/cms_scraper/working_directory \
  cms_scraper:latest
```



### Environment Variables (`.env`)

**Location:** Project root

Strict configuration is enforced. You **must** set these variables in your `.env` file:

| Variable | Description | Example |
|----------|-------------|---------|
| `CMS_API_URL` | CMS API Endpoint | `https://data.cms.gov/...` |
| `MAX_WORKERS` | Number of parallel downloads | `5` |
| `WORKING_DIR` | Base directory for data | `working_directory` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |

**Security:**
- Never commit `.env` to version control (already in `.gitignore`)
- Use `.env.example` as a template



### Production Deployment

#### 1. Apache Airflow (Docker/Kubernetes)
Use the `DockerOperator` or `KubernetesPodOperator` to run the container as a task.

```python
# Example: KubernetesPodOperator
extract_task = KubernetesPodOperator(
    task_id='extract_cms_data',
    image='your-registry/cms_scraper:latest',
    env_vars={'CMS_API_URL': '...', 'MAX_WORKERS': '10'},
    name='cms-extractor',
    ...
)
```

#### 2. Databricks

**Option A: Run Python Script**
1.  Upload `scripts/` and `requirements.txt` to DBFS.
2.  Create a job that runs:
    ```python
    %pip install -r requirements.txt
    from scripts.python_scripts.cms_metastore_extractor import run_job
    run_job()
    ```

**Option B: Use Docker Container**
1.  Push your image to a registry (Docker Hub, ECR, etc.).
2.  In Databricks **Compute** -> **Basic** (or Job Cluster):
    *   Enable "Use your own Docker container".
    *   Enter your image URL (e.g., `your-registry/cms_scraper:latest`).
3.  The cluster will start with all dependencies pre-installed. You can then run:
    ```python
    from scripts.python_scripts.cms_metastore_extractor import run_job
    run_job()
    ```

---

## Output Structure

All data is saved to the configured `WORKING_DIR` (default: `working_directory/`):

```
working_directory/
├── landing/      # Raw CSV files downloaded from CMS
├── output/       # Processed CSV files (snake_case headers)
├── control/      # State tracking (state.json)
└── logging/      # Daily log files (e.g., cms_extractor_20260209.log)
```

- **`dataset_catalog.json`**: A comprehensive catalog tracking the metadata (title, last modified date, first seen date) of all processed datasets.
- **Output Files**: Named descriptively as `{snake_case_title}_{id}.csv` (e.g., `hospital_general_information_xubh-q36u.csv`).

---

## Verification

You can verify that a downloaded file matches your theme filter using `curl`.

**Example:** Check if file `5gv4-jwyv.csv` (ID `5gv4-jwyv`) is actually related to "Hospitals":

```bash
curl -s "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/5gv4-jwyv" | grep -o '"theme":\[[^]]*\]'
```

**Output:**
```json
"theme":["Hospitals"]
```

This confirms the dataset is correctly tagged with the "Hospitals" theme.

---
