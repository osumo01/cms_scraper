import os
import csv
import logging
import requests
import concurrent.futures
from datetime import datetime, timezone
from dotenv import load_dotenv
from utils import to_snake_case, load_config, load_state, save_state, get_required_env, configure_logging, build_session, detect_csv_dialect

# ============================================================================
# Configuration - Load from .env and YAML
# ============================================================================

# Get base directory and project root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# Load environment variables from .env file at project root
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Environment-specific configuration from .env
API_URL = get_required_env("CMS_API_URL")
MAX_WORKERS = get_required_env("MAX_WORKERS", int)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Directory configuration from .env
WORKING_DIR_NAME = get_required_env("WORKING_DIR")
WORKING_DIR = os.path.join(PROJECT_ROOT, WORKING_DIR_NAME) if not os.path.isabs(WORKING_DIR_NAME) else WORKING_DIR_NAME

# Define sub-directories for the ETL pipeline
LANDING_DIR = os.path.join(WORKING_DIR, "landing")   # Raw data
OUTPUT_DIR = os.path.join(WORKING_DIR, "output")     # Processed data
CONTROL_DIR = os.path.join(WORKING_DIR, "control")   # State/Metadata
LOG_DIR = os.path.join(WORKING_DIR, "logging")       # Logs

STATE_FILE = os.path.join(CONTROL_DIR, "dataset_catalog.json")

# Business logic configuration from YAML
CONFIG_PATH = os.path.join(os.path.dirname(BASE_DIR), "config", "meta_store.yaml")
config = load_config(CONFIG_PATH)
THEME_FILTER = config.get('theme_filter')


def download_data(session, url, filepath):
    """Download data from URL to the specified filepath."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True
    except Exception as e:
        logging.error("Error downloading %s: %s", url, e)
        return False

def transform_data(input_path, output_path):
    """Read CSV, snake_case headers, and save to output."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        dialect = detect_csv_dialect(input_path)
        with open(input_path, "r", newline="", encoding="utf-8", errors="replace") as src, \
            open(output_path, "w", newline="", encoding="utf-8") as dst:
            reader = csv.reader(src, dialect)
            writer = csv.writer(dst, dialect)

            header = next(reader, None)
            if header is None:
                logging.warning("Empty file: %s", input_path)
                return False

            writer.writerow([to_snake_case(col) for col in header])
            for row in reader:
                writer.writerow(row)
        return True
    except Exception as e:
        logging.error("Error transforming %s: %s", input_path, e)
        return False

def _get_csv_distributions(dataset):
    csv_distributions = []
    for dist in dataset.get('distribution', []):
        media_type = (dist.get('mediaType') or "").lower()
        download_url = dist.get('downloadURL') or ""
        format_hint = (dist.get('format') or "").lower()
        if media_type == 'text/csv' or format_hint == "csv" or download_url.lower().endswith(".csv"):
            csv_distributions.append(dist)
    return csv_distributions

def _build_dist_suffix(dist, index):
    return f"distribution_{index}"

def _build_file_stem(identifier, title, dist_suffix):
    candidate_title = to_snake_case(title)
    if len(candidate_title) > 50:
        candidate_title = candidate_title[:50]
        
    stem = f"{candidate_title}_{identifier}"
    if dist_suffix:
        return f"{stem}_{dist_suffix}"
    return stem

def process_dataset(dataset, state, session):
    """
    Orchestrate download and transformation of a single dataset.
    Returns list of (state_key, modified_date) for processed files.
    """
    identifier = dataset.get('identifier')
    last_modified = dataset.get('modified')
    title = dataset.get('title', identifier)

    if not identifier:
        return None

    state_datasets = state.get("datasets", {})

    csv_distributions = _get_csv_distributions(dataset)
    if not csv_distributions:
        logging.info("Skipping '%s' (%s) - No CSV distribution found.", title, identifier)
        return None

    processed = []
    all_up_to_date = True
    for index, dist in enumerate(csv_distributions, start=1):
        dist_suffix = _build_dist_suffix(dist, index)
        state_key = f"{identifier}::{dist_suffix}"
        file_stem = _build_file_stem(identifier, title, dist_suffix)
        output_filename = os.path.join(OUTPUT_DIR, f"{file_stem}.csv")
        if last_modified and state_datasets.get(state_key) == last_modified and os.path.exists(output_filename):
            continue
        all_up_to_date = False

    if all_up_to_date:
        logging.info("Skipping '%s' (%s) - Already up to date.", title, identifier)
        return None

    logging.info("Processing '%s' (%s)...", title, identifier)

    for index, dist in enumerate(csv_distributions, start=1):
        csv_url = dist.get('downloadURL') or ""
        if not csv_url:
            continue

        dist_suffix = _build_dist_suffix(dist, index)
        state_key = f"{identifier}::{dist_suffix}"
        file_stem = _build_file_stem(identifier, title, dist_suffix)
        raw_filename = os.path.join(LANDING_DIR, f"{file_stem}.csv")

        output_filename = os.path.join(OUTPUT_DIR, f"{file_stem}.csv")

        if last_modified and state_datasets.get(state_key) == last_modified and os.path.exists(output_filename):
            continue

        if not download_data(session, csv_url, raw_filename):
            continue

        if not transform_data(raw_filename, output_filename):
            continue

        logging.info("Successfully processed %s -> %s", identifier, output_filename)
        current_time = datetime.now(timezone.utc).isoformat()
        processed.append({
            "identifier": identifier,
            "distribution_id": state_key,
            "last_modified": last_modified,
            "title": title,
            "last_processed": current_time,
            # 'first_seen' will be handled in run_job to preserve existing values
        })

    return processed if processed else None

def run_job():
    """Main job execution function."""
    logging.info("Starting CMS Data Extraction Job...")
    session = build_session()    
    
    # 1. Discovery Phase
    try:
        response = session.get(API_URL, timeout=30)
        response.raise_for_status()
        metastore = response.json()
    except Exception as e:
        logging.error("Critical Error: Failed to fetch metastore API. %s", e)
        return

    # 2. Filter Phase
    # Check if any of the configured themes are in the dataset's theme or keyword list
    theme_filters = config.get('theme_filter', [])
    if isinstance(theme_filters, str):
        theme_filters = [theme_filters]

    theme_filters = [t.strip().lower() for t in theme_filters if isinstance(t, str)]
    
    hospital_datasets = []
    logging.info("Scanning for datasets matching themes/keywords: %s", theme_filters)

    for item in metastore:
        # Get dataset tags
        item_themes = item.get('theme', [])
        item_keywords = item.get('keyword', [])
        
        # Normalize to lists
        if not isinstance(item_themes, list): item_themes = []
        if not isinstance(item_keywords, list): item_keywords = []
        
        # Combine all distinct tags to search against
        item_tags = {t.strip().lower() for t in (item_themes + item_keywords) if isinstance(t, str)}
        if not item_tags:
            continue

        # Check intersection: if any configured filter term is in the item's tags
        if any(t in item_tags for t in theme_filters):
            hospital_datasets.append(item)
    
    logging.info("Found %d datasets matching configured filters.", len(hospital_datasets))
    
    # 3. Evaluation & Execution Phase
    state = load_state(STATE_FILE)
    updates_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_dataset = {
            executor.submit(process_dataset, item, state, session): item
            for item in hospital_datasets
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_dataset):
            try:
                result = future.result()
            except Exception as e:
                dataset = future_to_dataset[future]
                logging.error("Failed processing dataset '%s': %s", dataset.get("identifier"), e)
                continue

            if result:
                for entry in result:
                    state_key = entry['distribution_id']
                    
                    # Preserve 'first_seen' if it existed in old state
                    old_entry = state["datasets"].get(state_key)
                    if isinstance(old_entry, dict) and 'first_seen' in old_entry:
                        entry['first_seen'] = old_entry['first_seen']
                    else:
                        entry['first_seen'] = entry['last_processed']
                    
                    # Update catalog
                    state["datasets"][state_key] = entry
                    updates_count += 1
                
    # 4. Commit Phase
    if updates_count > 0:
        save_state(state, STATE_FILE, CONTROL_DIR)
        logging.info("Job completed. Updated %d distributions.", updates_count)
    else:
        logging.info("Job completed. No new updates found.")

if __name__ == "__main__":
    configure_logging(LOG_DIR, LOG_LEVEL)
    run_job()
