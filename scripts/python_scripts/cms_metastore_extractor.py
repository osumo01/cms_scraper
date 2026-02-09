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
CONTROL_DIR = os.path.join(WORKING_DIR, "control")   # State/Metadata/Catalog
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

def process_dataset(dataset, state, session):
    """
    Orchestrate download and transformation of a single dataset.
    Returns metadata dict on success, None otherwise.
    """
    identifier = dataset.get('identifier')
    title = dataset.get('title', identifier)
    modified_date = dataset.get('modified')

    if not identifier:
        return None

    # 1. Check State
    # State structure can be old (string date) or new (dict with metadata)
    saved_state = state.get("datasets", {}).get(identifier)
    last_processed_date = None
    
    if isinstance(saved_state, dict):
        last_processed_date = saved_state.get('last_modified')
    else:
        last_processed_date = saved_state  # Legacy format
        
    # Incremental check: if modified date matches state, skip
    if last_processed_date and modified_date == last_processed_date:
        # Construct expected filename to check existence
        sanitized_title = to_snake_case(title)
        if len(sanitized_title) > 50: sanitized_title = sanitized_title[:50]
        expected_filename = os.path.join(OUTPUT_DIR, f"{sanitized_title}_{identifier}.csv")
        
        if os.path.exists(expected_filename):
            logging.info("Skipping '%s' (%s) - Already up to date.", title, identifier)
            return None

    # 2. Determine Filenames
    # Use snake_case title for better readability: "Hospital Info" -> "hospital_info"
    # Filename format: {sanitized_title}_{id}.csv
    sanitized_title = to_snake_case(title)
    # Truncate title if too long to avoid filesystem issues
    if len(sanitized_title) > 50:
        sanitized_title = sanitized_title[:50]
        
    filename = f"{sanitized_title}_{identifier}.csv"
    
    landing_path = os.path.join(LANDING_DIR, filename)
    output_path = os.path.join(OUTPUT_DIR, filename)

    # Find the CSV distribution
    csv_url = None
    for dist in dataset.get('distribution', []):
        media_type = (dist.get('mediaType') or "").lower()
        download_url = dist.get('downloadURL') or ""
        format_hint = (dist.get('format') or "").lower()
        if media_type == 'text/csv' or format_hint == "csv" or download_url.lower().endswith(".csv"):
            csv_url = download_url
            break
            
    if not csv_url:
        print(f"Skipping '{title}' ({identifier}) - No CSV distribution found.")
        return None

    logging.info("Processing '%s' (%s)...", title, identifier)

    # 3. Download RAW data to Landing
    if not download_data(session, csv_url, landing_path):
        return None
    
    # 4. Transform Phase (Read from Landing -> Process -> Save to Output)
    if not transform_data(landing_path, output_path):
        return None

    logging.info("Successfully processed %s -> %s", identifier, output_path)
    
    # Return enriched metadata
    current_time = datetime.now(timezone.utc).isoformat()
    return {
        "identifier": identifier,
        "last_modified": modified_date,
        "title": title,
        "first_seen": current_time, # Will be preserved if already exists in run_job
        "last_processed": current_time
    }

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
                identifier = result['identifier']
                
                # Preserve 'first_seen' if it existed in old state
                old_entry = state["datasets"].get(identifier)
                if isinstance(old_entry, dict) and 'first_seen' in old_entry:
                    result['first_seen'] = old_entry['first_seen']
                
                # Update catalog
                state["datasets"][identifier] = result
                updates_count += 1
                
    # 4. Commit Phase
    if updates_count > 0:
        save_state(state, STATE_FILE, CONTROL_DIR)
        logging.info("Job completed. Updated %d datasets.", updates_count)
    else:
        logging.info("Job completed. No new updates found.")

if __name__ == "__main__":
    configure_logging(LOG_DIR, LOG_LEVEL)
    logging.info("Starting CMS Metastore Extractor...")
    run_job()
    logging.info("Job execution completed.")
