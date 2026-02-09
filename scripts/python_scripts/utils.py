import re
import os
import json
import yaml
import csv
import logging
import requests
import tempfile
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def configure_logging(log_dir, log_level):
    """Configure logging to file and console."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"cms_extractor_{datetime.now().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def build_session():
    """Build a requests session with retry logic."""
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "cms-metastore-extractor/1.0"})
    return session

def detect_csv_dialect(file_path):
    """Detect the dialect of a CSV file."""
    with open(file_path, "r", newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(65536)
        f.seek(0)
        try:
            return csv.Sniffer().sniff(sample)
        except csv.Error:
            return csv.excel

def to_snake_case(text):
    """
    Convert a string to snake_case.
    Example: "Patients’ rating of the facility linear mean score" 
             -> "patients_rating_of_the_facility_linear_mean_score"
    """
    if not isinstance(text, str):
        return str(text)
        
    # Replace non-alphanumeric characters with underscores
    str_cleaned = re.sub(r'[^a-zA-Z0-9]', '_', text.strip())
    # Replace multiple underscores with a single one
    str_cleaned = re.sub(r'_+', '_', str_cleaned)
    # Remove leading/trailing underscores and convert to lowercase
    return str_cleaned.strip('_').lower()

def load_config(config_path):
    """Load YAML configuration."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def load_state(state_file):
    """Load the state file tracking download history."""
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print("Warning: Corrupt state file, starting fresh.")
            return {"datasets": {}, "last_run": None}

        # Backwards compatibility with older flat dict state
        if isinstance(data, dict) and "datasets" not in data:
            return {"datasets": data, "last_run": None}
        return data
    return {"datasets": {}, "last_run": None}

def save_state(state, state_file, control_dir=None):
    """Save the state file atomically."""
    if control_dir:
        os.makedirs(control_dir, exist_ok=True)

    state["last_run"] = datetime.now(timezone.utc).isoformat()
    directory = os.path.dirname(state_file)
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix="state_", suffix=".json")
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(state, f, indent=4)
        os.replace(tmp_path, state_file)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

def get_required_env(var_name: str, var_type=str):
    """Get required environment variable or raise error if not set."""
    value = os.getenv(var_name)
    if value is None:
        # Calculate project root relative to this utils script (scripts/python_scripts/utils.py)
        # Up 3 levels: utils.py -> python_scripts -> scripts -> project_root
        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(base_dir))
        env_path = os.path.join(project_root, ".env")
        
        raise ValueError(
            f"Required environment variable '{var_name}' is not set!\n"
            f"Please set it in your .env file at: {env_path}\n"
            f"See .env.example for template."
        )
    return var_type(value) if var_type != str else value
