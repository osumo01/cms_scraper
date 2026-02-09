# Use the specific Python slim image requested
FROM python:3.12.12-slim-bookworm

# Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing pyc files to disc
# PYTHONUNBUFFERED: Prevents Python from buffering stdout and stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/cms_scraper

# Application configuration with defaults (override via docker run -e or K8s env)
ENV CMS_API_URL=https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items \
    MAX_WORKERS=5 \
    WORKING_DIR=working_directory \
    LOG_LEVEL=INFO

# Set the working directory
WORKDIR $APP_HOME

# Install system dependencies
# git and other build tools are often needed for installing python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and set permissions
# Running as root is a security risk
RUN useradd -m -r appuser && \
    chown -R appuser:appuser $APP_HOME

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY --chown=appuser:appuser . .

# Switch to the non-root user
USER appuser

# Define the default command
CMD ["python", "scripts/python_scripts/cms_metastore_extractor.py"]
