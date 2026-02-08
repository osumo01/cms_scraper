# Use the specific Python slim image requested
FROM python:3.12.12-slim-bookworm

# Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing pyc files to disc
# PYTHONUNBUFFERED: Prevents Python from buffering stdout and stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/cms_scraper

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

# Copy requirements first for better caching (if it exists)
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY --chown=appuser:appuser . .

# Switch to the non-root user
USER appuser

# Define the default command
CMD ["python", "--version"]
