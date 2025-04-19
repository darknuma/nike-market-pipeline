# dbt_project/Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /dbt

# Copy requirements first for better caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy dbt project files
COPY . .

# Create directory for DuckDB database
RUN mkdir -p /data

# Set environment variables
ENV DBT_PROFILES_DIR=/dbt
ENV PYTHONPATH=/dbt

# Default command
CMD ["dbt", "debug"] 