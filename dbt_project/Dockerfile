FROM python:3.11-slim

WORKDIR /dbt

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data

ENV DBT_PROFILES_DIR=/dbt
ENV DBT_PROJECT_DIR=/dbt

RUN mkdir -p /dbt/target
RUN mkdir -p /dbt/logs

RUN dbt deps

ENTRYPOINT ["dbt"] 