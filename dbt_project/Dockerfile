# dbt_project/Dockerfile
FROM python:3.10-slim
WORKDIR /dbt
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .