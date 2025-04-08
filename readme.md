# Project Overview

This project was inspired by Joseph M (Start Data Engineering)'s prompt on an e-commerce POC, so I implemented this assuming I was working on *Nike's E-commerce Market Attribution" the goal is to understand etl incremental batch loading with the use of PostgreSQL, for CDC and using DuckDB for processing engine, dbt for incremental modelling, analysis done on Tableau.

We will answer questions based on this business case:

## Architecture Overview

[IMAGE ARCHI]

(explain the systems)

do a table here for each tools explaining reasoning of use
tool name, use,  reason for use

## Data Visualization

## Requirements

- Docker installed on your computer
- Knowledge of Python

## How to Run

(explain)

```sh
    docker compose up --build -d
```

## Lessons Learned

## Tools Used

| Tool Name | Use | Reason for Use |
|-----------|-----|----------------|
| PostgreSQL | Database Management System | - Robust and reliable relational database<br>- Excellent for structured data<br>- Strong support for complex queries<br>- Industry standard for data storage |
| Docker | Containerization Platform | - Ensures consistent environment across different machines<br>- Easy deployment and scaling<br>- Isolates database from other system components<br>- Simplifies development setup |
| Python (psycopg2) | Database Connection Library | - Native PostgreSQL adapter<br>- Efficient data handling<br>- Supports all PostgreSQL features<br>- Well-documented and widely used |
| Docker Compose | Container Orchestration | - Simplifies multi-container setup<br>- Easy configuration management<br>- Streamlines development workflow<br>- Handles volume management automatically |

## Project Structure
- `docker-compose.yml`: Defines the PostgreSQL service configuration
- `init.sql`: Contains database schema and initial data setup
- `create_data.py`: Python script for data generation and insertion
- `requirements.txt`: Lists Python package dependencies
