# Job Trend Analysis
This project is designed as a data pipeline and analysis platform that fetches, processes, and analyzes job-related data. It integrates with AWS services and provides both backend processing and frontend visualization using Streamlit.

## Project Structure
    app/
    ├─ .streamlit/           # Streamlit configuration
    │   └─ secrets.toml      
    ├─ streamlit_app.py      # Streamlit dashboard for visualization

    db/
    └─ schema.sql            # Database schema for RDS (tables, constraints, etc.)

    notebooks/
    ├─ eda.ipynb             # Exploratory Data Analysis notebook
    └─ .ipynb_checkpoints/   

    src/
    ├─ config.py             # Configuration (AWS keys, DB connection, etc)
    ├─ fetch_jobs.py         # Script to fetch job data from API
    ├─ process_jobs.py       # Script to process and transform job data
    └─ __pycache__/          

    dataQualityChecks.txt     # Notes on data validation and quality checks
    docker-compose.yml        # Orchestration of Docker containers
    Dockerfile.ingest         # Dockerfile for ingestion Lambda
    Dockerfile.processor      # Dockerfile for processing Lambda
    mydb.dump                 # Example database dump
    requirements.txt          # Python dependencies for local dev
    requirements.lambda.txt   # Minimal Python dependencies for AWS Lambda
    README.md                 

## Features
- **Data Ingestion**
    - Fetch job data from external APIs or sources (fetch_jobs.py), store raw data in AWS S3

- **Data Processing**
    - Clean, transform, and validate data (process_jobs.py), then store results into AWS RDS using schema defined in db/schema.sql

- **Data Quality Checks**
    - Rules and validations tracked in dataQualityChecks.txt

- **Data Visualization**
   - Interactive dashboard built with Streamlit (app/streamlit_app.py) to explore and analyze data

- **Automation with AWS Lambda**
    - Ingestion Lambda: Fetches and uploads raw data to an S3 bucket
    - Processor Lambda: Processes data and loads into RDS

- **Secrets Management**
    - Uses AWS Secrets Manager for database credentials and API keys

- **Containerization**
    - Dockerfiles provided for local testing, Lambda deployment, and ingestion/processing services