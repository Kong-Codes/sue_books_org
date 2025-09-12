# Sue Books

## Architecture/Design Decisions

### Ingestion
Loaded the raw data into the system. In this phase, I made sure to convert the DataFrames back into paths to avoid byte errors during orchestration. Afterwards, I transferred the data to the transformation job.

### Transformation and Data modelling
After collecting the file paths using XCom, I proceeded with transformation and created all necessary tables (users, transactions, books). Following this, I created the fact/dimension tables for my OLAP schema:
- `dim_date`
- `dim_book` 
- `dim_users`
- `fact_transactions`
- `fact_daily_sales`
- `fact_top_books`

The ERD will be attached below for both the OLTP and OLAP schemas.

OLTP ERD
![OLTP](images/oltp_erd.png)

OLAP ERD
![OLAP](images/olap_erd.png)

### Loading
For the loading phase, I created the schema and database, then loaded the data into the database with all data types enforced. I also created two separate schemas to differentiate between the OLAP and OLTP tables.

### Orchestration
For the ingestion pipeline creation, I used Apache Airflow to orchestrate my ingestion pipeline. This pipeline consists of 3 tasks:
- `extract_data`
- `transform_data` 
- `load_data`

These tasks are responsible for running the data pipeline from the ingestion phase up until the loading phase.

### Monitoring
I used the Python logging module to log all important outputs that can be used to check pipeline health and monitor for any pipeline failures. I also used it to log FastAPI outputs to track successful runs.

### Containerization
I used Docker containers to hold all my processes for easy deployment across different systems.

---

## Prerequisites

### System Requirements
- **Python**: 3.13 or higher
- **Docker**: 20.10+ and Docker Compose 2.0+
- **Memory**: Minimum 4GB RAM (8GB recommended)
- **Disk Space**: Minimum 10GB free space
- **CPU**: Minimum 2 cores (4 cores recommended)

### Package Managers
- **uv** (recommended): Modern Python package manager
- **pip**: Standard Python package installer

## Quick Start with Docker (Recommended)

The easiest way to run the entire platform is using Docker Compose, which includes:
- PostgreSQL database
- Apache Airflow (scheduler, webserver, worker, triggerer)
- Redis (message broker)
- FastAPI backend
- Flask frontend

### 1. Clone and Setup
```bash
git clone https://github.com/Kong-Codes/sue_books_org.git
```

### 2. Create Environment File
Create a `.env` file in the project root:
```bash
# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.10.2
AIRFLOW_PROJ_DIR=.

# Database Configuration
DATABASE_URL=postgresql://sue_books:sue123456789@postgres:5432/sales_db

# API Configuration
PROCESSED_DIR=/opt/airflow/datasets/processed

# Flask Configuration
FLASK_SECRET_KEY=dev-secret-key-change-in-production
API_BASE_URL=http://api:8000
```

### 3. Initialize Airflow
```bash
# Create necessary directories
mkdir -p logs dags plugins config

# Initialize Airflow (one-time setup)
docker-compose up airflow-init

# Set proper permissions (Linux/macOS)
sudo chown -R 50000:0 logs dags plugins config
```

### 4. Start All Services
```bash
# Start the entire platform
docker-compose up -d

# View logs
docker-compose logs -f
```

### 5. Access Services
Once all services are running, access them at:

- **Frontend Dashboard**: http://localhost:5000
- **API Documentation**: http://localhost:8000/docs
- **Airflow Web UI**: http://localhost:8080 (admin/airflow)
- **PostgreSQL**: localhost:5432
- **Redis**: localhost:6379

## Local Development Setup

### 1. Install Dependencies

#### Using uv (Recommended)
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

#### Using pip
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .
```

### 2. Database Setup

#### Option A: Use Docker PostgreSQL
```bash
# Start only PostgreSQL
docker-compose up -d postgres redis

# Wait for database to be ready
docker-compose logs postgres
```

#### Option B: Local PostgreSQL
Install PostgreSQL locally and create the database:
```sql
CREATE DATABASE sales_db;
CREATE USER sue_books WITH PASSWORD 'sue123456789';
GRANT ALL PRIVILEGES ON DATABASE sales_db TO sue_books;
```

### 3. Environment Variables
```bash
# Database connection
export DATABASE_URL="postgresql://sue_books:sue123456789@localhost:5432/sales_db"

# Data processing directory
export PROCESSED_DIR="./datasets/processed"

# Flask configuration
export FLASK_SECRET_KEY="dev-secret-key"
export API_BASE_URL="http://localhost:8000"
```

### 4. Run Services Locally

#### Start the API Server
```bash
# Using uv
uv run uvicorn endpoints:app --reload --host 0.0.0.0 --port 8000

# Using pip
uvicorn endpoints:app --reload --host 0.0.0.0 --port 8000
```

#### Start the Frontend (in another terminal)
```bash
# Using uv
uv run flask --app frontend_app:app run --debug --host 0.0.0.0 --port 5000

# Using pip
flask --app frontend_app:app run --debug --host 0.0.0.0 --port 5000
```

#### Run Airflow Locally
```bash
# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start scheduler (in background)
airflow scheduler &

# Start webserver
airflow webserver --port 8080
```

## Project Dependencies

### Core Dependencies
- **FastAPI** (0.115.0+): Modern web framework for APIs
- **Flask** (3.0.3+): Web framework for frontend
- **Polars** (1.33.1+): High-performance DataFrame library
- **Apache Airflow** (3.0.6+): Workflow orchestration
- **PostgreSQL**: Primary database
- **Redis**: Message broker for Airflow

### Python Packages
```toml
# From pyproject.toml
dependencies = [
    "apache-airflow>=3.0.6",
    "asyncpg>=0.30.0",
    "polars>=1.33.1", 
    "psycopg2>=2.9.10",
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "python-dotenv>=1.0.1",
    "flask>=3.0.3",
    "requests>=2.32.3",
]
```

## API Endpoints

### Sales Analytics
- `GET /sales/daily?date=YYYY-MM-DD&limit=365` — Daily sales data
- `GET /books/top?limit=5` — Top performing books by revenue
- `GET /users/{user_id}/purchases?limit=100` — User purchase history

### API Behavior
The API automatically falls back between data sources:
1. **Primary**: PostgreSQL OLAP tables (if `DATABASE_URL` is reachable)
2. **Fallback**: Parquet files in `PROCESSED_DIR` created by Airflow DAGs

### Data Files
- `daily_sales.parquet` — Aggregated daily sales data
- `top_book.parquet` — Top books by revenue
- `transactions_clean.parquet` — Cleaned transaction data
- `users_clean.parquet` — Cleaned user data
- `books_clean.parquet` — Cleaned book data

## Data Pipeline

### Airflow DAG: `Sue_books_data_pipeline`
The pipeline consists of three main tasks:

1. **extract_data**: Load raw CSV data and convert to parquet
2. **transform_data**: Create OLTP and OLAP tables
3. **load_data**: Load transformed data into PostgreSQL

### Database Schemas
- **OLTP Schema**: Raw transactional data (users, books, transactions)
- **OLAP Schema**: Analytics tables (dim_date, dim_book, dim_users, fact_transactions, fact_daily_sales, fact_top_books)

## Troubleshooting

### Common Issues

#### Port Conflicts
If ports are already in use:
```bash
# Check what's using the ports
lsof -i :8000  # API port
lsof -i :5000  # Frontend port
lsof -i :8080  # Airflow port
lsof -i :5432  # PostgreSQL port

# Kill processes or change ports in docker-compose.yaml
```

#### Database Connection Issues
```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# View database logs
docker-compose logs postgres

# Test connection
docker-compose exec postgres psql -U sue_books -d sales_db -c "SELECT 1;"
```

#### Airflow Issues
```bash
# Check Airflow services
docker-compose ps | grep airflow

# View Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Reset Airflow database
docker-compose down
docker volume rm sales_project_postgres-db-volume
docker-compose up airflow-init
```

#### Permission Issues (Linux/macOS)
```bash
# Fix Airflow permissions
sudo chown -R 50000:0 logs dags plugins config

# Or set your user ID
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Logs and Monitoring
- **API Logs**: `logs/api.log`
- **Frontend Logs**: `logs/frontend.log`
- **Airflow Logs**: `logs/dag_id=Sue_books_data_pipeline/`
- **Container Logs**: `docker-compose logs [service-name]`

## Development Workflow

### Making Changes
1. Edit code in your IDE
2. For API changes: Restart uvicorn (auto-reload enabled)
3. For frontend changes: Restart Flask (debug mode enabled)
4. For DAG changes: Airflow auto-detects changes

### Testing the Pipeline
1. Access Airflow UI at http://localhost:8080
2. Find the `Sue_books_data_pipeline` DAG
3. Enable the DAG if needed
4. Trigger a manual run or wait for scheduled execution
5. Monitor task execution and logs

### Stopping Services
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clears database)
docker-compose down -v

# Stop specific service
docker-compose stop [service-name]
```

## Data VIsualisation
This can be found in the jupyter notebook @sales_data_cleaning i used matplotlib, seaborn adn ploty to generate insights and also added report summary and reccomendations.

This are the insights gotten:

Daily revenue trend:
![daily revenue trend](images/revenue.png)

Top books
![Top books](images/top_books.png)

Active Users and transaction overtime
![Active users](images/active_users.png)
