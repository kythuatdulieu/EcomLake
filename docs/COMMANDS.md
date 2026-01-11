# Useful Commands

## Docker Management

### Start Services
Start all services in detached mode:
```bash
docker compose up -d
```

### Stop Services
Stop all services:
```bash
docker compose down
```

### View Logs
View logs for a specific service (e.g., etl_pipeline):
```bash
docker compose logs -f etl_pipeline
```

### Rebuild Images
Rebuild images after changing Dockerfiles:
```bash
docker compose build
```
Or force rebuild without cache:
```bash
docker compose build --no-cache
```

## Application Development

### Streamlit App
Run the Streamlit app locally (ensure dependencies are installed):
```bash
streamlit run app/Home.py
```

### Accessing Containers
Access the Spark Master shell:
```bash
docker exec -it spark-master /bin/bash
```

Access MySQL shell:
```bash
docker exec -it mysql_db mysql -u user -p
```
(Password is usually `password` or as defined in `.env`)

## ETL Operations

### Dagster
Reload Dagster code location:
Usually handled automatically by the daemon, but you can restart the container:
```bash
docker compose restart etl_pipeline
```
