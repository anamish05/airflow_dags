

# Amazon ETL with Apache Airflow + Docker

This repository contains the full project **Amazon ETL with Apache Airflow and Docker**.
It demonstrates a complete **end-to-end ETL workflow** using Apache Airflow, Docker, and MySQL, from local development all the way to production-like ready deployment patterns.

---

## Project Structure

* **`amazon-etl/`** — The Amazon scraping + transformation code used for the real-world ETL example.

* **`docker-compose.yaml`** – A fully configured Airflow environment (api-server, scheduler, triggerer, DAG processor, metadata DB, logs).

The project walks through:

* Running Airflow inside Docker Compose
* Scraping real Amazon book data
* Transforming messy HTML output into clean analytics data
* Loading data into MySQL
* Syncing DAGs from GitHub using `git-sync`
* Lightweight CI for validating DAGs on every push

---

## How to Run the Project

Clone the repository:

```bash
git clone git@github.com:dataquestio/tutorials.git
cd amazon-docker-tutorial
```

Create required folders:

```bash
mkdir -p ./dags ./logs ./plugins ./config
```

Initialize Airflow:

```bash
docker compose up airflow-init
```

Start all services:

```bash
docker compose up -d
```

Access the Airflow UI:

```
http://localhost:8080
```

**Credentials:**

```
Username: airflow
Password: airflow
```

---

## Example DAGs

### `amazon_books_etl`

A real-world ETL pipeline that:

* **Extracts** book listings from Amazon (title, author, price, rating)
* **Transforms** the raw HTML data into numeric fields
* **Loads** the cleaned dataset into a MySQL table
* Runs automatically on a daily schedule

Files are saved to `/opt/airflow/tmp/` inside the container.

---


## Resetting the Environment

Stop all running containers:

```bash
docker compose down
```

Reset the environment completely (including the metadata database):

```bash
docker compose down -v
```


