# End-to-End MLOps Traffic Prediction Pipeline

This project implements a robust, automated MLOps pipeline for predicting traffic conditions. It leverages **Apache Airflow** for orchestration, **MLflow** for experiment tracking and model registry, **Evidently AI** for data drift detection, and **FastAPI** for serving predictions. The entire stack is containerized using **Docker Compose**.

## 🚀 Project Overview

The goal of this project is to predict traffic delays using real-time data fetched from the TomTom API. The system handles the complete lifecycle of machine learning:
1.  **Ingestion:** Fetches traffic data every 5 minutes.
2.  **ETL:** Cleans and labels raw data daily.
3.  **Training:** Retrains a Logistic Regression model daily on new data.
4.  **Monitoring:** Detects data drift; checks if the new model performs better than the current production model ("Champion vs. Challenger").
5.  **Serving:** Exposes the best model via a REST API.

## 🛠 Tech Stack

* **Orchestration:** Apache Airflow
* **Model Tracking & Registry:** MLflow
* **Model Serving:** FastAPI
* **Data Monitoring:** Evidently AI
* **Database:** PostgreSQL (for Airflow/MLflow metadata)
* **Containerization:** Docker & Docker Compose
* **Language:** Python 3.11+
* **Libraries:** Pandas, Scikit-learn, PyArrow

## 📂 Project Structure

```bash
├── configs/             # Configuration files (routes, database settings)
├── dags/                # Airflow DAGs
│   ├── fetch_5min.py         # High-frequency data fetching
│   ├── label_daily.py        # Daily data cleaning/labeling
│   └── traffic_train_daily.py # Training, Drift Check, Promotion
├── data/                # Local data storage (Simulated Data Lake)
│   ├── raw/             # Raw JSONL logs
│   ├── clean/           # Processed parquet files
│   ├── features/        # Training/Validation sets
│   └── monitoring/      # Drift reports
├── docker/              # Dockerfiles for specific services
├── src/                 # Source code for pipeline logic
│   ├── data/            # Connectors (TomTom/Demo) & Cleaning logic
│   ├── features/        # Feature engineering
│   ├── models/          # Train & Promote logic
│   ├── monitoring/      # Drift detection logic
│   └── serve/           # FastAPI application
├── docker-compose.yaml  # Infrastructure orchestration
└── requirements.txt     # Python dependencies
```
## ⚙️ Setup & Installation

### Prerequisites
* Docker Desktop / Docker Engine installed.
* TomTom API Key (optional if using the Demo provider).

### 1. Clone the Repository
```bash
git clone [https://github.com/your-username/mlops_project-on-traffic-data.git](https://github.com/your-username/mlops_project-on-traffic-data.git)
cd mlops_project-on-traffic-data
```

### 2. Configure Environment
Ensure your ```configs/routes.yaml``` is set up correctly. If you wish to use real data, add your TomTom API key in the configuration or source code environment variables.

### 3. Build and Run Services
Run the following command to build the images and start the containers (Airflow, Postgres, MLflow, API):

```bash
docker-compose up -d --build
```
Wait a few minutes for all services to initialize.


## 🖥️ Usage & Interfaces
Once the Docker containers are running, you can access the following interfaces:
| Service | URL | Description |
| :--- | :--- | :--- |
| **Apache Airflow** | `http://localhost:8080` | Manage pipelines and view logs (User/Pass: `admin`/`admin`) |
| **MLflow UI** | `http://localhost:5000` | View experiments, metrics, and registered models |
| **Prediction API** | `http://localhost:8000/docs` | Swagger UI for testing model inference |


## 🔄 The Pipelines (DAGs)
This project contains three primary Airflow DAGs:

1. ```traffic_fetch_5min```
  * Schedule: Every 5 minutes.

  * Function: Hits the Traffic API (TomTom or Demo), retrieves the current traffic situation for configured routes, and appends it to a raw JSONL file in data/raw.

2. ```traffic_label_daily```
  * Schedule: Daily.

  * Function: Consolidates the raw data collected over the last 24 hours. It cleans the data and saves it in efficient Parquet format in data/clean.

3. ```traffic_train_daily```
  * Schedule: Daily (After labeling).

  * Function:

Feature Engineering: Creates features like traffic_index and delay_seconds.

Training: Trains a new Logistic Regression model.

Drift Detection: Uses Evidently to compare the new data distribution against the training baseline.

Model Promotion: Compares the metrics (F1-score/Accuracy) of the new model against the current Production model. If the new model is better, it replaces the old one in the MLflow Model Registry.
