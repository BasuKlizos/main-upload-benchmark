# 📂 Bulk Candidate File Uploader API

This is a scalable, asynchronous bulk candidate file uploader built with **FastAPI**, **Dramatiq**, and **Docker Compose**. It supports background processing of ZIP/PDF/DOCX files, stores metadata in MongoDB, and exposes Prometheus-compatible metrics.

---

## 🚀 Features

- Upload multiple candidate files in bulk.
- Supports `.pdf`, `.docx`, and `.zip` file formats.
- Background extraction and processing using Dramatiq workers.
- Upload tracking, validation, and metrics collection.
- Prometheus PushGateway integration.
- Asynchronous, scalable architecture using Redis-backed queues.

---

## 🧰 Tech Stack

- **FastAPI** – Modern Python web framework for APIs.
- **Dramatiq** – Background task processing.
- **Redis** – Queue broker for Dramatiq.
- **MongoDB** – Document-based NoSQL database.
- **Prometheus PushGateway** – Metrics tracking and monitoring.
- **Docker Compose** – Multi-service orchestration.

---

## Python Version

- **Python 3.10.8**

---

## 🐳 Getting Started with Docker

> Ensure you have **Docker** and **Docker Compose** installed.

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/BasuKlizos/main-upload-benchmark.git
cd main-upload-benchmark

### 2️⃣ Build and Start the Application

docker-compose up --build

### 3️⃣ Stop the Application

docker-compose down


## API Endpoints

### 1. Upload Bulk Candidate Files

- **URL:** `/bulk`
- **Method:** `POST`
- **Description:** Upload multiple candidate files in bulk (`.pdf`, `.docx`, `.zip` supported).
- **Example:**  
  `http://localhost:8000/bulk`

---

## Monitoring & Service URLs

| Service                  | URL                          | Description                          |
|--------------------------|------------------------------|------------------------------------|
| **Grafana Dashboard**    | http://localhost:3000         | Monitoring dashboard (default creds) |
| **Redis**                | http://localhost:3001         | Queue broker for Dramatiq            |
| **Prometheus PushGateway**| http://localhost:9091        | Metrics tracking and monitoring      |
| **Prometheus Dashboard** | http://localhost:9090         | Prometheus monitoring dashboard      |

---

## Notes

- Make sure all services are running (via Docker Compose) to use these endpoints effectively.
- The upload endpoint accepts files asynchronously processed in the background.
- Metrics endpoints are useful for monitoring system health and performance.

---

