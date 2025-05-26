# 📁 Bulk Candidate Upload Service

A FastAPI-based microservice to handle **bulk candidate file uploads** (ZIP, PDF, DOCX), extract and process them asynchronously using **Dramatiq** with **Redis** as a message broker.

---

## 🚀 Features

- Upload multiple candidate files via a single endpoint.
- Supports `.zip`, `.pdf`, and `.docx` formats.
- Asynchronous file extraction and processing via **Dramatiq**.
- Prometheus metrics tracking for observability.
- Secure file handling with role-based permissions.
- Decoupled background task system for scalability and performance.

---

## 🧰 Tech Stack

- **Python**: 3.10.8
- **FastAPI**: RESTful API framework
- **Dramatiq
