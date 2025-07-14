# 📦 Data Ingestion from S3 to RDS with Fallback to AWS Glue

This project demonstrates a **Dockerized Python application** that automates the process of:

- 📥 Reading a CSV file from an **Amazon S3 bucket**
- 🗃️ Inserting the data into a **MySQL-compatible Amazon RDS database**
- 🔁 If the RDS write fails, falling back to **AWS Glue** by:
  - Creating a table in the **Glue Data Catalog**
  - Registering the dataset from S3 for future use

---

## 🚀 Tech Stack & AWS Services

- **Python 3.9**
- **Docker**
- **AWS S3**
- **AWS RDS (MySQL)**
- **AWS Glue**
- **IAM** for secure programmatic access

---

## 🔧 Files in This Project

| File | Purpose |
|------|---------|
| `main.py` | Main Python script for data ingestion |
| `Dockerfile` | Defines the container environment |
| `requirements.txt` | Python dependencies |

---

## ⚙️ How to Run

Make sure you have Docker installed and AWS credentials ready.

Run the Docker container with environment variables:

```bash
docker run \
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  -e AWS_DEFAULT_REGION=ap-south-1 \
  -e S3_BUCKET=data-pipeline-samarth \
  -e CSV_KEY=sample_data.csv \
  -e RDS_HOST=mysql-rds.c9gy0wk24u3p.ap-south-1.rds.amazonaws.com \
  -e RDS_USER=ingestuser \
  -e RDS_PASSWORD=IngestPass123! \
  -e RDS_DB=ingestdb \
  -e RDS_TABLE=students \
  -e GLUE_DB=fallback_db \
  -e GLUE_TABLE=students_fallback \
  -e GLUE_S3_LOCATION=s3://data-pipeline-samarth/fallback/ \
  s3-to-rds-glue-app


