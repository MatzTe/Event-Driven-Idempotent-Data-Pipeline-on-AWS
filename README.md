# Containerized Serverless Data Ingestion Pipeline

A modular, idempotent data ingestion pipeline built with AWS Lambda using a Docker container image stored in Amazon ECR.

This project demonstrates production-style serverless architecture with data normalization, schema validation, row-level validation, and content-based idempotency.

---

## 🚀 Architecture

S3 Upload  
→ Lambda (Container Image)  
→ Download File  
→ Content Hash (MD5)  
→ Normalize Data  
→ Validate Schema  
→ Split Valid / Invalid  
→ Upload Results to S3  

---

## 🐳 Container-Based Deployment

Due to dependency size (Pandas + scientific stack), this Lambda is deployed using a Docker image instead of a ZIP package.

The image is:

- Built locally using Docker
- Pushed to Amazon ECR
- Referenced by AWS Lambda as its runtime image

---

## 🛠 Tech Stack

- Python 3.10+
- AWS Lambda (Container Image)
- Amazon S3
- Amazon ECR
- Pandas
- Boto3
- Docker

---

## 📂 Project Structure

serverless-data-ingestion-pipeline/
│
├── src/
│   ├── lambda_handler.py
│   ├── storage.py
│   ├── cleaning.py
│   ├── validation.py
│   └── utils.py
│
├── tests/
│
├── Dockerfile
├── requirements.txt
├── README.md
└── .gitignore

## 🔐 Idempotency Strategy

Each uploaded file is hashed using MD5 before processing.

If a file with the same content has already been processed,
the pipeline prevents duplicate execution by checking the hash-based output key.

---

## 🧠 Design Principles

- Clean separation of concerns
- Fail-fast schema validation
- In-memory processing (no disk writes)
- Content-based deduplication
- Containerized serverless deployment

## 🐳 Build and Push to Amazon ECR

1. Authenticate Docker with AWS:

aws ecr get-login-password --region <region> \
| docker login \
--username AWS \
--password-stdin <account-id>.dkr.ecr.<region>.amazonaws.com

2. Build the image:

docker build -t data-ingestion-lambda .

3. Tag the image:

docker tag data-ingestion-lambda:latest \
<account-id>.dkr.ecr.<region>.amazonaws.com/data-ingestion-lambda:latest

4. Push the image:

docker push <account-id>.dkr.ecr.<region>.amazonaws.com/data-ingestion-lambda:latest

5. Update the Lambda function to use the new image version.

## 🔔 S3 Trigger Configuration

The Lambda function is triggered automatically when a new CSV file
is uploaded to a designated S3 bucket.

Trigger type: S3 Event Notification  
Event: ObjectCreated (Put)  
Filter: *.csv  

This enables fully automated ingestion without manual execution.

## 🏗️ Architecture Diagram
mermaid
flowchart TD

    A[S3 Bucket<br>CSV Upload] -->|ObjectCreated Event| B[Lambda Function<br>Container Image]

    B --> C[Download File from S3]
    C --> D[Calculate MD5 Hash]

    D --> E{Already Processed?}

    E -- Yes --> F[Skip Processing]
    E -- No --> G[Normalize Data]

    G --> H[Validate Schema]
    H --> I[Row-level Validation]

    I --> J[Valid Records]
    I --> K[Invalid Records]

    J --> L[Upload to Processed/]
    K --> M[Upload to Errors/]
