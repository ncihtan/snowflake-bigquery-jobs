FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y gcc libpq-dev build-essential && apt-get clean

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy common and job-specific code
ARG JOB_NAME
COPY common ./common
COPY jobs/${JOB_NAME} ./job

WORKDIR /app/job
ENTRYPOINT ["python", "main.py"]