FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY blockchain_ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY blockchain_ingestion/ ./blockchain_ingestion/
COPY configs/ ./configs/

# Create directory for logs
RUN mkdir -p /app/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run the blockchain ingestion service
CMD ["python", "blockchain_ingestion/ethereum_client.py"]