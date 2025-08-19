FROM python:3.9

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY model_experiment/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY model_experiment/ ./model_experiment/
COPY configs/ ./configs/

# Create directories
RUN mkdir -p /app/models /app/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run the ML training service
CMD ["python", "model_experiment/smart_money_model.py"]