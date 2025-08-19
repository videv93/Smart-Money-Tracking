FROM flink:1.17.1-scala_2.12-java11

USER root

# Install Python and system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY stream_processing/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy JAR dependencies to Flink lib directory
COPY jars/ $FLINK_HOME/lib/

# Copy application code
COPY stream_processing/ ./stream_processing/
COPY configs/ ./configs/

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Switch back to flink user
USER flink

# Run the stream processing service
CMD ["python3", "stream_processing/smart_money_stream.py"]