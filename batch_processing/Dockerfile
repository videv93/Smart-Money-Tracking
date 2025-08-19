FROM openjdk:11-jre-slim

WORKDIR /app

# Install Python and system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Set Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install Python dependencies
COPY batch_processing/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy JAR dependencies
RUN mkdir -p /app/jars
COPY jars/ /app/jars/

# Copy application code
COPY batch_processing/ ./batch_processing/
COPY configs/ ./configs/

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run the batch processing service
CMD ["python3", "batch_processing/smart_money_processor.py"]