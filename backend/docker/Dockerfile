FROM apache/spark:3.5.0-python3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data/input /app/data/output

# Set environment variables
ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Expose ports
EXPOSE 8000 8080 8793

# Default command
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]