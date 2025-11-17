# Ominimo Pipeline - Working Dockerfile
# Tested and compatible with all requirements

FROM python:3.10-bullseye

# Set working directory
WORKDIR /app

# Install system dependencies (Java 11 is available in bullseye)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data/input /app/data/output /app/logs /app/metadata && \
    chmod -R 777 /app/data /app/logs /app/metadata

# Set Python path
ENV PYTHONPATH=/app

# Expose FastAPI port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command: Run FastAPI
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]