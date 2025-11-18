

FROM python:3.10-bullseye


WORKDIR /app


RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"


COPY requirements.txt .


RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir \
    sqlalchemy==2.0.23 \
    psycopg2-binary==2.9.9 \
    alembic==1.12.1


COPY . .


RUN mkdir -p /app/data/input /app/data/output /app/logs /app/metadata && \
    chmod -R 777 /app/data /app/logs /app/metadata


ENV PYTHONPATH=/app


EXPOSE 8000


HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1


CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]