
-- METADATA FILES

CREATE TABLE IF NOT EXISTS metadata_files (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    version VARCHAR(50) NOT NULL,
    description TEXT,
    content JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Update trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER update_metadata_files_updated_at
BEFORE UPDATE ON metadata_files
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();



-- PIPELINE RUNS

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) UNIQUE NOT NULL,
    metadata_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (
        status IN ('queued', 'queued_airflow', 'running', 'success', 'failed', 'cancelled')
    ),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds FLOAT,
    total_records INTEGER DEFAULT 0,
    valid_records INTEGER DEFAULT 0,
    invalid_records INTEGER DEFAULT 0,
    valid_percentage FLOAT,
    stages JSONB,
    error_message TEXT,

    FOREIGN KEY (metadata_name)
        REFERENCES metadata_files(name)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_id ON pipeline_runs(pipeline_id);



-- PIPELINE LOGS

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(20) NOT NULL CHECK (
        level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    ),
    stage VARCHAR(100),
    message TEXT NOT NULL,
    details JSONB,

    FOREIGN KEY (pipeline_id)
        REFERENCES pipeline_runs(pipeline_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_logs_pipeline ON pipeline_logs(pipeline_id);
