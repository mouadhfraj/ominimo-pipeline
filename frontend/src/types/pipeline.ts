export type PipelineStatus = "running" | "success" | "failed" | "pending" | "cancelled" | "queued" | "queued_airflow";
export type ExecutionMethod = "direct" | "airflow";

export interface PipelineRun {
  pipeline_id: string;
  metadata_name: string;
  execution_method?: ExecutionMethod;
  status: PipelineStatus;
  start_time: string;
  end_time?: string;
  duration_seconds?: number;
  total_records?: number;
  valid_records?: number;
  invalid_records?: number;
  valid_percentage?: number;
  error_message?: string;
  stages?: Record<string, any>;
  log_count?: number;
}

export interface PipelineStats {
  total_runs: number;
  successful_runs: number;
  failed_runs: number;
  running_pipelines: number;
  success_rate: number;
  avg_duration?: number;
  execution_methods?: {
    direct: {
      total: number;
      successful: number;
      failed: number;
    };
    airflow: {
      total: number;
      successful: number;
      failed: number;
    };
  };
  active_metadata_files?: number;
}

export interface MetadataFile {
  name: string;
  version?: string;
  description?: string;
  created_at: string;
  updated_at: string;
  is_active: boolean;
}

export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  active_pipelines: number;
  database?: string;
  airflow?: string;
  timestamp: string;
}

export interface PipelineLog {
  timestamp: string;
  level: string;
  stage?: string;
  message: string;
  details?: Record<string, any>;
}

export interface AirflowDAG {
  dag_id: string;
  is_paused: boolean;
  is_active: boolean;
  description?: string;
  tags: string[];
}

export interface PipelineStage {
  name: string;
  status: string;
  start_time?: string;
  end_time?: string;
  data: Record<string, any>;
  logs: Array<{
    timestamp: string;
    level: string;
    message: string;
  }>;
}
