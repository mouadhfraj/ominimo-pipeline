export type PipelineStatus = "running" | "success" | "failed" | "pending" | "cancelled";

export interface PipelineRun {
  pipeline_id: string;
  metadata_name: string;
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
}

export interface PipelineStats {
  total_runs: number;
  successful_runs: number;
  failed_runs: number;
  running_pipelines: number;
  success_rate: number;
  avg_duration?: number;
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
  timestamp: string;
}

export interface PipelineLog {
  timestamp: string;
  level: string;
  stage?: string;
  message: string;
  details?: Record<string, any>;
}
