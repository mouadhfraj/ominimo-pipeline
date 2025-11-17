export type PipelineStatus = "running" | "success" | "failed" | "pending" | "cancelled";

export interface PipelineRun {
  pipeline_id: string;
  metadata_path: string;
  status: PipelineStatus;
  start_time: string;
  end_time?: string;
  error_message?: string;
  stages?: {
    name: string;
    status: PipelineStatus;
    duration?: number;
  }[];
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
  path: string;
  size: number;
  created_at: string;
  version?: string;
  description?: string;
  dataflows?: number;
}

export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  active_pipelines: number;
  uptime: number;
  memory_usage?: number;
  cpu_usage?: number;
}
