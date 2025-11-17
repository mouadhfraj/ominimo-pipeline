import { PipelineRun, PipelineStats, MetadataFile, HealthStatus } from "@/types/pipeline";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export const api = {
  // Health & Root
  async getRoot() {
    const response = await fetch(`${API_BASE_URL}/`);
    if (!response.ok) throw new Error("Failed to fetch root");
    return response.json();
  },

  async getHealth(): Promise<HealthStatus> {
    const response = await fetch(`${API_BASE_URL}/health`);
    if (!response.ok) throw new Error("Failed to fetch health");
    const data = await response.json();
    return {
      status: data.status === "healthy" ? "healthy" : "unhealthy",
      active_pipelines: data.active_pipelines,
      uptime: 0, // Not provided by API
      memory_usage: undefined,
      cpu_usage: undefined,
    };
  },

  // Metadata
  async listMetadata(): Promise<MetadataFile[]> {
    const response = await fetch(`${API_BASE_URL}/metadata`);
    if (!response.ok) throw new Error("Failed to fetch metadata");
    const data = await response.json();
    return data.metadata_files.map((file: any) => ({
      name: file.name,
      path: file.path,
      size: 0, // Not provided by API
      created_at: new Date().toISOString(),
      version: file.version,
      description: file.description,
      dataflows: undefined,
    }));
  },

  async getMetadata(name: string) {
    const response = await fetch(`${API_BASE_URL}/metadata/${name}`);
    if (!response.ok) throw new Error(`Failed to fetch metadata: ${name}`);
    return response.json();
  },

  async uploadMetadata(file: File) {
    const formData = new FormData();
    formData.append("file", file);
    
    const response = await fetch(`${API_BASE_URL}/metadata/upload`, {
      method: "POST",
      body: formData,
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Failed to upload metadata");
    }
    
    return response.json();
  },

  // Pipeline Execution
  async runPipeline(metadataPath: string, asyncExecution: boolean = true) {
    const response = await fetch(`${API_BASE_URL}/pipeline/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        metadata_path: metadataPath,
        async_execution: asyncExecution,
      }),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Failed to run pipeline");
    }

    return response.json();
  },

  async getPipelineStatus(pipelineId: string): Promise<PipelineRun> {
    const response = await fetch(`${API_BASE_URL}/pipeline/status/${pipelineId}`);
    if (!response.ok) throw new Error(`Failed to fetch pipeline status: ${pipelineId}`);
    const data = await response.json();
    return {
      pipeline_id: data.pipeline_id,
      metadata_path: data.metadata_path,
      status: data.status,
      start_time: data.start_time,
      end_time: data.end_time,
      error_message: data.error,
    };
  },

  async listPipelineRuns(limit: number = 10): Promise<PipelineRun[]> {
    const response = await fetch(`${API_BASE_URL}/pipeline/runs?limit=${limit}`);
    if (!response.ok) throw new Error("Failed to fetch pipeline runs");
    const data = await response.json();
    return data.runs.map((run: any) => ({
      pipeline_id: run.pipeline_id,
      metadata_path: run.metadata_path,
      status: run.status,
      start_time: run.start_time,
      end_time: run.end_time,
      error_message: run.error,
    }));
  },

  async cancelPipeline(pipelineId: string) {
    const response = await fetch(`${API_BASE_URL}/pipeline/${pipelineId}`, {
      method: "DELETE",
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Failed to cancel pipeline");
    }

    return response.json();
  },

  // Logs
  async getLogs(pipelineId: string): Promise<string> {
    const response = await fetch(`${API_BASE_URL}/logs/${pipelineId}`);
    if (!response.ok) throw new Error(`Failed to fetch logs: ${pipelineId}`);
    const data = await response.json();
    return data.logs;
  },

  // Statistics
  async getStats(): Promise<PipelineStats> {
    const response = await fetch(`${API_BASE_URL}/stats`);
    if (!response.ok) throw new Error("Failed to fetch statistics");
    return response.json();
  },
};
