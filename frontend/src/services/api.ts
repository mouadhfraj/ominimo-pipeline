import { PipelineRun, PipelineStats, MetadataFile, HealthStatus, PipelineLog } from "@/types/pipeline";

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
      database: data.database,
      timestamp: data.timestamp,
    };
  },

  // Metadata
  async listMetadata(): Promise<MetadataFile[]> {
    const response = await fetch(`${API_BASE_URL}/metadata`);
    if (!response.ok) throw new Error("Failed to fetch metadata");
    const data = await response.json();
    return data.metadata_files;
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

  async deleteMetadata(name: string, hardDelete: boolean = false) {
    const response = await fetch(`${API_BASE_URL}/metadata/${name}?hard_delete=${hardDelete}`, {
      method: "DELETE",
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Failed to delete metadata");
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
    return response.json();
  },

  async listPipelineRuns(limit: number = 10): Promise<PipelineRun[]> {
    const response = await fetch(`${API_BASE_URL}/pipeline/runs?limit=${limit}`);
    if (!response.ok) throw new Error("Failed to fetch pipeline runs");
    const data = await response.json();
    return data.runs;
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
  async getLogs(pipelineId: string): Promise<PipelineLog[]> {
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
