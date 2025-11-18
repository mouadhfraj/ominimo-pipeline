import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { FileText, Download, Search, RefreshCw } from "lucide-react";
import { useState, useEffect } from "react";
import { toast } from "sonner";
import { api } from "@/services/api";
import { useSearchParams } from "react-router-dom";
import { PipelineLog } from "@/types/pipeline";

const Logs = () => {
  const [searchParams] = useSearchParams();
  const [selectedPipeline, setSelectedPipeline] = useState(searchParams.get("id") || "");
  const [searchQuery, setSearchQuery] = useState("");
  const [logs, setLogs] = useState<PipelineLog[]>([]);
  const [pipelineIds, setPipelineIds] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchPipelineIds();
  }, []);

  useEffect(() => {
    if (selectedPipeline) {
      fetchLogs();
    }
  }, [selectedPipeline]);

  const fetchPipelineIds = async () => {
    try {
      const runs = await api.listPipelineRuns(50);
      setPipelineIds(runs.map(run => run.pipeline_id));
      if (!selectedPipeline && runs.length > 0) {
        setSelectedPipeline(runs[0].pipeline_id);
      }
    } catch (error) {
      toast.error("Failed to fetch pipeline IDs");
    }
  };

  const fetchLogs = async () => {
    if (!selectedPipeline) return;
    try {
      setLoading(true);
      const logData = await api.getLogs(selectedPipeline);
      setLogs(logData);
    } catch (error) {
      toast.error("Failed to fetch logs");
      setLogs([]);
    } finally {
      setLoading(false);
    }
  };

  const filteredLogs = logs.filter((log) =>
    log.message.toLowerCase().includes(searchQuery.toLowerCase()) ||
    log.level.toLowerCase().includes(searchQuery.toLowerCase()) ||
    (log.stage && log.stage.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  const handleDownload = () => {
    const logText = logs
      .map((log) => `[${log.timestamp}] [${log.level}] ${log.stage ? `[${log.stage}] ` : ""}${log.message}`)
      .join("\n");
    const blob = new Blob([logText], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pipeline-${selectedPipeline}-logs.txt`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success("Logs downloaded successfully");
  };

  const getLevelColor = (level: string) => {
    switch (level.toUpperCase()) {
      case "ERROR":
      case "CRITICAL":
        return "destructive";
      case "WARNING":
        return "warning";
      case "INFO":
        return "default";
      default:
        return "secondary";
    }
  };

  const handleRefresh = () => {
    fetchLogs();
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Pipeline Logs</h1>
        <p className="text-muted-foreground mt-1">
          View detailed execution logs for troubleshooting and monitoring
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5 text-primary" />
            Log Viewer
          </CardTitle>
          <CardDescription>
            Real-time and historical pipeline execution logs
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="pipeline">Pipeline ID</Label>
              <Select value={selectedPipeline} onValueChange={setSelectedPipeline}>
                <SelectTrigger id="pipeline">
                  <SelectValue placeholder="Select a pipeline" />
                </SelectTrigger>
                <SelectContent>
                  {pipelineIds.map((id) => (
                    <SelectItem key={id} value={id}>
                      {id}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="search">Search Logs</Label>
              <div className="relative">
                <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
                <Input
                  id="search"
                  placeholder="Filter log entries..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
          </div>

          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={handleRefresh} className="gap-2">
              <RefreshCw className="h-4 w-4" />
              Refresh
            </Button>
            <Button variant="outline" size="sm" onClick={handleDownload} className="gap-2">
              <Download className="h-4 w-4" />
              Download
            </Button>
          </div>

          <div className="rounded-lg border border-border bg-muted/30 p-4">
            {loading ? (
              <div className="flex items-center justify-center p-8">
                <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : (
              <div className="space-y-2">
                {filteredLogs.length === 0 ? (
                  <p className="text-muted-foreground text-center py-8">No logs available</p>
                ) : (
                  filteredLogs.map((log, index) => (
                    <div
                      key={index}
                      className="bg-muted/50 p-3 rounded-lg border border-border hover:bg-muted transition-colors"
                    >
                      <div className="flex items-start gap-3">
                        <Badge variant={getLevelColor(log.level)} className="mt-0.5">
                          {log.level}
                        </Badge>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-xs text-muted-foreground">
                              {new Date(log.timestamp).toLocaleString()}
                            </span>
                            {log.stage && (
                              <Badge variant="outline" className="text-xs">
                                {log.stage}
                              </Badge>
                            )}
                          </div>
                          <p className="text-sm break-words">{log.message}</p>
                          {log.details && (
                            <pre className="mt-2 text-xs bg-background p-2 rounded overflow-auto">
                              {JSON.stringify(log.details, null, 2)}
                            </pre>
                          )}
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Logs;
