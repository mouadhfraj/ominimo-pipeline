import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { 
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { PlayCircle, FileJson, Settings, RefreshCw } from "lucide-react";
import { useState, useEffect } from "react";
import { toast } from "sonner";
import { useNavigate } from "react-router-dom";
import { api } from "@/services/api";
import { MetadataFile } from "@/types/pipeline";

const ExecutePipeline = () => {
  const navigate = useNavigate();
  const [selectedMetadata, setSelectedMetadata] = useState("");
  const [asyncExecution, setAsyncExecution] = useState(true);
  const [parameters, setParameters] = useState("");
  const [metadataFiles, setMetadataFiles] = useState<MetadataFile[]>([]);
  const [loading, setLoading] = useState(false);
  const [executing, setExecuting] = useState(false);

  useEffect(() => {
    fetchMetadata();
  }, []);

  const fetchMetadata = async () => {
    try {
      setLoading(true);
      const data = await api.listMetadata();
      setMetadataFiles(data);
    } catch (error) {
      toast.error("Failed to fetch metadata files");
    } finally {
      setLoading(false);
    }
  };

  const handleExecute = async () => {
    if (!selectedMetadata) {
      toast.error("Please select a metadata file");
      return;
    }

    try {
      setExecuting(true);
      const result = await api.runPipeline(selectedMetadata, asyncExecution);
      
      toast.success(
        asyncExecution 
          ? `Pipeline queued: ${result.pipeline_id}` 
          : "Pipeline completed successfully"
      );

      setTimeout(() => {
        navigate(`/logs?id=${result.pipeline_id}`);
      }, 1000);
    } catch (error: any) {
      toast.error(error.message || "Failed to execute pipeline");
    } finally {
      setExecuting(false);
    }
  };

  return (
    <div className="space-y-6 max-w-3xl">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Execute Pipeline</h1>
        <p className="text-muted-foreground mt-1">
          Trigger a new pipeline run with selected metadata configuration
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <FileJson className="h-5 w-5 text-primary" />
            Metadata Configuration
          </CardTitle>
          <CardDescription>
            Select the metadata file to use for this pipeline execution
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="space-y-2">
            <Label htmlFor="metadata">Metadata File</Label>
            <Select value={selectedMetadata} onValueChange={setSelectedMetadata}>
              <SelectTrigger id="metadata">
                <SelectValue placeholder="Select a metadata file" />
              </SelectTrigger>
              <SelectContent>
                {metadataFiles.map((file) => (
                  <SelectItem key={file.path} value={file.path}>
                    {file.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              Choose the configuration file that defines the pipeline dataflows
            </p>
          </div>

          <div className="space-y-2">
            <Label htmlFor="parameters">Additional Parameters (Optional)</Label>
            <Textarea
              id="parameters"
              placeholder='{"batch_size": 1000, "parallel_jobs": 4}'
              value={parameters}
              onChange={(e) => setParameters(e.target.value)}
              className="font-mono text-sm min-h-[100px]"
            />
            <p className="text-xs text-muted-foreground">
              JSON format parameters to override default configuration
            </p>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings className="h-5 w-5 text-primary" />
            Execution Options
          </CardTitle>
          <CardDescription>
            Configure how the pipeline should be executed
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5">
              <Label htmlFor="async-mode">Asynchronous Execution</Label>
              <p className="text-xs text-muted-foreground">
                Run pipeline in background and return immediately
              </p>
            </div>
            <Switch
              id="async-mode"
              checked={asyncExecution}
              onCheckedChange={setAsyncExecution}
            />
          </div>

          <div className="pt-4 border-t border-border">
            <Button 
              size="lg" 
              className="w-full gap-2"
              onClick={handleExecute}
              disabled={executing || loading}
            >
              {executing ? (
                <>
                  <RefreshCw className="h-5 w-5 animate-spin" />
                  Executing...
                </>
              ) : (
                <>
                  <PlayCircle className="h-5 w-5" />
                  Execute Pipeline
                </>
              )}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-accent/50 border-accent">
        <CardContent className="pt-6">
          <div className="flex gap-4">
            <div className="flex-shrink-0">
              <div className="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center">
                <PlayCircle className="h-5 w-5 text-primary" />
              </div>
            </div>
            <div className="space-y-1">
              <p className="font-medium">Pipeline Execution Tips</p>
              <ul className="text-sm text-muted-foreground space-y-1">
                <li>• Ensure metadata file is validated before execution</li>
                <li>• Use async mode for long-running pipelines</li>
                <li>• Monitor logs in real-time for debugging</li>
                <li>• Check health status before starting critical jobs</li>
              </ul>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default ExecutePipeline;
