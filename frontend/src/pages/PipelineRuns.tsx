import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { 
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { PipelineRun } from "@/types/pipeline";
import { Search, Eye, XCircle, RefreshCw } from "lucide-react";
import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { api } from "@/services/api";
import { toast } from "sonner";

const PipelineRuns = () => {
  const [searchQuery, setSearchQuery] = useState("");
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchRuns = async () => {
    try {
      setLoading(true);
      const data = await api.listPipelineRuns(50);
      setRuns(data);
    } catch (error) {
      toast.error("Failed to fetch pipeline runs");
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRuns();
    const interval = setInterval(fetchRuns, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleCancel = async (pipelineId: string) => {
    try {
      await api.cancelPipeline(pipelineId);
      toast.success("Pipeline cancelled");
      fetchRuns();
    } catch (error) {
      toast.error("Failed to cancel pipeline");
    }
  };

  const filteredRuns = runs.filter(
    (run) =>
      run.pipeline_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
      run.metadata_path.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const formatDuration = (start: string, end?: string) => {
    if (!end) return "In progress";
    const duration = new Date(end).getTime() - new Date(start).getTime();
    const minutes = Math.floor(duration / 60000);
    const seconds = Math.floor((duration % 60000) / 1000);
    return `${minutes}m ${seconds}s`;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Pipeline Runs</h1>
          <p className="text-muted-foreground mt-1">
            View and manage all pipeline executions
          </p>
        </div>
        <Button onClick={fetchRuns} variant="outline" size="sm">
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>All Runs</CardTitle>
          <CardDescription>Complete history of pipeline executions</CardDescription>
          <div className="relative mt-4">
            <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search by pipeline ID or metadata file..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Pipeline ID</TableHead>
                <TableHead>Metadata File</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Start Time</TableHead>
                <TableHead>Duration</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredRuns.map((run) => (
                <TableRow key={run.pipeline_id}>
                  <TableCell className="font-mono text-sm">
                    {run.pipeline_id}
                  </TableCell>
                  <TableCell className="max-w-xs truncate">
                    {run.metadata_path}
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={run.status} />
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {new Date(run.start_time).toLocaleString()}
                  </TableCell>
                  <TableCell className="text-sm">
                    {formatDuration(run.start_time, run.end_time)}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Link to={`/logs?id=${run.pipeline_id}`}>
                        <Button variant="ghost" size="sm">
                          <Eye className="h-4 w-4" />
                        </Button>
                      </Link>
                      {run.status === "running" && (
                        <Button 
                          variant="ghost" 
                          size="sm"
                          onClick={() => handleCancel(run.pipeline_id)}
                        >
                          <XCircle className="h-4 w-4 text-destructive" />
                        </Button>
                      )}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
};

export default PipelineRuns;
