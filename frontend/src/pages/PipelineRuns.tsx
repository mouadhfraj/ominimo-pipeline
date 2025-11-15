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
import { Search, Eye, XCircle } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";

// Mock data
const mockRuns: PipelineRun[] = [
  {
    pipeline_id: "pip-2024-001",
    metadata_path: "motor_insurance_config_v2.json",
    status: "running",
    start_time: "2024-01-15T10:30:00Z",
  },
  {
    pipeline_id: "pip-2024-002",
    metadata_path: "claims_processing_v1.json",
    status: "success",
    start_time: "2024-01-15T09:15:00Z",
    end_time: "2024-01-15T09:18:30Z",
  },
  {
    pipeline_id: "pip-2024-003",
    metadata_path: "premium_calculation_v3.json",
    status: "failed",
    start_time: "2024-01-15T08:45:00Z",
    end_time: "2024-01-15T08:47:15Z",
    error_message: "Validation error in dataflow step 3",
  },
  {
    pipeline_id: "pip-2024-004",
    metadata_path: "risk_assessment_v2.json",
    status: "success",
    start_time: "2024-01-15T08:00:00Z",
    end_time: "2024-01-15T08:02:45Z",
  },
  {
    pipeline_id: "pip-2024-005",
    metadata_path: "customer_segmentation_v1.json",
    status: "success",
    start_time: "2024-01-15T07:30:00Z",
    end_time: "2024-01-15T07:33:15Z",
  },
];

const PipelineRuns = () => {
  const [searchQuery, setSearchQuery] = useState("");

  const filteredRuns = mockRuns.filter(
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

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Pipeline Runs</h1>
        <p className="text-muted-foreground mt-1">
          View and manage all pipeline executions
        </p>
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
                        <Button variant="ghost" size="sm">
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
