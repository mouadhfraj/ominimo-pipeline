import { Button } from "@/components/ui/button";
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
import { useState } from "react";
import { toast } from "sonner";

const mockLogs = `
[2024-01-15 10:30:15] INFO: Pipeline pip-2024-001 started
[2024-01-15 10:30:16] INFO: Loading metadata: motor_insurance_config_v2.json
[2024-01-15 10:30:17] INFO: Validating metadata structure
[2024-01-15 10:30:17] INFO: Found 5 dataflows to process
[2024-01-15 10:30:18] INFO: Starting dataflow: customer_data_ingestion
[2024-01-15 10:30:20] INFO: Processing 10,000 records
[2024-01-15 10:30:25] INFO: Dataflow customer_data_ingestion completed successfully
[2024-01-15 10:30:26] INFO: Starting dataflow: risk_calculation
[2024-01-15 10:30:28] INFO: Applying risk assessment rules
[2024-01-15 10:30:32] INFO: Risk scores calculated for 10,000 customers
[2024-01-15 10:30:33] INFO: Starting dataflow: premium_adjustment
[2024-01-15 10:30:35] INFO: Applying premium calculation formulas
[2024-01-15 10:30:40] INFO: Premium adjustments completed
[2024-01-15 10:30:41] INFO: Starting dataflow: validation_checks
[2024-01-15 10:30:43] INFO: Running data quality checks
[2024-01-15 10:30:45] INFO: All validation checks passed
[2024-01-15 10:30:46] INFO: Starting dataflow: output_generation
[2024-01-15 10:30:48] INFO: Writing results to STANDARD_OK
[2024-01-15 10:30:50] INFO: Output files generated successfully
[2024-01-15 10:30:51] INFO: Pipeline pip-2024-001 completed successfully
[2024-01-15 10:30:51] INFO: Total execution time: 36 seconds
`.trim();

const mockPipelineIds = [
  "pip-2024-001",
  "pip-2024-002",
  "pip-2024-003",
  "pip-2024-004",
];

const Logs = () => {
  const [selectedPipeline, setSelectedPipeline] = useState("pip-2024-001");
  const [searchQuery, setSearchQuery] = useState("");

  const filteredLogs = mockLogs
    .split("\n")
    .filter((line) => line.toLowerCase().includes(searchQuery.toLowerCase()))
    .join("\n");

  const handleDownload = () => {
    toast.success(`Downloading logs for ${selectedPipeline}`);
  };

  const handleRefresh = () => {
    toast.success("Logs refreshed");
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
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {mockPipelineIds.map((id) => (
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
            <pre className="text-xs font-mono whitespace-pre-wrap break-words max-h-[600px] overflow-y-auto">
              {filteredLogs || "No logs match your search criteria"}
            </pre>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Logs;
