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
import { MetadataFile } from "@/types/pipeline";
import { FileJson, Upload, Download, Eye, Trash2, Search } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

// Mock data
const mockMetadata: MetadataFile[] = [
  {
    name: "motor_insurance_config_v2.json",
    path: "/metadata/motor_insurance_config_v2.json",
    size: 15420,
    created_at: "2024-01-10T14:30:00Z",
    version: "2.0",
    dataflows: 5,
  },
  {
    name: "claims_processing_v1.json",
    path: "/metadata/claims_processing_v1.json",
    size: 8930,
    created_at: "2024-01-08T10:15:00Z",
    version: "1.0",
    dataflows: 3,
  },
  {
    name: "premium_calculation_v3.json",
    path: "/metadata/premium_calculation_v3.json",
    size: 12340,
    created_at: "2024-01-05T16:45:00Z",
    version: "3.0",
    dataflows: 4,
  },
];

const Metadata = () => {
  const [searchQuery, setSearchQuery] = useState("");
  const [metadata] = useState<MetadataFile[]>(mockMetadata);

  const filteredMetadata = metadata.filter((file) =>
    file.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const formatFileSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const handleUpload = () => {
    toast.info("Upload functionality will be implemented with backend API");
  };

  const handleDownload = (file: MetadataFile) => {
    toast.success(`Downloading ${file.name}`);
  };

  const handleDelete = (file: MetadataFile) => {
    toast.success(`Deleted ${file.name}`);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Metadata Management</h1>
          <p className="text-muted-foreground mt-1">
            Upload and manage pipeline configuration files
          </p>
        </div>
        <Button onClick={handleUpload} className="gap-2">
          <Upload className="h-4 w-4" />
          Upload Metadata
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Metadata Files</CardTitle>
          <CardDescription>Pipeline configuration JSON files</CardDescription>
          <div className="relative mt-4">
            <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search metadata files..."
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
                <TableHead>Name</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Dataflows</TableHead>
                <TableHead>Size</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredMetadata.map((file) => (
                <TableRow key={file.path}>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <FileJson className="h-4 w-4 text-primary" />
                      <span className="font-medium">{file.name}</span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <span className="text-sm text-muted-foreground">
                      v{file.version}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="text-sm">{file.dataflows} flows</span>
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {formatFileSize(file.size)}
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {new Date(file.created_at).toLocaleDateString()}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Button variant="ghost" size="sm">
                        <Eye className="h-4 w-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => handleDownload(file)}
                      >
                        <Download className="h-4 w-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => handleDelete(file)}
                      >
                        <Trash2 className="h-4 w-4 text-destructive" />
                      </Button>
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

export default Metadata;
