import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Badge } from "@/components/ui/badge";
import { FileJson, CheckCircle, AlertTriangle, Info } from "lucide-react";

const MetadataGuide = () => {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <FileJson className="h-5 w-5 text-primary" />
          Metadata Creation Guide
        </CardTitle>
        <CardDescription>
          Tips and rules for creating valid pipeline metadata files
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Accordion type="multiple" className="w-full">
          <AccordionItem value="structure">
            <AccordionTrigger>
              <div className="flex items-center gap-2">
                <Info className="h-4 w-4 text-primary" />
                <span>Basic Structure</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="space-y-3">
              <div className="bg-muted p-3 rounded-lg">
                <pre className="text-xs overflow-x-auto">
{`{
  "version": "1.0.0",
  "description": "Pipeline description",
  "dataflows": [{
    "name": "unique-pipeline-name",
    "description": "What this pipeline does",
    "version": "1.0.0",
    "sources": [...],
    "transformations": [...],
    "sinks": [...],
    "settings": {...}
  }]
}`}
                </pre>
              </div>
              <div className="space-y-2 text-sm">
                <div className="flex items-start gap-2">
                  <Badge variant="outline" className="mt-0.5">Required</Badge>
                  <span><strong>sources</strong> - Define where to read input data</span>
                </div>
                <div className="flex items-start gap-2">
                  <Badge variant="outline" className="mt-0.5">Required</Badge>
                  <span><strong>sinks</strong> - Define where to save output data</span>
                </div>
                <div className="flex items-start gap-2">
                  <Badge variant="secondary" className="mt-0.5">Optional</Badge>
                  <span><strong>transformations</strong> - Process and validate data</span>
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>

          <AccordionItem value="sources">
            <AccordionTrigger>
              <div className="flex items-center gap-2">
                <CheckCircle className="h-4 w-4 text-primary" />
                <span>Data Sources</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="space-y-3">
              <div className="bg-muted p-3 rounded-lg">
                <pre className="text-xs overflow-x-auto">
{`{
  "name": "raw_policies",
  "path": "/app/data/input/events/motor_policy/*",
  "format": "JSON",
  "options": { "multiLine": false }
}`}
                </pre>
              </div>
              <div className="space-y-2 text-sm">
                <p className="font-semibold">Supported Formats:</p>
                <div className="flex flex-wrap gap-2">
                  <Badge>JSON</Badge>
                  <Badge>CSV</Badge>
                  <Badge>PARQUET</Badge>
                  <Badge>AVRO</Badge>
                </div>
                <div className="flex items-start gap-2 mt-3">
                  <AlertTriangle className="h-4 w-4 text-warning mt-0.5" />
                  <div>
                    <p className="font-semibold">Path Rules:</p>
                    <ul className="list-disc list-inside mt-1 space-y-1">
                      <li>Must start with <code className="bg-background px-1 rounded">/app/ or /opt/airflow/ (for airflow execution)</code></li>
                      <li>Use wildcards for multiple files: <code className="bg-background px-1 rounded">*.json</code></li>
                      <li>Input directory: <code className="bg-background px-1 rounded">/app/data/input/</code></li>
                    </ul>
                  </div>
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>

          <AccordionItem value="validators">
            <AccordionTrigger>
              <div className="flex items-center gap-2">
                <CheckCircle className="h-4 w-4 text-primary" />
                <span>Available Validators</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="space-y-3">
              <div className="text-sm space-y-3">
                <div className="grid gap-2">
                  <div className="bg-muted p-2 rounded">
                    <Badge variant="outline" className="mb-1">notNull</Badge>
                    <p className="text-xs text-muted-foreground">Field cannot be null</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge variant="outline" className="mb-1">notEmpty</Badge>
                    <p className="text-xs text-muted-foreground">Field cannot be empty string</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge variant="outline" className="mb-1">isNumeric</Badge>
                    <p className="text-xs text-muted-foreground">Field must be numeric</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge variant="outline" className="mb-1">rangeCheck</Badge>
                    <p className="text-xs text-muted-foreground">Value within range (params: min, max)</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge variant="outline" className="mb-1">regex</Badge>
                    <p className="text-xs text-muted-foreground">Match pattern (params: pattern)</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge variant="outline" className="mb-1">inList</Badge>
                    <p className="text-xs text-muted-foreground">Value in allowed list (params: values[])</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge variant="outline" className="mb-1">email</Badge>
                    <p className="text-xs text-muted-foreground">Valid email format</p>
                  </div>
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>

          <AccordionItem value="transformations">
            <AccordionTrigger>
              <div className="flex items-center gap-2">
                <CheckCircle className="h-4 w-4 text-primary" />
                <span>Transformation Types</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="space-y-3">
              <div className="text-sm space-y-3">
                <div className="grid gap-2">
                  <div className="bg-muted p-2 rounded">
                    <Badge className="mb-1">validate_fields</Badge>
                    <p className="text-xs text-muted-foreground">Validate data quality with multiple validators</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge className="mb-1">add_fields</Badge>
                    <p className="text-xs text-muted-foreground">Add computed or constant fields (uuid, timestamp, hash, etc.)</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge className="mb-1">select_columns</Badge>
                    <p className="text-xs text-muted-foreground">Choose specific columns to keep</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge className="mb-1">deduplicate</Badge>
                    <p className="text-xs text-muted-foreground">Remove duplicate records based on subset of columns</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge className="mb-1">filter</Badge>
                    <p className="text-xs text-muted-foreground">Keep only rows matching SQL WHERE conditions</p>
                  </div>
                  <div className="bg-muted p-2 rounded">
                    <Badge className="mb-1">aggregate</Badge>
                    <p className="text-xs text-muted-foreground">Group and compute statistics (sum, avg, count, min, max)</p>
                  </div>
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>

          <AccordionItem value="functions">
            <AccordionTrigger>
              <div className="flex items-center gap-2">
                <CheckCircle className="h-4 w-4 text-primary" />
                <span>Add Field Functions</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="space-y-3">
              <div className="text-sm space-y-2">
                <div className="grid grid-cols-2 gap-2">
                  <Badge variant="outline">current_timestamp</Badge>
                  <Badge variant="outline">current_date</Badge>
                  <Badge variant="outline">uuid</Badge>
                  <Badge variant="outline">literal</Badge>
                  <Badge variant="outline">hash</Badge>
                  <Badge variant="outline">upper / lower / trim</Badge>
                  <Badge variant="outline">concat</Badge>
                  <Badge variant="outline">when</Badge>
                  <Badge variant="outline">year / month / day</Badge>
                </div>
                <div className="bg-muted p-3 rounded-lg mt-3">
                  <p className="font-semibold mb-2">Example:</p>
                  <pre className="text-xs overflow-x-auto">
{`{
  "name": "record_id",
  "function": "uuid"
},
{
  "name": "policy_hash",
  "function": "hash",
  "columns": ["policy_number"],
  "algorithm": "sha256"
}`}
                  </pre>
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>

          <AccordionItem value="sinks">
            <AccordionTrigger>
              <div className="flex items-center gap-2">
                <CheckCircle className="h-4 w-4 text-primary" />
                <span>Output Sinks</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="space-y-3">
              <div className="bg-muted p-3 rounded-lg">
                <pre className="text-xs overflow-x-auto">
{`{
  "input": "validated_data",
  "name": "parquet_output",
  "paths": ["/app/data/output/policies"],
  "format": "PARQUET",
  "saveMode": "APPEND",
  "partitionBy": ["ingestion_date"]
}`}
                </pre>
              </div>
              <div className="space-y-2 text-sm">
                <p className="font-semibold">Save Modes:</p>
                <div className="grid gap-2">
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">APPEND</Badge>
                    <span className="text-xs">Add to existing data</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">OVERWRITE</Badge>
                    <span className="text-xs">Replace all existing data</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">IGNORE</Badge>
                    <span className="text-xs">Skip if exists</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">ERROR</Badge>
                    <span className="text-xs">Fail if exists (default)</span>
                  </div>
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>

          <AccordionItem value="best-practices">
            <AccordionTrigger>
              <div className="flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 text-warning" />
                <span>Best Practices</span>
              </div>
            </AccordionTrigger>
            <AccordionContent>
              <div className="space-y-3 text-sm">
                <div className="flex items-start gap-2">
                  <CheckCircle className="h-4 w-4 text-success mt-0.5 shrink-0" />
                  <div>
                    <p className="font-semibold">Always validate critical fields</p>
                    <p className="text-muted-foreground text-xs">Use notNull and notEmpty for required fields</p>
                  </div>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle className="h-4 w-4 text-success mt-0.5 shrink-0" />
                  <div>
                    <p className="font-semibold">Add timestamps to track ingestion</p>
                    <p className="text-muted-foreground text-xs">Use current_timestamp function</p>
                  </div>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle className="h-4 w-4 text-success mt-0.5 shrink-0" />
                  <div>
                    <p className="font-semibold">Deduplicate before saving</p>
                    <p className="text-muted-foreground text-xs">Prevents duplicate records in output</p>
                  </div>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle className="h-4 w-4 text-success mt-0.5 shrink-0" />
                  <div>
                    <p className="font-semibold">Use partitioning for large datasets</p>
                    <p className="text-muted-foreground text-xs">Improves query performance</p>
                  </div>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle className="h-4 w-4 text-success mt-0.5 shrink-0" />
                  <div>
                    <p className="font-semibold">Set quality rules in settings</p>
                    <p className="text-muted-foreground text-xs">min_valid_record_percentage: 80</p>
                  </div>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle className="h-4 w-4 text-success mt-0.5 shrink-0" />
                  <div>
                    <p className="font-semibold">Save invalid records to separate sink</p>
                    <p className="text-muted-foreground text-xs">Helps debugging and monitoring</p>
                  </div>
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </CardContent>
    </Card>
  );
};

export default MetadataGuide;
