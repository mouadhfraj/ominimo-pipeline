import { StatCard } from "@/components/StatCard";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { 
  Activity, 
  Cpu, 
  HardDrive, 
  Clock,
  CheckCircle2,
  Server,
  RefreshCw
} from "lucide-react";
import { HealthStatus } from "@/types/pipeline";
import { useState, useEffect } from "react";
import { api } from "@/services/api";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";

const Health = () => {
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchHealth = async () => {
    try {
      setLoading(true);
      const data = await api.getHealth();
      setHealth(data);
    } catch (error) {
      toast.error("Failed to fetch health status");
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHealth();
    const interval = setInterval(fetchHealth, 10000);
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!health) return null;

  const getStatusColor = (status: string) => {
    switch (status) {
      case "healthy":
        return "success";
      case "degraded":
        return "warning";
      case "unhealthy":
        return "destructive";
      default:
        return "default";
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">System Health</h1>
          <p className="text-muted-foreground mt-1">
            Monitor pipeline infrastructure and performance metrics
          </p>
        </div>
        <div className="flex gap-2 items-center">
          <Button onClick={fetchHealth} variant="outline" size="sm">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
          <Badge variant={getStatusColor(health.status)} className="text-lg px-4 py-2">
            <CheckCircle2 className="h-4 w-4 mr-2" />
            {health.status.toUpperCase()}
          </Badge>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <StatCard
          title="System Status"
          value={health.status}
          icon={Activity}
          description="Overall system health"
        />
        <StatCard
          title="Active Pipelines"
          value={health.active_pipelines.toString()}
          icon={Cpu}
          description="Currently running"
        />
        <StatCard
          title="Database"
          value={health.database === "healthy" ? "Connected" : "Error"}
          icon={CheckCircle2}
          description="Database status"
        />
      </div>

      {/* Database Status */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Server className="h-5 w-5 text-primary" />
            Database Connection
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-muted-foreground">Connection Status</p>
              <p className="text-lg font-semibold mt-1">
                {health.database === "healthy" ? "Connected" : health.database}
              </p>
            </div>
            <Badge variant={health.database === "healthy" ? "default" : "destructive"}>
              {health.database === "healthy" ? "Active" : "Error"}
            </Badge>
          </div>
        </CardContent>
      </Card>

      {/* Service Components */}
      <Card>
        <CardHeader>
          <CardTitle>Service Components</CardTitle>
          <CardDescription>Individual service status overview</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <div className="flex items-center justify-between p-3 rounded-lg border">
              <span className="text-sm font-medium">API Server</span>
              <Badge variant="success">Active</Badge>
            </div>
            <div className="flex items-center justify-between p-3 rounded-lg border">
              <span className="text-sm font-medium">Database</span>
              <Badge variant={health.database === "healthy" ? "success" : "destructive"}>
                {health.database === "healthy" ? "Active" : "Error"}
              </Badge>
            </div>
            <div className="flex items-center justify-between p-3 rounded-lg border">
              <span className="text-sm font-medium">Pipeline Engine</span>
              <Badge variant="success">Active</Badge>
            </div>
            <div className="flex items-center justify-between p-3 rounded-lg border">
              <span className="text-sm font-medium">Log Service</span>
              <Badge variant="success">Active</Badge>
            </div>
            <div className="flex items-center justify-between p-3 rounded-lg border">
              <span className="text-sm font-medium">Metadata Store</span>
              <Badge variant="success">Active</Badge>
            </div>
            <div className="flex items-center justify-between p-3 rounded-lg border">
              <span className="text-sm font-medium">Monitoring</span>
              <Badge variant="success">Active</Badge>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Health;
