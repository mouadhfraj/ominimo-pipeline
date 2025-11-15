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
  Server
} from "lucide-react";
import { HealthStatus } from "@/types/pipeline";

const mockHealth: HealthStatus = {
  status: "healthy",
  active_pipelines: 3,
  uptime: 345600, // seconds (4 days)
  memory_usage: 45,
  cpu_usage: 32,
};

const Health = () => {
  const formatUptime = (seconds: number) => {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${days}d ${hours}h ${minutes}m`;
  };

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
        <Badge variant={getStatusColor(mockHealth.status)} className="text-lg px-4 py-2">
          <CheckCircle2 className="h-4 w-4 mr-2" />
          {mockHealth.status.toUpperCase()}
        </Badge>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="System Status"
          value={mockHealth.status}
          icon={CheckCircle2}
          variant="success"
          description="All systems operational"
        />
        <StatCard
          title="Active Pipelines"
          value={mockHealth.active_pipelines}
          icon={Activity}
          variant="info"
          description="Currently running"
        />
        <StatCard
          title="Uptime"
          value={formatUptime(mockHealth.uptime)}
          icon={Clock}
          description="System availability"
        />
        <StatCard
          title="API Status"
          value="Operational"
          icon={Server}
          variant="success"
          description="Responding normally"
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Cpu className="h-5 w-5 text-primary" />
              CPU Usage
            </CardTitle>
            <CardDescription>Processor utilization metrics</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Current Usage</span>
                <span className="font-bold text-2xl">{mockHealth.cpu_usage}%</span>
              </div>
              <Progress value={mockHealth.cpu_usage} className="h-3" />
              <p className="text-xs text-muted-foreground">
                {mockHealth.cpu_usage! < 50 ? "Normal" : mockHealth.cpu_usage! < 80 ? "Moderate" : "High"} load
              </p>
            </div>
            <div className="grid grid-cols-3 gap-4 pt-4 border-t border-border">
              <div>
                <p className="text-xs text-muted-foreground">1 min avg</p>
                <p className="text-lg font-semibold">28%</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">5 min avg</p>
                <p className="text-lg font-semibold">32%</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">15 min avg</p>
                <p className="text-lg font-semibold">35%</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <HardDrive className="h-5 w-5 text-primary" />
              Memory Usage
            </CardTitle>
            <CardDescription>RAM allocation and availability</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Current Usage</span>
                <span className="font-bold text-2xl">{mockHealth.memory_usage}%</span>
              </div>
              <Progress value={mockHealth.memory_usage} className="h-3" />
              <p className="text-xs text-muted-foreground">
                {mockHealth.memory_usage! < 60 ? "Healthy" : mockHealth.memory_usage! < 85 ? "Moderate" : "Critical"} allocation
              </p>
            </div>
            <div className="grid grid-cols-3 gap-4 pt-4 border-t border-border">
              <div>
                <p className="text-xs text-muted-foreground">Total</p>
                <p className="text-lg font-semibold">32 GB</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Used</p>
                <p className="text-lg font-semibold">14.4 GB</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Free</p>
                <p className="text-lg font-semibold">17.6 GB</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Service Components</CardTitle>
          <CardDescription>Individual service health status</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {[
              { name: "FastAPI Server", status: "operational" },
              { name: "PySpark Engine", status: "operational" },
              { name: "Metadata Storage", status: "operational" },
              { name: "Log System", status: "operational" },
              { name: "File Storage", status: "operational" },
            ].map((service) => (
              <div 
                key={service.name}
                className="flex items-center justify-between p-3 rounded-lg border border-border"
              >
                <span className="font-medium">{service.name}</span>
                <Badge variant="success">
                  <CheckCircle2 className="h-3.5 w-3.5 mr-1" />
                  {service.status}
                </Badge>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Health;
