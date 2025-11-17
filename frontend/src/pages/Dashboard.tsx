import { StatCard } from "@/components/StatCard";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { 
  Activity, 
  CheckCircle, 
  XCircle, 
  TrendingUp, 
  PlayCircle,
  Clock,
  RefreshCw
} from "lucide-react";
import { Link } from "react-router-dom";
import { PipelineRun, PipelineStats } from "@/types/pipeline";
import { useState, useEffect } from "react";
import { api } from "@/services/api";
import { toast } from "sonner";

const Dashboard = () => {
  const [stats, setStats] = useState<PipelineStats | null>(null);
  const [recentRuns, setRecentRuns] = useState<PipelineRun[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchData = async () => {
    try {
      setLoading(true);
      const [statsData, runsData] = await Promise.all([
        api.getStats(),
        api.listPipelineRuns(5)
      ]);
      setStats(statsData);
      setRecentRuns(runsData);
    } catch (error) {
      toast.error("Failed to fetch dashboard data");
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!stats) return null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-muted-foreground mt-1">
            Motor insurance pipeline monitoring and control
          </p>
        </div>
        <div className="flex gap-2">
          <Button onClick={fetchData} variant="outline" size="sm">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
          <Link to="/execute">
            <Button size="lg" className="gap-2">
              <PlayCircle className="h-5 w-5" />
              Execute Pipeline
            </Button>
          </Link>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Total Runs"
          value={stats.total_runs}
          icon={Activity}
          description="All time pipeline executions"
        />
        <StatCard
          title="Success Rate"
          value={`${stats.success_rate.toFixed(1)}%`}
          icon={TrendingUp}
          variant="success"
          description="Overall pipeline success"
          trend={{ value: 2.5, isPositive: true }}
        />
        <StatCard
          title="Active Pipelines"
          value={stats.running_pipelines}
          icon={Clock}
          variant="info"
          description="Currently running"
        />
        <StatCard
          title="Failed Runs"
          value={stats.failed_runs}
          icon={XCircle}
          variant="destructive"
          description="Requires attention"
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Pipeline Performance</CardTitle>
            <CardDescription>Success vs Failed executions</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-success" />
                  <span className="text-sm font-medium">Successful</span>
                </div>
                <span className="text-2xl font-bold text-success">
                  {stats.successful_runs}
                </span>
              </div>
              <div className="h-2 w-full bg-muted rounded-full overflow-hidden">
                <div 
                  className="h-full bg-success"
                  style={{ width: `${stats.success_rate}%` }}
                />
              </div>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <XCircle className="h-5 w-5 text-destructive" />
                  <span className="text-sm font-medium">Failed</span>
                </div>
                <span className="text-2xl font-bold text-destructive">
                  {stats.failed_runs}
                </span>
              </div>
              <div className="h-2 w-full bg-muted rounded-full overflow-hidden">
                <div 
                  className="h-full bg-destructive"
                  style={{ width: `${100 - stats.success_rate}%` }}
                />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Recent Pipeline Runs</CardTitle>
            <CardDescription>Latest execution activity</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {recentRuns.map((run) => (
                <Link 
                  key={run.pipeline_id} 
                  to={`/runs/${run.pipeline_id}`}
                  className="block"
                >
                  <div className="flex items-center justify-between p-3 rounded-lg border border-border hover:bg-accent transition-colors">
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium truncate">
                        {run.pipeline_id}
                      </p>
                      <p className="text-xs text-muted-foreground truncate">
                        {run.metadata_path}
                      </p>
                    </div>
                    <StatusBadge status={run.status} />
                  </div>
                </Link>
              ))}
              <Link to="/runs">
                <Button variant="outline" className="w-full">
                  View All Runs
                </Button>
              </Link>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Dashboard;
