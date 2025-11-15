import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { CheckCircle2, XCircle, Clock, Loader2, AlertCircle } from "lucide-react";

export type PipelineStatus = "running" | "success" | "failed" | "pending" | "cancelled";

interface StatusBadgeProps {
  status: PipelineStatus;
  className?: string;
}

const statusConfig = {
  running: {
    label: "Running",
    icon: Loader2,
    variant: "info" as const,
    iconClass: "animate-spin",
  },
  success: {
    label: "Success",
    icon: CheckCircle2,
    variant: "success" as const,
    iconClass: "",
  },
  failed: {
    label: "Failed",
    icon: XCircle,
    variant: "destructive" as const,
    iconClass: "",
  },
  pending: {
    label: "Pending",
    icon: Clock,
    variant: "warning" as const,
    iconClass: "",
  },
  cancelled: {
    label: "Cancelled",
    icon: AlertCircle,
    variant: "secondary" as const,
    iconClass: "",
  },
};

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const config = statusConfig[status];
  const Icon = config.icon;

  return (
    <Badge variant={config.variant} className={cn("gap-1.5", className)}>
      <Icon className={cn("h-3.5 w-3.5", config.iconClass)} />
      {config.label}
    </Badge>
  );
}
