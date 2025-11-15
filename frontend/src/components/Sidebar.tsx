import { NavLink } from "@/components/NavLink";
import { cn } from "@/lib/utils";
import { 
  LayoutDashboard, 
  PlayCircle, 
  FileJson, 
  Activity, 
  FileText,
  Shield
} from "lucide-react";

const navigation = [
  { name: "Dashboard", href: "/", icon: LayoutDashboard },
  { name: "Pipeline Runs", href: "/runs", icon: PlayCircle },
  { name: "Metadata", href: "/metadata", icon: FileJson },
  { name: "Execute Pipeline", href: "/execute", icon: Activity },
  { name: "Logs", href: "/logs", icon: FileText },
  { name: "Health", href: "/health", icon: Shield },
];

export function Sidebar() {
  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-64 border-r border-border bg-card">
      <div className="flex h-full flex-col">
        <div className="flex h-16 items-center border-b border-border px-6">
          <h1 className="text-xl font-bold text-primary">Ominimo Pipeline</h1>
        </div>
        <nav className="flex-1 space-y-1 px-3 py-4">
          {navigation.map((item) => (
            <NavLink
              key={item.name}
              to={item.href}
              className={cn(
                "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
              )}
              activeClassName="bg-accent text-accent-foreground"
            >
              <item.icon className="h-5 w-5" />
              {item.name}
            </NavLink>
          ))}
        </nav>
        <div className="border-t border-border p-4">
          <p className="text-xs text-muted-foreground">
            Motor Insurance Pipeline v1.0
          </p>
        </div>
      </div>
    </aside>
  );
}
