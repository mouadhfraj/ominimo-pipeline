import { Sidebar } from "@/components/Sidebar";
import { Outlet } from "react-router-dom";

const Index = () => {
  return (
    <div className="min-h-screen bg-background">
      <Sidebar />
      <main className="pl-64">
        <div className="container py-8">
          <Outlet />
        </div>
      </main>
    </div>
  );
};

export default Index;
