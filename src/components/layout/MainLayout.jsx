import React from 'react';
import Sidebar from './Sidebar';

const MainLayout = ({ children, activeTab, setActiveTab }) => {
  const titles = {
    control: "Panel de Control",
    results: "Horarios Optimizados",
    analytics: "Analítica del Modelo",
    dataset: "Explorador de Datos"
  };

  return (
    <div className="flex min-h-screen bg-slate-50 font-sans text-slate-900 pl-64">
      <Sidebar activeTab={activeTab} onTabChange={setActiveTab} />
      
      <main className="flex-1 flex flex-col w-full">
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 sticky top-0 z-10">
          <h2 className="text-xl font-semibold text-slate-800">{titles[activeTab]}</h2>
          <div className="flex gap-3">
            <span className="text-xs text-slate-400 flex items-center">v1-Prueba_datos_simulados</span>
          </div>
        </header>

        <div className="p-8 max-w-7xl mx-auto w-full">
          {children}
        </div>
      </main>
    </div>
  );
};

export default MainLayout;