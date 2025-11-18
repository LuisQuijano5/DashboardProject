import React from 'react';
import { LayoutDashboard, Table, BarChart3, Settings, Database, User } from 'lucide-react';

const SidebarItem = ({ icon, label, id, isActive, onClick }) => (
  <button
    onClick={() => onClick(id)}
    className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all text-sm font-medium mb-1
      ${isActive 
        ? 'bg-blue-600 text-white shadow-md shadow-blue-900/20' 
        : 'text-slate-400 hover:bg-slate-800 hover:text-white'
      }`}
  >
    {icon}
    <span>{label}</span>
  </button>
);

const Sidebar = ({ activeTab, onTabChange }) => {
  return (
    <aside className="w-64 bg-slate-900 text-white flex flex-col h-screen fixed left-0 top-0 z-20">
      <div className="p-6 border-b border-slate-800">
        <h1 className="text-2xl font-bold tracking-tighter text-blue-400 flex items-center gap-2">
           Generador Horarios <span className="text-white text-xs bg-slate-700 px-1 py-0.5 rounded">ITC</span>
        </h1>
      </div>

      <nav className="flex-1 p-4 overflow-y-auto">
        <div className="mb-2 px-4 text-xs font-bold text-slate-500 uppercase tracking-wider">Gestión</div>
        <SidebarItem id="control" label="Configuración" icon={<Settings size={18} />} isActive={activeTab === 'control'} onClick={onTabChange} />
        <SidebarItem id="results" label="Horarios Generados" icon={<Table size={18} />} isActive={activeTab === 'results'} onClick={onTabChange} />
        
        <div className="mt-6 mb-2 px-4 text-xs font-bold text-slate-500 uppercase tracking-wider">Análisis</div>
        <SidebarItem id="analytics" label="Métricas ML" icon={<BarChart3 size={18} />} isActive={activeTab === 'analytics'} onClick={onTabChange} />
        <SidebarItem id="dataset" label="Dataset Histórico" icon={<Database size={18} />} isActive={activeTab === 'dataset'} onClick={onTabChange} />
      </nav>

      <div className="p-4 bg-slate-950 border-t border-slate-800">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-full bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center text-white font-bold shadow-lg">
            <User size={16} />
          </div>
          <div>
            <p className="text-sm font-medium text-slate-200">Coordinador</p>
            <p className="text-xs text-slate-500">Sistemas</p>
          </div>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;