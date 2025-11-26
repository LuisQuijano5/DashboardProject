import Sidebar from './Sidebar';
import { useMutation } from '@tanstack/react-query';
import { api } from '../../services/api';
import { Play, RotateCw, Loader2 } from 'lucide-react';

const MainLayout = ({ children, activeTab, setActiveTab }) => {
  const titles = {
    control: "Panel de Control",
    results: "Horarios Optimizados",
    analytics: "Analítica del Modelo",
    dataset: "Explorador de Datos"
  };

  const triggerMutation = useMutation({
    mutationFn: api.triggerWorkflow,
    onSuccess: (data, variables) => {
      const accion = variables === 'prediccion' ? 'Generación de Horarios' : 'Re-entrenamiento';
      alert(`¡${accion} iniciada con éxito.\nID: ${data.executionArn}\n\nEl proceso corre en segundo plano.`);
    },
    onError: (err) => alert(`Error: ${err.message}`)
  });

  const handlePredict = () => {
    if(confirm("¿Iniciar la generación de horarios para el próximo semestre? Esto tomará unos minutos.")) {
      triggerMutation.mutate('prediccion');
    }
  };

  const handleRetrain = () => {
    if(confirm("¿Re-entrenar el modelo con todo el histórico? (Operación pesada)")) {
      triggerMutation.mutate('entrenamiento');
    }
  };

  return (
    <div className="flex min-h-screen bg-slate-50 font-sans text-slate-900 pl-64">
      <Sidebar activeTab={activeTab} onTabChange={setActiveTab} />
      
      <main className="flex-1 flex flex-col w-full">
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 sticky top-0 z-10">
          <h2 className="text-xl font-semibold text-slate-800">{titles[activeTab]}</h2>
          
          <div className="flex items-center gap-4">
            <button
              onClick={handleRetrain}
              disabled={triggerMutation.isPending}
              className="flex items-center px-3 py-1.5 text-xs font-medium text-slate-600 bg-slate-100 hover:bg-slate-200 rounded-md transition-colors border border-slate-200 disabled:opacity-50"
              title="Entrenar modelo con datos históricos"
            >
              {triggerMutation.isPending && triggerMutation.variables === 'entrenamiento' ? (
                <Loader2 className="w-3.5 h-3.5 mr-2 animate-spin"/>
              ) : (
                <RotateCw className="w-3.5 h-3.5 mr-2"/>
              )}
              Re-entrenar
            </button>

            <button
              onClick={handlePredict}
              disabled={triggerMutation.isPending}
              className="flex items-center px-4 py-2 text-sm font-semibold text-white bg-blue-600 hover:bg-blue-700 rounded-lg shadow-sm hover:shadow-md transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {triggerMutation.isPending && triggerMutation.variables === 'prediccion' ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin"/>
                  Procesando...
                </>
              ) : (
                <>
                  <Play className="w-4 h-4 mr-2 fill-current"/>
                  Generar Horarios
                </>
              )}
            </button>

            <div className="h-6 w-px bg-slate-200 mx-1"></div>
            <span className="text-xs text-slate-400 font-mono">v1-Datos-Inventados</span>
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