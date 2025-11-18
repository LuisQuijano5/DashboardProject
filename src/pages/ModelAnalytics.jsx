import React, { useState, useEffect } from 'react';
import { modelMetrics as mockMetrics, modelHistory as mockHistory } from '../assets/mockData';
import KpiCard from '../components/dashboard/KpiCard';
import Loader from '../components/ui/Loader';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '../components/ui/Card';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, Legend } from 'recharts';
import { AlertTriangle } from 'lucide-react';

const ModelAnalytics = () => {
  const [metrics, setMetrics] = useState(null);
  const [history, setHistory] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchAnalytics = async () => {
      setIsLoading(true);
      try {
        await new Promise(resolve => setTimeout(resolve, 1200)); // SIMULAR
        setMetrics(mockMetrics);
        setHistory(mockHistory);
      } catch (err) {
        setError('No se pudieron cargar las métricas del modelo.');
      } finally {
        setIsLoading(false);
      }
    };
    fetchAnalytics();
  }, []);

  if (isLoading) return <div className="h-[60vh] flex items-center justify-center"><Loader text="Analizando rendimiento del modelo..." /></div>;
  
  if (error) return (
    <div className="h-[50vh] flex flex-col items-center justify-center text-slate-400">
        <AlertTriangle size={48} className="mb-4 text-red-400 opacity-80"/>
        <p>{error}</p>
    </div>
  );

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      
      <div className="grid gap-4 md:grid-cols-3">
        <KpiCard title="Score Promedio (TOO)" value={`${metrics.kpis.avgScore}%`} subtitle="Calidad global del horario generado" color="blue" />
        <KpiCard title="Demanda Satisfecha" value={`${metrics.kpis.demandaSatisfecha}%`} subtitle="Alumnos con carga completa" color="green" />
        <KpiCard title="Eficiencia de Aulas" value={`${metrics.kpis.eficienciaAulas}%`} subtitle="Promedio de ocupación por hora" color="purple" />
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        
        <Card>
          <CardHeader>
            <CardTitle>Justificación del Modelo</CardTitle>
            <CardDescription>Factores de mayor peso en la decisión.</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={metrics.topFeatures} layout="vertical" margin={{ left: 20 }}>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
                <XAxis type="number" stroke="#64748b" />
                <YAxis dataKey="feature" type="category" width={130} tick={{fontSize: 12, fill: '#475569'}} />
                <Tooltip contentStyle={{ borderRadius: '8px' }} cursor={{ fill: '#f8fafc' }} />
                <Bar dataKey="importancia" fill="#3b82f6" radius={[0, 4, 4, 0]} barSize={24} name="Peso (%)" />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
            <CardHeader>
                <CardTitle>Evolución del Rendimiento</CardTitle>
                <CardDescription>Mejora de métricas (RMSE vs F1-Score) por semestre.</CardDescription>
            </CardHeader>
            <CardContent className="h-[300px]">
                <ResponsiveContainer width="100%" height="100%">
                <LineChart data={history} margin={{ right: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                    <XAxis dataKey="semestre" stroke="#64748b" tick={{fontSize: 12}} />
                    {/* NO SE SI SE VAYA A USAR */}
                    <YAxis yAxisId="left" domain={[0, 1]} stroke="#16a34a" />
                    {/* Eje Y Derecho para RMSE */}
                    <YAxis yAxisId="right" orientation="right" stroke="#ef4444" />
                    <Tooltip contentStyle={{ borderRadius: '8px' }} />
                    <Legend verticalAlign="top" height={36}/>
                    <Line yAxisId="left" type="monotone" dataKey="f1Score" stroke="#16a34a" strokeWidth={3} dot={{r:4}} name="F1-Score (Precisión)" />
                    <Line yAxisId="right" type="monotone" dataKey="rmse" stroke="#ef4444" strokeWidth={2} strokeDasharray="5 5" dot={{r:4}} name="RMSE (Error)" />
                </LineChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default ModelAnalytics;