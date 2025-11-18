import React, { useState, useEffect } from 'react';
import { datasetStats as mockStats } from '../assets/mockData';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '../components/ui/Card';
import Loader from '../components/ui/Loader';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, Legend, AreaChart, Area, ReferenceLine } from 'recharts';
import { AlertTriangle } from 'lucide-react';

const DatasetExplorer = () => {
  const [stats, setStats] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDatasetStats = async () => {
      setIsLoading(true);
      try {
        // Simular 
        await new Promise(resolve => setTimeout(resolve, 1500));
        setStats(mockStats);
      } catch (err) {
        setError('Error al recuperar estadísticas históricas de S3.');
      } finally {
        setIsLoading(false);
      }
    };
    fetchDatasetStats();
  }, []);

  if (isLoading) return <div className="h-[60vh] flex items-center justify-center"><Loader text="Procesando datos históricos..." /></div>;

  if (error) return (
    <div className="h-[50vh] flex flex-col items-center justify-center text-slate-400">
        <AlertTriangle size={48} className="mb-4 text-red-400 opacity-80"/>
        <p>{error}</p>
    </div>
  );

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>1. Cuellos de Botella Históricos</CardTitle>
            <CardDescription>Top 5 materias con mayor tasa de reprobación.</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={stats.reprobacion} layout="vertical" margin={{ left: 20 }}>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                <XAxis type="number" unit="%" hide />
                <YAxis dataKey="materia" type="category" width={120} tick={{ fontSize: 12 }} />
                <Tooltip cursor={{ fill: '#f1f5f9' }} />
                <Bar dataKey="tasa" fill="#f43f5e" radius={[0, 4, 4, 0]} name="% Reprobación" barSize={30} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>2. Evolución de la Matrícula</CardTitle>
            <CardDescription>Crecimiento de la demanda estudiantil por año.</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={stats.matricula}>
                <defs>
                  <linearGradient id="colorIsc" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                  </linearGradient>
                  <linearGradient id="colorInd" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="anio" stroke="#64748b" />
                <YAxis stroke="#64748b" />
                <Tooltip />
                <Legend />
                <Area type="monotone" dataKey="ISC" stroke="#3b82f6" fillOpacity={1} fill="url(#colorIsc)" />
                <Area type="monotone" dataKey="IND" stroke="#10b981" fillOpacity={1} fill="url(#colorInd)" />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>3. Impacto del Horario en Aprobación</CardTitle>
            <CardDescription>Evidencia del rendimiento académico según la hora de clase.</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={stats.aprobacionPorHora} margin={{ top: 20 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="hora" stroke="#64748b" />
                <YAxis domain={[0, 100]} stroke="#64748b" />
                <Tooltip />
                <ReferenceLine y={70} stroke="#6c0b0bff" strokeDasharray="3 3" strokeWidth={2} label={{ value: "Mínimo Aprobatorio (70)", fill: "#6c0b0bff", fontSize: 15, fontWeight: "bold", dy: -10 }} />
                <Bar dataKey="tasa" fill="#8b5cf6" radius={[4, 4, 0, 0]} name="Tasa Aprobación (%)" />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>5. Sesgo de Asistencia</CardTitle>
            <CardDescription>Correlación entre horario extremo y ausentismo.</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={stats.asistenciaPorHora} margin={{ top: 20 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="hora" stroke="#64748b" />
                <YAxis domain={[50, 100]} stroke="#64748b" />
                <Tooltip />
                <Line type="monotone" dataKey="asistencia" stroke="#f59e0b" strokeWidth={3} dot={{r: 5}} name="% Asistencia" />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>4. Ranking Histórico de Desempeño Docente</CardTitle>
          <CardDescription>Comparativa basada en promedio de calificaciones (Top 5 vs Bottom 5).</CardDescription>
        </CardHeader>
        <CardContent className="h-[400px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={stats.calidadDocente} layout="vertical" margin={{ left: 40, right: 40 }} barCategoryGap="20%">
              <CartesianGrid strokeDasharray="3 3" horizontal={false} />
              <XAxis type="number" domain={[60, 100]} />
              <YAxis dataKey="nombre" type="category" width={150} tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              <ReferenceLine x={70} stroke="#6c0b0bff" strokeDasharray="3 3" strokeWidth={2} label={{ value: "Zona de Riesgo (<70)", fill: "#6c0b0bff", fontSize: 15, fontWeight: "bold"}} />
              <Bar dataKey="promedio" name="Calif. Promedio del Grupo" radius={[0, 4, 4, 0]}>
                {stats.calidadDocente.map((entry, index) => (
                    <cell key={`cell-${index}`} fill={index < 5 ? '#10b981' : '#f43f5e'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    </div>
  );
};

export default DatasetExplorer;