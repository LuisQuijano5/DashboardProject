import { useState, useEffect } from 'react';
import { api } from '../services/api';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '../components/ui/Card';
import Loader from '../components/ui/Loader';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, Legend, AreaChart, Area, ReferenceLine, Cell } from 'recharts';
import { AlertTriangle } from 'lucide-react';

const DatasetExplorer = () => {
  const [stats, setStats] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDatasetStats = async () => {
      setIsLoading(true);
      try {
        const [dashboardData, asistenciaData] = await Promise.all([
            api.getDashboardData(), 
            api.getAsistencia()     
        ]);


        const processedBottlenecks = dashboardData.bottlenecks.map(item => ({
            materia: item.materia_nombre,
            tasa: parseFloat(item.tasa_reprobacion) <= 1 
                  ? (parseFloat(item.tasa_reprobacion) * 100).toFixed(1) 
                  : parseFloat(item.tasa_reprobacion)
        })).slice(0, 5); 

 
        const tuitionMap = {};
        dashboardData.tuitionEvolution.forEach(item => {
            const anio = item.anio || item.semestre_historico; 
            if (!tuitionMap[anio]) {
                tuitionMap[anio] = { anio: anio };
            }
            tuitionMap[anio][item.carrera] = parseInt(item.matricula || item.num_alumnos_activos);
        });
        const processedTuition = Object.values(tuitionMap).sort((a, b) => String(a.anio).localeCompare(String(b.anio)));

        const processedTimeBias = dashboardData.timeBias.map(item => ({
            hora: item.hora || item.hora_inicio_promedio,
            tasa: parseFloat(item.tasa_aprobacion_promedio) <= 1
                  ? (parseFloat(item.tasa_aprobacion_promedio) * 100).toFixed(1)
                  : item.tasa_aprobacion_promedio
        })).sort((a, b) => parseInt(a.hora) - parseInt(b.hora));

        const processedTeachers = dashboardData.teacherQuality.map(item => ({
            nombre: item.nombre_profesor,
            promedio: parseFloat(item.calificacion_promedio).toFixed(1)
        })).sort((a, b) => b.promedio - a.promedio); 

        let processedAttendance = [];
        if (Array.isArray(asistenciaData) && asistenciaData.length > 0) {
            processedAttendance = asistenciaData.map(item => ({
                hora: item.hora_inicio_promedio, 
                asistencia: (parseFloat(item.tasa_asistencia_promedio) * 100).toFixed(1)
            }));
            processedAttendance.sort((a, b) => parseInt(a.hora) - parseInt(b.hora));
        }

        setStats({
            reprobacion: processedBottlenecks,
            matricula: processedTuition,
            aprobacionPorHora: processedTimeBias,
            calidadDocente: processedTeachers,
            asistenciaPorHora: processedAttendance 
        });

      } catch (err) {
        console.error(err);
        setError('Error al recuperar estadísticas históricas de S3/DynamoDB.');
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
        <button onClick={() => window.location.reload()} className="mt-4 text-blue-600 hover:underline">Reintentar</button>
    </div>
  );

  if (!stats) return null;

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
                <YAxis dataKey="materia" type="category" width={120} tick={{ fontSize: 10 }} />
                <Tooltip cursor={{ fill: '#f1f5f9' }} />
                <Bar dataKey="tasa" fill="#f43f5e" radius={[0, 4, 4, 0]} name="% Reprobación" barSize={20} />
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
                <YAxis domain={[0, 100]} stroke="#64748b" />
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
          <CardDescription>Comparativa basada en promedio de calificaciones.</CardDescription>
        </CardHeader>
        <CardContent className="h-[400px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={stats.calidadDocente} layout="vertical" margin={{ left: 40, right: 40 }} barCategoryGap="20%">
              <CartesianGrid strokeDasharray="3 3" horizontal={false} />
              <XAxis type="number" domain={[60, 100]} />
              <YAxis dataKey="nombre" type="category" width={150} tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <ReferenceLine x={70} stroke="#6c0b0bff" strokeDasharray="3 3" strokeWidth={2} label={{ value: "Riesgo (<70)", fill: "#6c0b0bff", fontSize: 12, fontWeight: "bold"}} />
              <Bar dataKey="promedio" name="Calif. Promedio" radius={[0, 4, 4, 0] } fill={'#f43f5e'}>
                {stats.calidadDocente.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.promedio >= 80 ? '#10b981' : '#f43f5e'} />
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