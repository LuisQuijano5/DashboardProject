import React, { useState, useEffect } from 'react';
import { sugerenciasHorario as mockData } from '../assets/mockData'; 
import { Card } from '../components/ui/Card';
import Badge from '../components/ui/Badge';
import Loader from '../components/ui/Loader';
import Toast from '../components/ui/Toast';
import { Filter, Search, XCircle, AlertTriangle } from 'lucide-react';

const ResultsView = () => {
  const [data, setData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  const [carreraFilter, setCarreraFilter] = useState('all');
  const [semestreFilter, setSemestreFilter] = useState('all');
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      try {
        // Simular espera para verloo
        await new Promise(resolve => setTimeout(resolve, 1500));
        setData(mockData);
      } catch (err) {
        setError('No se pudieron cargar los horarios generados. Intente recargar.');
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, []);

  const filteredData = data.filter((item) => {
    const matchesCarrera = 
      carreraFilter === 'all' || 
      (carreraFilter === 'ISC' && (item.salon.includes('SISTEMAS') || item.salon.includes('PROG') || item.salon.includes('CC') || item.salon.includes('CB'))) ||
      (carreraFilter === 'IND' && (item.salon.includes('IND') || item.salon.includes('DH')));

    const matchesSemestre = 
        semestreFilter === 'all' || 
        item.semestre.toString() === semestreFilter;

    const matchesSearch = 
      item.materia.toLowerCase().includes(searchTerm.toLowerCase()) ||
      item.profesor.toLowerCase().includes(searchTerm.toLowerCase());

    return matchesCarrera && matchesSemestre && matchesSearch;
  });

  const clearFilters = () => {
    setCarreraFilter('all');
    setSemestreFilter('all');
    setSearchTerm('');
  };

  if (isLoading) {
    return <div className="h-[60vh] flex items-center justify-center"><Loader text="Recuperando sugerencias óptimas..." /></div>;
  }

  if (error) {
    return (
      <div className="h-[50vh] flex flex-col items-center justify-center text-slate-400">
        <AlertTriangle size={48} className="mb-4 text-red-400 opacity-80"/>
        <p className="text-lg font-medium text-slate-600">{error}</p>
        <button onClick={() => window.location.reload()} className="mt-4 text-blue-600 hover:underline">Recargar página</button>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
        <div className="flex flex-col sm:flex-row gap-3 w-full md:w-auto">
          <div className="flex items-center gap-2 text-slate-500 text-sm font-medium min-w-fit">
              <Filter size={16}/> Filtros:
          </div>
          <select value={carreraFilter} onChange={(e) => setCarreraFilter(e.target.value)} className="bg-slate-50 border border-slate-200 text-slate-700 text-sm py-2 px-3 rounded-md outline-none cursor-pointer hover:bg-slate-100">
            <option value="all">Todas las Carreras</option>
            <option value="ISC">Ing. Sistemas (ISC)</option>
            <option value="IND">Ing. Industrial (IND)</option>
          </select>
          <select value={semestreFilter} onChange={(e) => setSemestreFilter(e.target.value)} className="bg-slate-50 border border-slate-200 text-slate-700 text-sm py-2 px-3 rounded-md outline-none cursor-pointer hover:bg-slate-100">
            <option value="all">Todos los Semestres</option>
            {[1,2,3,4,5,6,7,8].map(n => <option key={n} value={n}>Semestre {n}</option>)}
          </select>
        </div>
        <div className="flex items-center gap-2 w-full md:w-auto">
            <div className="relative w-full md:w-64">
                <input type="text" placeholder="Buscar materia o profesor..." value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} className="w-full pl-9 pr-3 py-2 bg-slate-50 border border-slate-200 rounded-md text-sm outline-none"/>
                <Search size={16} className="absolute left-3 top-2.5 text-slate-400 pointer-events-none"/>
            </div>
            {(carreraFilter !== 'all' || semestreFilter !== 'all' || searchTerm !== '') && (
                <button onClick={clearFilters} className="text-slate-400 hover:text-red-500 p-2 transition-colors" title="Limpiar"><XCircle size={20} /></button>
            )}
        </div>
      </div>

      <Card className="overflow-hidden min-h-[400px]">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="bg-slate-50 border-b border-slate-200 sticky top-0">
              <tr>
                <th className="px-6 py-4 font-semibold text-slate-700 w-16">Sem.</th> 
                <th className="px-6 py-4 font-semibold text-slate-700">Materia / Grupo</th>
                <th className="px-6 py-4 font-semibold text-slate-700">Profesor</th>
                <th className="px-6 py-4 font-semibold text-slate-700">Horario</th>
                <th className="px-6 py-4 font-semibold text-slate-700">Salón</th>
                <th className="px-6 py-4 font-semibold text-slate-700">Demanda</th>
                <th className="px-6 py-4 font-semibold text-slate-700">TOO</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {filteredData.length > 0 ? (
                filteredData.map((item) => (
                    <tr key={item.id} className="hover:bg-slate-50/80 transition-colors">
                    <td className="px-6 py-4">
                        <span className="flex items-center justify-center w-8 h-8 rounded-full bg-slate-100 text-slate-600 font-bold text-xs">{item.semestre}</span>
                    </td>
                    <td className="px-6 py-4">
                        <div className="font-medium text-slate-900">{item.materia}</div>
                        <div className="text-xs text-slate-500 font-mono mt-0.5">{item.grupo}</div>
                    </td>
                    <td className="px-6 py-4 text-slate-600">{item.profesor}</td>
                    <td className="px-6 py-4">
                        <span className="inline-block bg-slate-100 text-slate-700 px-2 py-1 rounded text-xs font-mono border border-slate-200">{item.horario}</span>
                    </td>
                    <td className="px-6 py-4 text-slate-600">{item.salon}</td>
                    <td className="px-6 py-4 text-slate-600">{item.demanda}</td>
                    <td className="px-6 py-4">
                        <Badge variant={item.score >= 90 ? 'success' : item.score >= 80 ? 'warning' : 'danger'}>{item.score}%</Badge>
                    </td>
                    </tr>
                ))
              ) : (
                  <tr>
                      <td colSpan="7" className="px-6 py-12 text-center text-slate-500">
                          <div className="flex flex-col items-center gap-2">
                              <Search size={32} className="text-slate-300"/>
                              <p>No se encontraron horarios con esos filtros.</p>
                          </div>
                      </td>
                  </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
};

export default ResultsView;