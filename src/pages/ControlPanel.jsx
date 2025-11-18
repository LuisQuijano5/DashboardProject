import React, { useState, useEffect } from 'react';
import { api } from '../services/api';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '../components/ui/Card';
import Badge from '../components/ui/Badge';
import Loader from '../components/ui/Loader'; 
import Toast from '../components/ui/Toast';  
import { Trash2, Save, UserCheck, Settings2, AlertTriangle } from 'lucide-react';

const ControlPanel = () => {
  const [rules, setRules] = useState([]); 
  const [isLoadingRules, setIsLoadingRules] = useState(true); 
  const [isSaving, setIsSaving] = useState(false);
  const [toast, setToast] = useState(null); 

  const [globalStart, setGlobalStart] = useState(7);
  const [globalEnd, setGlobalEnd] = useState(20);
  const [selectedTeacher, setSelectedTeacher] = useState("");
  const [selectedDay, setSelectedDay] = useState("Lunes");
  const [selectedHours, setSelectedHours] = useState([]);
  const hoursGrid = Array.from({ length: 15 }, (_, i) => i + 7);

  useEffect(() => {
    fetchRulesData();
  }, []);

  const fetchRulesData = async () => {
    setIsLoadingRules(true);
    try {
      const data = await api.getRules(); 
      setRules(data);
    } catch (error) {
      setToast({ type: 'error', message: 'Error de conexión: No se pudieron cargar las reglas.' });
    } finally {
      setIsLoadingRules(false);
    }
  };

  const toggleHour = (hour) => {
    if (selectedHours.includes(hour)) {
      setSelectedHours(selectedHours.filter(h => h !== hour));
    } else {
      setSelectedHours([...selectedHours, hour].sort((a, b) => a - b));
    }
  };

  const handleSaveAvailability = async () => {
    setIsSaving(true); 
    
    const payload = {
        pk: `TEACHER#${selectedTeacher.replace(/\s+/g, '_').toUpperCase()}`,
        sk: `AVAILABILITY#${selectedDay.toUpperCase()}`,
        type: 'TEACHER_AVAILABILITY',
        teacher_name: selectedTeacher,
        day: selectedDay,
        available_hours: selectedHours,
        timestamp: new Date().toISOString()
    };

    try {
        await api.createRule(payload); 
        setToast({ type: 'success', message: `Disponibilidad guardada para ${selectedTeacher}` });
        setSelectedHours([]); 
        fetchRulesData(); 
    } catch (error) {
        setToast({ type: 'error', message: 'Error al guardar. Intente nuevamente.' });
    } finally {
        setIsSaving(false); 
    }
  };

  const handleDeleteRule = async (id) => {
    if(!window.confirm("¿Estás seguro de eliminar esta regla?")) return;

    try {
        await api.deleteRule(id);
        setRules(rules.filter(r => r.id !== id)); 
        setToast({ type: 'success', message: 'Regla eliminada correctamente.' });
    } catch (error) {
        setToast({ type: 'error', message: 'No se pudo eliminar la regla.' });
    }
  }

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      
      {toast && (
        <Toast 
            message={toast.message} 
            type={toast.type} 
            onClose={() => setToast(null)} 
        />
      )}

      <div className="grid lg:grid-cols-3 gap-8">
        
        <div className="lg:col-span-2 space-y-6">

          <Card className="border-blue-100 shadow-md">
            <CardHeader className="bg-blue-50/50 border-b border-blue-100">
              <CardTitle className="text-blue-700 flex items-center gap-2">
                <UserCheck size={20} /> Disponibilidad Docente
              </CardTitle>
              <CardDescription>Marca las horas disponibles por maestro.</CardDescription>
            </CardHeader>
            
            <CardContent className="pt-6 space-y-6">
               <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1.5">
                    <label className="text-sm font-semibold text-slate-700">Profesor</label>
                    <select 
                        value={selectedTeacher}
                        onChange={(e) => setSelectedTeacher(e.target.value)}
                        className="w-full p-2.5 bg-white border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                    >
                        <option value="" disabled>Seleccionar...</option>
                        <option value="Dr. Alan Turing">Dr. Alan Turing</option>
                        <option value="Dra. Ada Lovelace">Dra. Ada Lovelace</option>
                    </select>
                </div>
                <div className="space-y-1.5">
                    <label className="text-sm font-semibold text-slate-700">Día</label>
                    <select 
                        value={selectedDay}
                        onChange={(e) => setSelectedDay(e.target.value)}
                        className="w-full p-2.5 bg-white border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                    >
                        {['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes'].map(d => <option key={d} value={d}>{d}</option>)}
                    </select>
                </div>
              </div>

              <div className="grid grid-cols-5 sm:grid-cols-8 gap-2">
                  {hoursGrid.map(hour => (
                    <button
                        key={hour}
                        onClick={() => toggleHour(hour)}
                        className={`py-2 rounded text-xs font-bold transition-all border
                            ${selectedHours.includes(hour) 
                                ? 'bg-blue-600 text-white' 
                                : 'bg-slate-50 hover:bg-blue-50'}`}
                    >
                        {hour}:00
                    </button>
                  ))}
              </div>

              <button 
                onClick={handleSaveAvailability}
                disabled={!selectedTeacher || selectedHours.length === 0 || isSaving}
                className="w-full bg-slate-900 hover:bg-slate-800 disabled:bg-slate-300 disabled:cursor-not-allowed text-white font-medium py-3 rounded-lg flex items-center justify-center gap-2 transition-colors shadow-lg shadow-slate-900/20"
              >
                {isSaving ? (
                    <>
                        <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                        Guardando...
                    </>
                ) : (
                    <>
                        <Save size={18} /> Guardar Regla
                    </>
                )}
              </button>

            </CardContent>
          </Card>
        </div>

        <div className="lg:col-span-1">
          <Card className="h-full flex flex-col">
            <CardHeader>
              <CardTitle>Reglas Activas</CardTitle>
              <CardDescription>Datos cargados desde DynamoDB.</CardDescription>
            </CardHeader>
            
            <CardContent className="flex-1 overflow-y-auto max-h-[600px] pr-2 min-h-[200px]">
              
              {isLoadingRules ? (
                 <Loader text="Recuperando reglas..." />
              ) : rules.length === 0 ? (
                 <div className="flex flex-col items-center justify-center h-full text-slate-400 py-10">
                    <AlertTriangle size={32} className="mb-2 opacity-50"/>
                    <p>No se encontraron reglas.</p>
                 </div>
              ) : (
                 <div className="space-y-3">
                    {rules.map((regla) => (
                      <div key={regla.id} className="p-3 bg-white rounded-lg border border-slate-200 shadow-sm hover:border-blue-300 transition-colors group">
                        <div className="flex justify-between items-start mb-2">
                            <Badge variant={regla.tipo === 'Global' ? 'blue' : 'default'}>
                                {regla.tipo}
                            </Badge>
                            <button 
                                onClick={() => handleDeleteRule(regla.id)}
                                className="text-slate-300 hover:text-red-500 transition-colors"
                            >
                                <Trash2 size={16} />
                            </button>
                        </div>
                        <p className="text-sm text-slate-700 font-medium leading-snug">{regla.detalle}</p>
                      </div>
                    ))}
                 </div>
              )}
            </CardContent>
          </Card>
        </div>

      </div>
    </div>
  );
};

export default ControlPanel;

 {/* <Card className="border-indigo-100 shadow-sm">
            <CardHeader className="pb-4 border-b border-slate-100">
              <CardTitle className="flex items-center gap-2 text-indigo-700">
                <Settings2 size={20} /> Horario Hábil de la Institución
              </CardTitle>
              <CardDescription>Define el rango global de operación para todos los grupos.</CardDescription>
            </CardHeader>
            <CardContent className="pt-6">
                <div className="flex items-end gap-4">
                    <div className="space-y-1.5 w-1/3">
                        <label className="text-sm font-semibold text-slate-700">Hora Apertura (24h)</label>
                        <input 
                            type="number" 
                            min="6" max="22"
                            value={globalStart}
                            onChange={(e) => setGlobalStart(e.target.value)}
                            className="w-full p-2.5 bg-white border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 outline-none"
                        />
                    </div>
                    <div className="space-y-1.5 w-1/3">
                        <label className="text-sm font-semibold text-slate-700">Hora Cierre (24h)</label>
                        <input 
                            type="number" 
                            min="7" max="23"
                            value={globalEnd}
                            onChange={(e) => setGlobalEnd(e.target.value)}
                            className="w-full p-2.5 bg-white border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 outline-none"
                        />
                    </div>
                    <button 
                        onClick={handleSaveGlobal}
                        className="w-1/3 bg-indigo-600 hover:bg-indigo-700 text-white font-medium py-2.5 rounded-lg flex items-center justify-center gap-2 transition-colors"
                    >
                        <Save size={18} /> Actualizar
                    </button>
                </div>
            </CardContent>
          </Card> */}