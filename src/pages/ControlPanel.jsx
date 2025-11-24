import React, { useState, useEffect } from 'react';
import { api } from '../services/api';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '../components/ui/Card';
import Badge from '../components/ui/Badge';
import Loader from '../components/ui/Loader'; 
import Toast from '../components/ui/Toast';  
import { TEACHER_CATALOG } from '../assets/catalogo_profesores';
import { Trash2, Save, UserCheck, AlertTriangle } from 'lucide-react';

const ControlPanel = () => {
  const [rules, setRules] = useState([]); 
  const [isLoadingRules, setIsLoadingRules] = useState(true); 
  const [isSaving, setIsSaving] = useState(false);
  const [toast, setToast] = useState(null); 

  const [selectedTeacher, setSelectedTeacher] = useState("");
  const [selectedDay, setSelectedDay] = useState("LUNES"); 
  
  const [schedule, setSchedule] = useState({});
  
  const hoursGrid = Array.from({ length: 13 }, (_, i) => i + 7);

  useEffect(() => {
    fetchRulesData();
  }, []);

  const fetchRulesData = async () => {
    setIsLoadingRules(true);
    try {
      const data = await api.getRules(); 
      setRules(data);
    } catch (error) {
      console.log(error)
      setToast({ type: 'error', message: 'Error de conexión.' });
    } finally {
      setIsLoadingRules(false);
    }
  };

  const toggleHour = (hour) => {
    const currentHours = schedule[selectedDay] || [];
    let newHours;

    if (currentHours.includes(hour)) {
      newHours = currentHours.filter(h => h !== hour);
    } else {
      newHours = [...currentHours, hour].sort((a, b) => a - b);
    }

    const newSchedule = { ...schedule };
    if (newHours.length > 0) {
        newSchedule[selectedDay] = newHours;
    } else {
        delete newSchedule[selectedDay];
    }
    
    setSchedule(newSchedule);
  };

  const handleSaveAvailability = async () => {
    setIsSaving(true); 
    
    const teacherObj = TEACHER_CATALOG.find(t => t.profesor_id === selectedTeacher);
    const teacherName = teacherObj ? teacherObj.nombre_profesor : selectedTeacher;

    const payload = {
        tipo: "BLOQUEO",
        id: selectedTeacher,      
        nombre: teacherName,       
        bloqueos: schedule        
    };

    try {
        await api.createRule(payload); 
        setToast({ type: 'success', message: `Bloqueos guardados para ${teacherName}` });
        setSchedule({});
        setSelectedTeacher("");
        fetchRulesData(); 
    } catch (error) {
        setToast({ type: 'error', message: 'Error al guardar. Intente nuevamente.' });
    } finally {
        setIsSaving(false); 
    }
  };

  const handleDeleteRule = async (id, tipo) => {
    if(!window.confirm("¿Estás seguro de eliminar esta regla?")) return;

    try {
        await api.deleteRule(id, tipo);
        
        setRules(rules.filter(r => r.id !== id)); 
        setToast({ type: 'success', message: 'Regla eliminada correctamente.' });
    } catch (error) {
        console.error(error);
        setToast({ type: 'error', message: 'No se pudo eliminar la regla.' });
    }
  }

  const isHourSelected = (hour) => {
      return schedule[selectedDay]?.includes(hour);
  };

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
              <CardDescription>Marca las horas a bloquear. Puedes cambiar de día y seguir seleccionando.</CardDescription>
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
                        {TEACHER_CATALOG.map((teacher, index) => (
                            <option key={index} value={teacher.profesor_id}>
                                {teacher.nombre_profesor}
                            </option>
                        ))}
                    </select>
                </div>
                <div className="space-y-1.5">
                    <label className="text-sm font-semibold text-slate-700">Día</label>
                    <select 
                        value={selectedDay}
                        onChange={(e) => setSelectedDay(e.target.value)}
                        className="w-full p-2.5 bg-white border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                    >
                        {['LUNES', 'MARTES', 'MIERCOLES', 'JUEVES', 'VIERNES'].map(d => (
                            <option key={d} value={d}>
                                {d} {schedule[d] ? '•' : ''}
                            </option>
                        ))}
                    </select>
                </div>
              </div>

              <div className="grid grid-cols-5 sm:grid-cols-8 gap-2">
                  {hoursGrid.map(hour => (
                    <button
                        key={hour}
                        onClick={() => toggleHour(hour)}
                        className={`py-2 rounded text-xs font-bold transition-all border
                            ${isHourSelected(hour) 
                                ? 'bg-blue-600 text-white' 
                                : 'bg-slate-50 hover:bg-blue-50'}`}
                    >
                        {hour}:00
                    </button>
                  ))}
              </div>

              <div className="text-xs text-slate-500">
                Resumen: {Object.keys(schedule).map(d => `${d} (${schedule[d].length}h)`).join(', ')}
              </div>

              <button 
                onClick={handleSaveAvailability}
                disabled={!selectedTeacher || Object.keys(schedule).length === 0 || isSaving}
                className="w-full bg-slate-900 hover:bg-slate-800 disabled:bg-slate-300 disabled:cursor-not-allowed text-white font-medium py-3 rounded-lg flex items-center justify-center gap-2 transition-colors shadow-lg shadow-slate-900/20"
              >
                {isSaving ? (
                    <>
                        <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                        Guardando...
                    </>
                ) : (
                    <>
                        <Save size={18} /> Guardar Bloqueos
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
                ) : (
                    <>
                        <div className="p-3 bg-white rounded-lg border border-blue-400 shadow-lg mb-3 transition-colors group opacity-95">
                            <div className="flex justify-between items-start mb-2">
                                <Badge variant="blue">GLOBAL</Badge>
                            </div>
                            <p className="text-sm text-slate-700 font-bold leading-snug">
                                Horario de 7:00 a 19:00
                            </p>
                            <p className="text-xs text-slate-500">
                                (Regla de horario de la escuela)
                            </p>
                        </div>

                        {rules.length === 0 ? (
                            <div className="flex flex-col items-center justify-center h-full text-slate-400 py-10">
                                <AlertTriangle size={32} className="mb-2 opacity-50"/>
                                <p>No se encontraron reglas adicionales.</p>
                            </div>
                        ) : (
                            <div className="space-y-3">
                                {rules
                                  .filter(regla => regla.tipo !== 'GLOBAL') 
                                  .map((regla) => (
                                  <div 
                                      key={regla.id} 
                                      className="p-3 bg-white rounded-lg border border-slate-200 shadow-sm hover:border-blue-300 transition-colors group"
                                  >
                                      <div className="flex justify-between items-start mb-2">
                                          <Badge variant="default">{regla.tipo}</Badge>
                                          
                                          <button 
                                              onClick={() => handleDeleteRule(regla.id, regla.tipo)}
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
                    </>
                )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};

export default ControlPanel;