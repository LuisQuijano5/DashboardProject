const BASE_URL = 'https://f6hyqa1nwb.execute-api.us-east-1.amazonaws.com/v2/';
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export const api = {
  getSugerencias: async () => {
      try {
        const response = await fetch(`${BASE_URL}sugerencias`);
        if (!response.ok) throw new Error('Error al cargar sugerencias');
        
        const apiResponse = await response.json();
        const data = apiResponse.body ? JSON.parse(apiResponse.body) : apiResponse;

        return (data || []).map(item => ({
          ...item,
          id: item.grupo_id || item.id,
          materia: item.materia_nombre || item.materia,
          grupo: item.grupo_id || item.grupo,
          profesor: item.profesor_sugerido_id || item.profesor,
          horario: item.horario_sugerido_json || item.horario,
          salon: item.salon_sugerido_id || item.salon,
          score: parseFloat(item.score_TOO_predicho || item.score || 0).toFixed(1),
          demanda: item.demanda ? `${item.demanda}` : 'N/A',
          carrera: item.carrera
        }));
      } catch (error) {
        console.error("Error Sugerencias:", error);
        throw error;
      }
    },

    getAnalytics: async () => {
      try {
        const response = await fetch(`${BASE_URL}analytics`);
        if (!response.ok) throw new Error('Error al cargar analytics');

        const apiResponse = await response.json();
        const data = apiResponse.body ? JSON.parse(apiResponse.body) : apiResponse;

        return {
          kpis: {
            avgScore: parseFloat(data.kpis?.avgScore || 0).toFixed(1),
            demandaSatisfecha: parseFloat(data.kpis?.demandaSatisfecha || 0).toFixed(1),
            eficienciaAulas: parseFloat(data.kpis?.eficienciaAulas || 0).toFixed(1),
          },
          topFeatures: (data.topFeatures || []).map(f => ({
            feature: f.feature,
            importancia: parseFloat(f.importancia)
          })),
          modelHistory: data.modelHistory || []
        };
      } catch (error) {
        console.error("Error Analytics:", error);
        throw error;
      }
    },

    getDashboardData: async () => {
      try {
        const response = await fetch(`${BASE_URL}dashboard-academico`); 
        if (!response.ok) throw new Error('Error al cargar dashboard');
        
        const apiResponse = await response.json();
        const data = apiResponse.body ? JSON.parse(apiResponse.body) : apiResponse;

        return {
            teacherQuality: data.teacherQuality || [],
            bottlenecks: data.bottlenecks || [],
            tuitionEvolution: data.tuitionEvolution || [],
            timeBias: data.timeBias || []
        };
      } catch (error) {
        console.error("Error Dashboard:", error);
        throw error;
      }
  },

  getDashboardData: async () => {
      try {
        const response = await fetch(`${BASE_URL}charts`); 
        if (!response.ok) throw new Error('Error al cargar dashboard');
        
        const apiResponse = await response.json();
        const data = apiResponse.body ? JSON.parse(apiResponse.body) : apiResponse;

        return {
            teacherQuality: data.teacherQuality || [],
            bottlenecks: data.bottlenecks || [],
            tuitionEvolution: data.tuitionEvolution || [],
            timeBias: data.timeBias || []
        };
      } catch (error) {
        console.error("Error Dashboard:", error);
        throw error;
      }
  },


  getAsistencia: async () => {
      try {
        const response = await fetch(`${BASE_URL}asistencia`);
        if (!response.ok) throw new Error('Error al cargar asistencia');

        const apiResponse = await response.json();
        const data = apiResponse.body ? JSON.parse(apiResponse.body) : apiResponse;
        
        return data || []; 
      } catch (error) {
        console.error("Error Asistencia:", error);
        throw error;
      }
  },

  /**
   * Obtener restricc
   */
  getRules: async () => {
      try {
          const response = await fetch(`${BASE_URL}restricciones`);
          if (!response.ok) throw new Error('Error al obtener reglas');
          const apiResponse = await response.json(); 
          
          const processedRules = apiResponse.map(data => {
              
              const bloqueos = data.bloqueos || {}; 
              let detalleBloqueos = "";

              if (Object.keys(bloqueos).length > 0) {
                  const partes = Object.keys(bloqueos).map(dia => {
                      const horas = bloqueos[dia].join(', '); 
                      return `${dia}: ${horas}`; 
                  });
                  detalleBloqueos = `${partes.join(' | ')}`; 
              } else {
                  detalleBloqueos = 'Sin bloqueos de horario definidos.';
              }

              console.log(detalleBloqueos)

              return {
                  id: data.profesor_id || 'N/A',
                  tipo: data.tipo_restriccion || 'BLOQUEO', 
                  detalle: `Profesor(a): ${data.nombre_visual || 'Desconocido'} - ${detalleBloqueos}`, 
                  estado: 'Activo' 
              };
          });

          const finalRules = [...processedRules];

          return finalRules;

      } catch (error) {
          throw error;
      }
  },

  /**
   * Crear restric
   */
  createRule: async (payload) => {
    const response = await fetch(`${BASE_URL}restricciones`, {
        method: 'POST',
        headers: { 
            'Content-Type': 'application/json' 
        },
        body: JSON.stringify(payload)
    });
    
    if (!response.ok) {
        const errorBody = await response.json().catch(() => ({ message: `Error ${response.status}: No se pudo guardar la regla.` }));
        throw new Error(errorBody.message || `Error ${response.status}: No se pudo guardar la regla.`);
    }
    
    return await response.json();
  },


deleteRule: async (id, tipo = "BLOQUEO") => {
    const response = await fetch(`${BASE_URL}restricciones`, { 
        method: 'DELETE',
        headers: { 
            'Content-Type': 'application/json' 
        },
        body: JSON.stringify({
            tipo: tipo,
            id: id
        })
    });
    
    if (!response.ok) {
        const errorBody = await response.json().catch(() => ({ message: `Error ${response.status}: Error al eliminar la regla.` }));
        throw new Error(errorBody.message || `Error ${response.status}: Error al eliminar la regla.`);
    }
    
    return true; 
  }
};