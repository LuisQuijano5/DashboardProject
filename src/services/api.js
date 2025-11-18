const BASE_URL = 'https://tu-api-id.execute-api.us-east-1.amazonaws.com/prod';
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export const api = {
  /**
   * Obtener restricc
   */
  getRules: async () => {
    // Simular
    await delay(1500); 

    // retornar mock
    return [
        { id: 1, tipo: 'Bloqueo', detalle: 'Dr. Turing NO disponible Lunes 7-9am', estado: 'Activo' },
        { id: 2, tipo: 'Global', detalle: 'Hora máxima de salida: 20:00', estado: 'Activo' },
    ];

    /* Real
    try {
      const response = await fetch(`${BASE_URL}/rules`);
      if (!response.ok) throw new Error('Error al obtener reglas');
      return await response.json();
    } catch (error) {
      throw error;
    }
    */
  },

  /**
   * Crear restric
   */
  createRule: async (payload) => {
    await delay(2000); 
    
    // Simular
    console.log("Enviado a AWS:", payload);
    return { success: true, message: "Regla guardada correctamente en DynamoDB" };

    /* Real
    const response = await fetch(`${BASE_URL}/rules`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    if (!response.ok) throw new Error('No se pudo guardar la regla');
    return await response.json();
    */
  },

  /**
   * Eliminar Regla 
   */
  deleteRule: async (ruleId) => {
    await delay(1000);
    console.log("Eliminando ID:", ruleId);
    return true;

    /* Real
    const response = await fetch(`${BASE_URL}/rules/${ruleId}`, { method: 'DELETE' });
    if (!response.ok) throw new Error('Error al eliminar');
    return true;
    */
  }
};