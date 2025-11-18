export const datasetStats = {
  reprobacion: [
    { materia: 'Cálculo Integral', tasa: 45 },
    { materia: 'Física General', tasa: 38 },
    { materia: 'Programación OO', tasa: 32 },
    { materia: 'Ecuaciones Dif.', tasa: 30 },
    { materia: 'Química', tasa: 15 },
  ],

  matricula: [
    { anio: '2019', ISC: 210, IND: 160 },
    { anio: '2020', ISC: 225, IND: 170 },
    { anio: '2021', ISC: 240, IND: 175 },
    { anio: '2022', ISC: 260, IND: 180 },
    { anio: '2023', ISC: 250, IND: 190 },
  ],

  aprobacionPorHora: [
    { hora: '7:00', tasa: 65 },  
    { hora: '9:00', tasa: 88 },  
    { hora: '11:00', tasa: 92 }, 
    { hora: '13:00', tasa: 85 }, 
    { hora: '15:00', tasa: 78 }, 
    { hora: '17:00', tasa: 68 }, 
  ],

  calidadDocente: [
    { nombre: 'Dr. Alan Turing', promedio: 95, grupos: 12 },
    { nombre: 'Dra. Grace Hopper', promedio: 94, grupos: 15 },
    { nombre: 'Ing. Henry Ford', promedio: 91, grupos: 18 },
    { nombre: 'Lic. Ada Lovelace', promedio: 89, grupos: 14 },
    { nombre: 'Dr. Carl Gauss', promedio: 88, grupos: 20 },
    { nombre: 'Prof. P. Skinner', promedio: 74, grupos: 11 },
    { nombre: 'Lic. Malo', promedio: 72, grupos: 13 },
    { nombre: 'Ing. Desinteresado', promedio: 70, grupos: 10 },
    { nombre: 'Dr. Aburrido', promedio: 68, grupos: 16 },
    { nombre: 'Lic. Faltista', promedio: 65, grupos: 12 },
  ],

  asistenciaPorHora: [
    { hora: '7:00', asistencia: 70 }, 
    { hora: '9:00', asistencia: 95 },
    { hora: '11:00', asistencia: 96 },
    { hora: '13:00', asistencia: 88 },
    { hora: '15:00', asistencia: 82 },
    { hora: '17:00', asistencia: 75 }, 
  ]
};

export const sugerenciasHorario = [ // Mandarlas así a dynamo, cualquier cambio nos avisas Ulises. Obviamente se obtienen después de la predicción
  { id: 1, materia: 'Programación Web', semestre: 6, grupo: 'G101', profesor: 'Dr. Alan Turing', horario: 'L-Mi-V 09:00-11:00', salon: 'LAB-SISTEMAS-1', score: 98, demanda: '35/35' },
  { id: 2, materia: 'Investigación de Ops', semestre: 4, grupo: 'G204', profesor: 'Ing. Henry Ford', horario: 'Ma-Ju 11:00-13:00', salon: 'IND-A4', score: 92, demanda: '30/35' },
  { id: 3, materia: 'Cálculo Vectorial', semestre: 3, grupo: 'G105', profesor: 'Dr. Carl Gauss', horario: 'L-Mi-V 07:00-09:00', salon: 'CB-01', score: 85, demanda: '28/40' },
  { id: 4, materia: 'Taller de Ética', semestre: 1, grupo: 'G102', profesor: 'Lic. Carl Rogers', horario: 'Ma-Ju 15:00-17:00', salon: 'DH-02', score: 76, demanda: '40/40' },
  { id: 5, materia: 'Simulación', semestre: 7, grupo: 'G301', profesor: 'Dra. Grace Hopper', horario: 'L-Mi-V 13:00-15:00', salon: 'LAB-SISTEMAS-2', score: 99, demanda: '25/25' },
  { id: 6, materia: 'Fundamentos de Prog.', semestre: 1, grupo: 'G101', profesor: 'Dra. Ada Lovelace', horario: 'L-Mi-V 07:00-09:00', salon: 'LAB-PROG-1', score: 95, demanda: '40/40' },
  { id: 7, materia: 'Ingeniería Económica', semestre: 5, grupo: 'IND-501', profesor: 'Lic. Adam Smith', horario: 'Ma-Ju 09:00-11:00', salon: 'IND-A2', score: 88, demanda: '32/35' },
];

export const modelMetrics = {
  kpis: { // Estas se obtienen después de la predicción Ulises
    avgScore: 94.5, // El promedio del Score_TOO_Predicho, mandalo a dynamo
    demandaSatisfecha: 98.2, // El porcentaje de cupos_asignados / demanda_total_calculada
    eficienciaAulas: 88.4 // El porcentaje de horas_usadas / horas_disponibles
  },
  topFeatures: [ // Las puedes obtener con model.featureImportances cuando lo entrenes. Mandalas con un formato similar a dynamo Ulises
    { feature: 'Ranking Profesor', importancia: 35 },
    { feature: 'Preferencia Horaria', importancia: 25 },
    { feature: 'Evitar Empalmes', importancia: 20 },
    { feature: 'Capacidad Salón', importancia: 15 },
    { feature: 'Historial Reprobación', importancia: 5 },
  ]
};

// Las dos o solo una métrica, tu dices como Ulises
export const modelHistory = [ // Por ejemplo, rmse = evaluator.evaluate(predictions) y mandar a dynamo, con el semestre obviamente
  { semestre: '2022-1', f1Score: 0.72, rmse: 0.45 },
  { semestre: '2022-2', f1Score: 0.78, rmse: 0.38 },
  { semestre: '2023-1', f1Score: 0.85, rmse: 0.25 },
  { semestre: '2023-2', f1Score: 0.89, rmse: 0.18 },
  { semestre: '2024-1', f1Score: 0.92, rmse: 0.12 },
  { semestre: '2024-2', f1Score: 0.95, rmse: 0.08 },
];

export const reglasActivas = [
  { id: 1, tipo: 'Bloqueo', detalle: 'Dr. Turing NO disponible Lunes 7-9am'},
  { id: 2, tipo: 'Global', detalle: 'Hora máxima de salida: 20:00' }
];