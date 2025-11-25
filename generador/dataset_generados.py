import pandas as pd
import numpy as np
import json
import random
import datetime
import os
import sys

ANIO_INICIO = 2015
ANIOS_A_SIMULAR = 10
SEMESTRES_ANUALES = [1, 2]
N_ALUMNOS_POR_CARRERA_ANIO = 150
CALIF_MIN_APROBATORIA = 70
PROBABILIDAD_REPROBACION_BASE = 0.15
PROBABILIDAD_DESERCION_ANUAL = 0.08
PROBABILIDAD_CONFLICTO_RECURSO = 0.10

MIN_MATERIAS_POR_SEMESTRE = 4
MAX_MATERIAS_POR_SEMESTRE = 8

# ESTOS SON LOS QUE GENERAN EL CATALOGO DE MAESTROS
DEPARTAMENTOS = {
    "Sistemas": ["Dr. Alan Turing", "Dra. Ada Lovelace", "Dr. Edsger Dijkstra", "Dra. Grace Hopper",
                 "Dr. Donald Knuth", "Dr. Vint Cerf", "Dra. Radia Perlman", "Dr. Tim Berners-Lee",
                 "Dr. Bjarne Stroustrup", "Ing. Linus Torvalds", "Dra. Shafi Goldwasser", "Dra. Margaret Hamilton"],
    "Industrial": ["Ing. Henry Ford", "Ing. Taiichi Ohno", "Dra. Lillian Gilbreth", "Ing. Shigeo Shingo",
                   "Ing. W. Edwards Deming", "Dra. Irma Wyman", "Ing. Eliyahu Goldratt", "Dr. Joseph Juran",
                   "Dr. Kaoru Ishikawa", "Dr. Genichi Taguchi", "Ing. Frank Gilbreth"],
    "Ciencias Basicas": ["Dr. Isaac Newton", "Dra. Marie Curie", "Dr. Albert Einstein", "Dr. Carl Gauss",
                         "Dra. Rosalind Franklin", "Dr. Richard Feynman", "Dr. Niels Bohr", "Dra. Barbara McClintock"],
    "Administracion": ["Lic. Adam Smith", "Lic. Peter Drucker", "Lic. Mary Parker Follett",
                        "Lic. Michael Porter", "Lic. Philip Kotler"],
    "Desarrollo Humano": ["Lic. Carl Rogers", "Lic. Viktor Frankl", "Dra. Brené Brown", "Lic. Jean Piaget"]
}

SALONES_INDUSTRIAL = [{"nombre": f"IND-A{i + 1}", "capacidad": 35} for i in range(14)]
LABS_INDUSTRIAL = [{"nombre": "LAB-MANUFACTURA", "capacidad": 25}, {"nombre": "LAB-ERGONOMIA", "capacidad": 20}]
SALONES_SISTEMAS = [{"nombre": f"ISC-A{i + 1}", "capacidad": 35} for i in range(10)]
LABS_SISTEMAS = [
    {"nombre": "LAB-SO", "capacidad": 25},
    {"nombre": "LAB-REDES", "capacidad": 25},
    {"nombre": "LAB-ARQUITECTURA", "capacidad": 25},
    {"nombre": "LAB-ING-SOFTWARE", "capacidad": 30},
    {"nombre": "LAB-PROG-BASICA", "capacidad": 35},
    {"nombre": "LAB-PROG-AVANZADA", "capacidad": 30}
]
SALONES_COMPUTO_GENERAL = [{"nombre": f"CC-{i + 1}", "capacidad": 35} for i in range(2)]

HORAS_INICIO_CLASES = [7, 9, 11, 13, 15, 17]

DIAS_BLOQUE_OPCIONES = [
    ["Lunes", "Miércoles", "Viernes"],
    ["Martes", "Jueves"]
]

def cargar_materias():
    try:
        with open("materias_ISC.json", 'r', encoding='utf-8') as f:
            materias_isc_raw = json.load(f)
        with open("materias_IND.json", 'r', encoding='utf-8') as f:
            materias_ind_raw = json.load(f)

        for m in materias_ind_raw:
            if m['nombre'] == 'SIMULACIÓN':
                m['nombre'] = 'SIMULACIÓN (IND)'
                print("INFO: 'SIMULACIÓN' de IND renombrada a 'SIMULACIÓN (IND)'")
                break

        n_materias_isc = len(materias_isc_raw)
        n_materias_ind = len(materias_ind_raw)

        materias_isc = []
        for m in materias_isc_raw:
            m['nombre_materia'] = m['nombre'].upper().strip()
            m['carrera'] = 'ISC'
            m['materia_id'] = f"ISC-{m['nombre_materia']}"

            prereqs_raw = m.get('prerrequisitos')
            if prereqs_raw is None:
                prereqs_raw = m.get('prerrequisito')

            if prereqs_raw is None:
                m['prerrequisitos'] = []
            elif isinstance(prereqs_raw, str):
                m['prerrequisitos'] = [prereqs_raw.upper().strip()]
            elif isinstance(prereqs_raw, list):
                m['prerrequisitos'] = [p.upper().strip() for p in prereqs_raw]
            else:
                m['prerrequisitos'] = []

            materias_isc.append(m)

        materias_ind = []
        for m in materias_ind_raw:
            m['nombre_materia'] = m['nombre'].upper().strip()
            m['carrera'] = 'IND'
            m['materia_id'] = f"IND-{m['nombre_materia']}"

            prereqs_raw = m.get('prerrequisitos')
            if prereqs_raw is None:
                prereqs_raw = m.get('prerrequisito')

            if prereqs_raw is None:
                m['prerrequisitos'] = []
            elif isinstance(prereqs_raw, str):
                m['prerrequisitos'] = [prereqs_raw.upper().strip()]
            elif isinstance(prereqs_raw, list):
                m['prerrequisitos'] = [p.upper().strip() for p in prereqs_raw]
            else:
                m['prerrequisitos'] = []

            if m['nombre_materia'] in [mi['nombre_materia'] for mi in materias_isc]:
                m['materia_id'] = f"CB-{m['nombre_materia']}"
                for mi in materias_isc:
                    if mi['nombre_materia'] == m['nombre_materia']:
                        mi['materia_id'] = f"CB-{m['nombre_materia']}"
                        break
            materias_ind.append(m)

        materias_df = pd.DataFrame(materias_isc + materias_ind)
        prereq_map = pd.Series(materias_df.prerrequisitos.values, index=materias_df.nombre_materia).to_dict()
        return materias_df, prereq_map, n_materias_isc, n_materias_ind

    except FileNotFoundError:
        print("Error: Faltan los archivos 'materias_ISC.json' o 'materias_IND.json'.")
        sys.exit(1)


def generar_profesores_y_salones():
    profesores = []
    pid_counter = 101
    for depto, nombres in DEPARTAMENTOS.items():
        for nombre in nombres:
            calidad = np.random.choice(['Baja', 'Media', 'Alta'], p=[0.2, 0.6, 0.2])
            profesores.append({
                "profesor_id": f"P{pid_counter}",
                "nombre_profesor": nombre,
                "departamento": depto,
                "calidad_simulada": calidad
            })
            pid_counter += 1

    salones_data = SALONES_INDUSTRIAL + LABS_INDUSTRIAL + SALONES_SISTEMAS + LABS_SISTEMAS + SALONES_COMPUTO_GENERAL
    salones = []
    sid_counter = 101
    for salon in salones_data:
        salones.append({
            "salon_id": f"S{sid_counter}",
            "nombre_salon": salon['nombre'],
            "capacidad_base": salon['capacidad']
        })
        sid_counter += 1

    return pd.DataFrame(profesores), pd.DataFrame(salones)


def generar_horario_complejo(creditos, hora_inicio_preferida_idx):
    horario_json = {}
    dias_preferidos_idx = np.random.choice([0, 1], p=[0.4, 0.6])
    dias = DIAS_BLOQUE_OPCIONES[dias_preferidos_idx]
    hora_inicio = HORAS_INICIO_CLASES[hora_inicio_preferida_idx]

    if creditos <= 4:
        bloque_horario = f"{hora_inicio}-{hora_inicio + 2}"
        horario_json[dias[0]] = bloque_horario
        horario_json[dias[1]] = bloque_horario
    else:
        bloque_horario_2h = f"{hora_inicio}-{hora_inicio + 2}"
        if dias_preferidos_idx == 1:
            horario_json["Martes"] = bloque_horario_2h
            horario_json["Jueves"] = bloque_horario_2h
            quinta_hora_inicio = 7 if hora_inicio == 7 else hora_inicio - 1
            horario_json["Lunes"] = f"{quinta_hora_inicio}-{quinta_hora_inicio + 1}"
        else:
            horario_json["Lunes"] = bloque_horario_2h
            horario_json["Miércoles"] = bloque_horario_2h
            horario_json["Viernes"] = f"{hora_inicio}-{hora_inicio + 1}"

    return json.dumps(horario_json), hora_inicio


def calcular_calificacion_con_sesgo(horario_json_str, profesor_calidad, conflicto_recurso, materia_semestre):
    hora_inicio = 12
    try:
        horas_inicio = [int(v.split('-')[0]) for v in json.loads(horario_json_str).values()]
        hora_inicio = np.mean(horas_inicio)
    except:
        pass

    penalizacion_horario = 0
    if hora_inicio <= 7:
        penalizacion_horario = -15
    elif hora_inicio >= 17:
        penalizacion_horario = -10

    penalizacion_profesor = 0
    if profesor_calidad == 'Baja':
        penalizacion_profesor = -10
    elif profesor_calidad == 'Alta':
        penalizacion_profesor = 5

    penalizacion_conflicto = -20 if conflicto_recurso else 0
    penalizacion_dificultad = - (materia_semestre * 1.5)

    calif_base = np.random.normal(loc=85, scale=5)
    calif_final = calif_base + penalizacion_horario + penalizacion_profesor + penalizacion_conflicto + penalizacion_dificultad

    prob_reprobar_ajustada = PROBABILIDAD_REPROBACION_BASE - (calif_final - 80) * 0.01

    if np.random.rand() < prob_reprobar_ajustada:
        calif_final = np.random.uniform(40, CALIF_MIN_APROBATORIA - 1)
    else:
        calif_final = np.random.uniform(CALIF_MIN_APROBATORIA, 100)

    return round(max(0, min(100, calif_final)), 2)


def generar_alumnos(n, carrera, anio_inicio, start_index):
    alumnos = []
    for i in range(n):
        alumno_id = f"A{anio_inicio}{start_index + i:04d}"
        alumnos.append({
            "alumno_id": alumno_id,
            "carrera": carrera,
            "semestre_actual": 1,
            "estado": "Activo",
            "materias_aprobadas_json": json.dumps([]),
            "anio_ingreso": anio_inicio
        })
    return pd.DataFrame(alumnos)


def alumno_es_elegible(alumno_row, materia_row, prereq_map):
    if alumno_row['semestre_actual'] < materia_row['semestre']:
        return False

    materias_aprobadas = set(json.loads(alumno_row['materias_aprobadas_json']))
    if materia_row['nombre_materia'] in materias_aprobadas:
        return False

    prerrequisitos_necesarios = set(prereq_map.get(materia_row['nombre_materia'], []))

    if prerrequisitos_necesarios.issubset(materias_aprobadas):
        return True

    return False


def actualizar_estado_y_semestre(row, n_isc, n_ind):
    if row['estado'] != 'Activo':
        return row['estado'], row['semestre_actual']

    aprobadas = len(json.loads(row['materias_aprobadas_json']))

    total_necesarias = 0
    if row['carrera'] == 'ISC':
        total_necesarias = n_isc
    elif row['carrera'] == 'IND':
        total_necesarias = n_ind
    else:
        return row['estado'], row['semestre_actual']

    if aprobadas >= total_necesarias:
        return 'Egresado', row['semestre_actual']

    nuevo_semestre = row['semestre_actual'] + 1
    return 'Activo', nuevo_semestre


def simular_progresion_y_grupos(materias_df, profesores_df, salones_df, prereq_map, n_materias_isc, n_materias_ind):
    print("Iniciando simulación de 10 años...")

    if not os.path.exists("snapshots"):
        os.makedirs("snapshots")

    lista_todos_los_grupos = []
    lista_todas_las_inscripciones = []
    lista_toda_la_asistencia = []

    alumnos_df = pd.DataFrame()

    inscripcion_id_counter = 100000
    asistencia_id_counter = 9000000
    grupo_id_counter = 1000

    for anio in range(ANIO_INICIO, ANIO_INICIO + ANIOS_A_SIMULAR):

        alumno_id_counter_anual = 1

        nuevos_alumnos_isc = generar_alumnos(N_ALUMNOS_POR_CARRERA_ANIO, "ISC", anio, alumno_id_counter_anual)
        alumno_id_counter_anual += len(nuevos_alumnos_isc)

        nuevos_alumnos_ind = generar_alumnos(N_ALUMNOS_POR_CARRERA_ANIO, "IND", anio, alumno_id_counter_anual)

        alumnos_df = pd.concat([alumnos_df, nuevos_alumnos_isc, nuevos_alumnos_ind], ignore_index=True)
        alumnos_df = alumnos_df[alumnos_df['estado'] == 'Activo'].copy()

        for semestre in SEMESTRES_ANUALES:
            semestre_historico_id = f"{anio}-{semestre}"
            print(f"--- Simulando Semestre: {semestre_historico_id} ---")

            snapshot_path = f"snapshots/alumnos_{semestre_historico_id}.csv"
            print(f"Guardando snapshot en {snapshot_path}")
            alumnos_df.to_csv(snapshot_path, index=False)

            grupos_semestre_actual = []
            recursos_ocupados = {}

            for idx, materia in materias_df.iterrows():

                alumnos_elegibles_idx = alumnos_df.apply(
                    alumno_es_elegible,
                    axis=1,
                    materia_row=materia,
                    prereq_map=prereq_map
                )
                alumnos_elegibles = alumnos_df[alumnos_elegibles_idx]
                demanda_potencial = len(alumnos_elegibles)

                if demanda_potencial == 0:
                    continue

                if "CÁLCULO" in materia['nombre_materia'] or "ÁLGEBRA" in materia['nombre_materia'] or "ECUACIONES" in \
                        materia['nombre_materia'] or "FÍSICA" in materia['nombre_materia'] or "QUÍMICA" in materia[
                    'nombre_materia'] or "PROBABILIDAD" in materia['nombre_materia'] or "MÉTODOS" in materia[
                    'nombre_materia'] or "MATEMÁTICAS" in materia['nombre_materia']:
                    depto_profesor = "Ciencias Basicas"
                elif materia['carrera'] == 'ISC':
                    depto_profesor = "Sistemas"
                elif materia['carrera'] == 'IND':
                    depto_profesor = "Industrial"
                elif "ADMINISTRACIÓN" in materia['nombre_materia'] or "CONTABILIDAD" in materia[
                    'nombre_materia'] or "ECONOMÍA" in materia['nombre_materia'] or "MERCADOTECNIA" in materia[
                    'nombre_materia'] or "CULTURA" in materia['nombre_materia']:
                    depto_profesor = "Administracion"
                else:
                    depto_profesor = "Desarrollo Humano"

                profesores_disponibles = profesores_df[profesores_df['departamento'] == depto_profesor]
                if profesores_disponibles.empty:
                    profesores_disponibles = profesores_df

                if "LAB" in materia['nombre_materia'] or "TALLER" in materia['nombre_materia'] or "DIBUJO" in materia[
                    'nombre_materia']:
                    salones_disponibles = salones_df[salones_df['nombre_salon'].str.contains("LAB|CC")]
                else:
                    salones_disponibles = salones_df[~salones_df['nombre_salon'].str.contains("LAB|CC")]

                capacidad_grupo = 25 if materia['semestre'] > 5 else 35
                num_grupos_necesarios = int(np.ceil(demanda_potencial / capacidad_grupo))

                for _ in range(num_grupos_necesarios):
                    profesor = profesores_disponibles.sample(1).iloc[0]
                    salon = salones_disponibles.sample(1).iloc[0]

                    capacidad_real = min(capacidad_grupo, salon['capacidad_base'])
                    hora_idx = np.random.randint(0, len(HORAS_INICIO_CLASES))
                    horario_json, hora_inicio = generar_horario_complejo(materia['creditos'], hora_idx)

                    conflicto_detectado = False
                    if np.random.rand() < PROBABILIDAD_CONFLICTO_RECURSO:
                        conflicto_detectado = True

                    grupo_id = f"G{grupo_id_counter}"
                    grupo_id_counter += 1

                    grupo_actual = {
                        "grupo_id": grupo_id,
                        "semestre_historico": semestre_historico_id,
                        "carrera": materia['carrera'],
                        "materia_id": materia['materia_id'],
                        "materia_nombre": materia['nombre_materia'],
                        "materia_semestre": materia['semestre'],
                        "profesor_id": profesor['profesor_id'],
                        "profesor_calidad_simulada": profesor['calidad_simulada'],
                        "salon_id": salon['salon_id'],
                        "cupo_ofrecido": capacidad_real,
                        "horario_json": horario_json,
                        "hora_inicio_promedio": hora_inicio,
                        "conflicto_recurso_detectado": conflicto_detectado
                    }
                    grupos_semestre_actual.append(grupo_actual)

            lista_todos_los_grupos.extend(grupos_semestre_actual)
            df_grupos_semestre = pd.DataFrame(grupos_semestre_actual)

            nuevas_materias_aprobadas_map = {}

            for idx, alumno in alumnos_df.iterrows():

                if np.random.rand() < PROBABILIDAD_DESERCION_ANUAL / 2:
                    alumnos_df.at[idx, 'estado'] = 'Desercion'
                    continue

                materias_elegibles_idx = materias_df.apply(
                    lambda m: (m['carrera'] == alumno['carrera']) and \
                              alumno_es_elegible(alumno, m, prereq_map),
                    axis=1
                )
                materias_elegibles_df = materias_df[materias_elegibles_idx]

                if materias_elegibles_df.empty:
                    continue

                materias_reprobadas = materias_elegibles_df[
                    materias_elegibles_df['semestre'] < alumno['semestre_actual']
                    ]
                materias_regulares = materias_elegibles_df[
                    materias_elegibles_df['semestre'] == alumno['semestre_actual']
                    ]

                num_materias_a_tomar = np.random.randint(MIN_MATERIAS_POR_SEMESTRE, MAX_MATERIAS_POR_SEMESTRE + 1)

                carga_final_df = pd.concat([materias_reprobadas, materias_regulares])

                if len(carga_final_df) > num_materias_a_tomar:
                    weights = [5 if s < alumno['semestre_actual'] else 1 for s in carga_final_df['semestre']]
                    if sum(weights) == 0:
                        materias_seleccionadas_df = carga_final_df.sample(n=num_materias_a_tomar)
                    else:
                        materias_seleccionadas_df = carga_final_df.sample(n=num_materias_a_tomar, weights=weights)
                else:
                    materias_seleccionadas_df = carga_final_df

                for _, materia_a_tomar in materias_seleccionadas_df.iterrows():

                    grupos_disponibles = df_grupos_semestre[
                        df_grupos_semestre['materia_id'] == materia_a_tomar['materia_id']
                        ]

                    if grupos_disponibles.empty:
                        continue

                    grupo_elegido = grupos_disponibles.sample(1).iloc[0]

                    calif_final = calcular_calificacion_con_sesgo(
                        grupo_elegido['horario_json'],
                        grupo_elegido['profesor_calidad_simulada'],
                        grupo_elegido['conflicto_recurso_detectado'],
                        grupo_elegido['materia_semestre']
                    )

                    estado_desercion = calif_final < CALIF_MIN_APROBATORIA

                    lista_todas_las_inscripciones.append({
                        "inscripcion_id": f"I{inscripcion_id_counter}",
                        "grupo_id": grupo_elegido['grupo_id'],
                        "alumno_id": alumno['alumno_id'],
                        "calificacion_final": calif_final,
                        "estado_desercion": estado_desercion,
                    })
                    inscripcion_id_counter += 1

                    num_asistencias = int(materia_a_tomar['creditos'] * 16)
                    for _ in range(num_asistencias):
                        tipo_evento = "Entrada"
                        if grupo_elegido['hora_inicio_promedio'] <= 7 and np.random.rand() < 0.3:
                            tipo_evento = "Ausencia"
                        elif grupo_elegido['hora_inicio_promedio'] >= 17 and np.random.rand() < 0.2:
                            tipo_evento = "Ausencia"

                        lista_toda_la_asistencia.append({
                            "asistencia_id": f"AS{asistencia_id_counter}",
                            "alumno_id": alumno['alumno_id'],
                            "grupo_id": grupo_elegido['grupo_id'],
                            "fecha_hora_evento": f"{semestre_historico_id}-D{np.random.randint(1, 28)}",
                            "tipo_evento": tipo_evento
                        })
                        asistencia_id_counter += 1

                    if calif_final >= CALIF_MIN_APROBATORIA:
                        if alumno['alumno_id'] not in nuevas_materias_aprobadas_map:
                            nuevas_materias_aprobadas_map[alumno['alumno_id']] = []
                        nuevas_materias_aprobadas_map[alumno['alumno_id']].append(materia_a_tomar['nombre_materia'])

            def actualizar_aprobadas(row):
                if row['alumno_id'] in nuevas_materias_aprobadas_map:
                    aprobadas_actuales = set(json.loads(row['materias_aprobadas_json']))
                    aprobadas_actuales.update(nuevas_materias_aprobadas_map[row['alumno_id']])
                    return json.dumps(list(aprobadas_actuales))
                return row['materias_aprobadas_json']

            alumnos_df['materias_aprobadas_json'] = alumnos_df.apply(actualizar_aprobadas, axis=1)

            nuevos_valores = alumnos_df.apply(
                actualizar_estado_y_semestre,
                axis=1,
                n_isc=n_materias_isc,
                n_ind=n_materias_ind,
                result_type='expand'
            )

            alumnos_df['estado'] = nuevos_valores[0]
            alumnos_df['semestre_actual'] = nuevos_valores[1]

    print("Simulación de 10 años completada.")

    df_grupos = pd.DataFrame(lista_todos_los_grupos)
    df_inscripciones = pd.DataFrame(lista_todas_las_inscripciones)
    df_asistencia = pd.DataFrame(lista_toda_la_asistencia)

    return df_grupos, df_inscripciones, df_asistencia, alumnos_df

if __name__ == "__main__":
    print("Cargando planes de estudio...")
    materias_df, prereq_map, n_isc, n_ind = cargar_materias()

    print("Generando catálogos de profesores y salones...")
    profesores_df, salones_df = generar_profesores_y_salones()

    encoding_csv = 'utf-8-sig'

    print(f"Guardando catalogo_profesores.csv ({len(profesores_df)} filas)...")
    profesores_df.to_csv("catalogo_profesores.csv", index=False, encoding=encoding_csv)

    print(f"Guardando catalogo_salones.csv ({len(salones_df)} filas)...")
    salones_df.to_csv("catalogo_salones.csv", index=False, encoding=encoding_csv)

    df_grupos, df_inscripciones, df_asistencia, df_alumnos_final = simular_progresion_y_grupos(
        materias_df, profesores_df, salones_df, prereq_map, n_isc, n_ind
    )

    print(f"Guardando grupos_historicos.csv ({len(df_grupos)} filas)...")
    df_grupos.to_csv("grupos_historicos.csv", index=False, encoding=encoding_csv)

    print(f"Guardando inscripciones.csv ({len(df_inscripciones)} filas)...")
    df_inscripciones.to_csv("inscripciones.csv", index=False, encoding=encoding_csv)

    print(f"Guardando asistencia.csv ({len(df_asistencia)} filas)...")
    df_asistencia.to_csv("asistencia.csv", index=False, encoding=encoding_csv)

    print(f"Guardando alumnos.csv (estado final) ({len(df_alumnos_final)} filas)...")
    df_alumnos_final.to_csv("alumnos.csv", index=False, encoding=encoding_csv)

    print(f"Total de snapshots de alumnos guardados en la carpeta /snapshots: {len(os.listdir('snapshots'))}")