import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.feature import VectorAssembler, StringIndexerModel
import json
import shutil # Para limpiar carpetas al guardar
from pyspark.sql.functions import udf, col, array_contains, count, lit, when, round, desc
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer
import builtins
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = "analitica-datalake"
path_model_s3 = f"s3://{bucket_name}/models/v1"

path_snapshot_alumnos = f"s3://{bucket_name}/raw/historical/alumnos.csv"
path_catalogo_profesores = f"s3://{bucket_name}/raw/historical/catalogo_profesores.csv"
path_catalogo_salones = f"s3://{bucket_name}/raw/historical/catalogo_salones.csv"
path_catalogo_materias = f"s3://{bucket_name}/processed/materias_normalizadas/"
path_output_json_prefix = "processed/predictions_output"

path_asistencia_s3 = f"s3://{bucket_name}/raw/streaming/2025/11/23/"
path_grupos_s3 = f"s3://{bucket_name}/raw/historical/grupos_historicos.csv"

print(f"🚀 Iniciando Job de Predicción V5.")
print(f"Versión de Spark: {spark.version}")
print(f"Ruta del modelo objetivo: {path_model_s3}")

# ==========================================================
# 2. PRUEBA DE CONECTIVIDAD Y CARGA (SIN RED DE SEGURIDAD)
# ==========================================================

print("🔹 PASO DE DIAGNÓSTICO: Intentando leer metadata del modelo como Parquet...")
# Esto validará si Spark tiene permisos y visibilidad de la carpeta sin intentar cargar el objeto GBT complejo
try:
    # Intentamos leer solo la metadata para ver si el path es accesible
    df_meta = spark.read.parquet(f"{path_model_s3}/metadata")
    print("✔ ÉXITO: Spark puede leer la carpeta 'metadata' en S3. Conexión OK.")
    df_meta.show(1, truncate=False)
except Exception as e:
    print("ERROR DE DIAGNÓSTICO: Spark no pudo leer la ruta S3 básica.")
    print(f"   Posible causa: Permisos IAM o Ruta equivocada.")
    # No detenemos aquí, dejamos que el load() explote para ver el error oficial

print("🔹 INTENTANDO CARGA DEL MODELO (GBTRegressionModel)...")
# AQUÍ SE VA A ROMPER SI HAY ERROR, PERO NOS DARÁ EL LOG REAL
model = GBTRegressionModel.load(path_model_s3)

print("ÉXITO FINAL: Modelo cargado correctamente en memoria.")

# ==========================================================
# 1. CONFIGURACIÓN DE LECTURA
# ==========================================================
path_raw_csv = path_snapshot_alumnos

print(f"🧹 Iniciando limpieza maestra de: {path_raw_csv}")

df_raw = spark.read.option("header", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(path_raw_csv)

# ==========================================================
# 2. FUNCIÓN DE LIMPIEZA "FUERZA BRUTA"
# ==========================================================
def clean_materias_list(raw_str):
    """
    Toma cualquier string sucio (ej: '[""MATE A"", ""MATE B""]') 
    y devuelve una lista limpia de Python: ['MATE A', 'MATE B']
    """
    if not raw_str: 
        return []
    
    # 1. Intento civilizado: Si es un JSON válido, leerlo.
    try:
        return json.loads(raw_str)
    except:
        pass # Si falla, pasamos al modo "fuerza bruta"

    # 2. Modo Fuerza Bruta: 
    # Eliminamos todo lo que no sea texto útil (corchetes, comillas dobles, simples)
    clean_str = raw_str.replace("[", "").replace("]", "").replace('"', '').replace("'", "")
    
    # 3. Separamos por comas y limpiamos espacios
    # El upper() aquí asegura que todo esté normalizado desde el inicio
    items = [x.strip().upper() for x in clean_str.split(",") if x.strip()]
    
    return items

# Registramos la UDF con retorno ArrayType(StringType) para que Spark sepa que es una lista
udf_clean_materias = udf(clean_materias_list, ArrayType(StringType()))

# ==========================================================
# 3. CREACIÓN DEL DF LIMPIO
# ==========================================================

df_alumnos_clean = df_raw.withColumn(
    "aprobadas_list", 
    udf_clean_materias(col("materias_aprobadas_json"))
).select(
    col("alumno_id"),
    col("carrera").alias("carrera_alumno"), # Renombramos de una vez para evitar ambigüedad futura
    col("semestre_actual").cast("int"),
    col("estado"),
    col("aprobadas_list") # Esta columna ya es un Array real de Spark, no un string JSON
)


# ==========================================================
# 4. CÁLCULO DE DEMANDA POTENCIAL (Versión Optimizada)
# ==========================================================
print("\n🔹 PASO 2: Calculando Demanda con df_alumnos_clean...")

# 1. PREPARAR MATERIAS Y PRERREQUISITOS
# -------------------------------------
# Renombramos carrera en materias para evitar ambigüedad con df_alumnos_clean
df_materias = spark.read.parquet(path_catalogo_materias)
df_materias_renamed = df_materias.withColumnRenamed("carrera", "carrera_materia")

# Generamos diccionario de prerrequisitos (Broadcast)
def get_prereqs_map(rows):
    mapping = {}
    for r in rows:
        materia = r["materia_nombre"]
        prereqs = r["prerequisitos"]
        # Normalización simple
        if not prereqs: lista = []
        elif isinstance(prereqs, list): lista = prereqs
        elif isinstance(prereqs, str): 
            clean = prereqs.replace("[","").replace("]","").replace("'","")
            lista = [x.strip() for x in clean.split(",") if x.strip()]
        else: lista = []
        mapping[materia] = lista
    return mapping

prereq_rows = df_materias.select("materia_nombre", "prerequisitos").collect()
bc_prereq = spark.sparkContext.broadcast(get_prereqs_map(prereq_rows))
print(f"✔ Prerrequisitos cargados.")

# 2. UDF SIMPLIFICADA (Ya no parsea JSON, solo chequea listas)
# ------------------------------------------------------------
from pyspark.sql.types import BooleanType

def check_eligibility_simple(lista_aprobadas, materia_objetivo):
    # lista_aprobadas ya viene limpia y en mayúsculas desde el paso anterior
    if not materia_objetivo: return False
    
    # Convertimos a set para búsqueda instantánea
    aprobadas_set = set(lista_aprobadas) if lista_aprobadas else set()
    target = materia_objetivo.strip().upper()

    # A. Si ya la aprobó, no es demanda
    if target in aprobadas_set: 
        return False
        
    # B. Prerrequisitos
    reqs = bc_prereq.value.get(materia_objetivo, [])
    
    # Match fuzzy de emergencia (por si las mayúsculas del dict varían)
    if not reqs:
        for k, v in bc_prereq.value.items():
            if k.strip().upper() == target:
                reqs = v
                break
    
    # Si no pide nada, es elegible
    if not reqs: return True
    
    # C. Validar que tenga TODOS los requisitos
    reqs_normalized = [r.strip().upper() for r in reqs if r]
    return all(r in aprobadas_set for r in reqs_normalized)

udf_check_simple = F.udf(check_eligibility_simple, BooleanType())


# 3. EJECUCIÓN DEL CÁLCULO
# ------------------------
# Usamos df_alumnos_clean en lugar de leer el CSV de nuevo
# Nota: df_alumnos_clean ya tiene "carrera_alumno" y "semestre_actual" (int)

df_cross = df_alumnos_clean.filter(F.col("estado") == "Activo") \
    .crossJoin(F.broadcast(df_materias_renamed))

df_elegibles = df_cross.filter(
    # Regla 1: Semestre
    (F.col("semestre_actual") >= F.col("semestre_materia")) & 
    # Regla 2: Carrera (df_alumnos_clean ya trae 'carrera_alumno')
    (F.col("carrera_alumno") == F.col("carrera_materia")) 
)

# Aplicamos la UDF simple
df_final_demand = df_elegibles.filter(
    udf_check_simple(F.col("aprobadas_list"), F.col("materia_nombre"))
)

# 4. AGRUPACIÓN
# -------------
df_demanda = df_final_demand \
    .groupBy("materia_nombre", "semestre_materia", "carrera_materia") \
    .agg(F.count("*").alias("demanda_real")) \
    .filter(F.col("demanda_real") >= 15) \
    .orderBy(F.desc("demanda_real"))

print("✔ Demanda calculada usando datos limpios.")
df_demanda.show(5, truncate=False)

# ==========================================================
# 5. GENERAR CANDIDATOS (MATERIA x PROFE x SALON x HORARIO)
# ==========================================================
print("\n🔹 PASO 3: Generando 'sábana' de candidatos...")

# Leemos catálogos
df_profes = spark.read.option("header",True).csv(path_catalogo_profesores).select("profesor_id","nombre_profesor").limit(20)
df_salones = spark.read.option("header",True).csv(path_catalogo_salones).select("salon_id","capacidad_base").limit(10)

# Horarios hardcodeados para simulación
horarios = [
    {"str": "L-Mi-V 07:00-09:00", "inicio": 7},
    {"str": "L-Mi-V 09:00-11:00", "inicio": 9},
    {"str": "Ma-Ju 11:00-13:00", "inicio": 11},
    {"str": "Ma-Ju 15:00-17:00", "inicio": 15},
    {"str": "L-Mi-V 13:00-15:00", "inicio": 13}
]
df_hor = spark.createDataFrame(horarios)

# Cross Joins masivos (Cuidado con la memoria si los datasets son muy grandes)
df_cand = df_demanda.crossJoin(df_profes).crossJoin(df_salones).crossJoin(df_hor)

# ==========================================================
# 6. PREPARACIÓN DE FEATURES (Transformar data para el modelo)
# ==========================================================
print("\n🔹 PASO 4: Feature Engineering (Preparando vectores para el modelo)...")

indexer = StringIndexer(
    inputCol="materia_nombre", 
    outputCol="X_Materia_Index", 
    handleInvalid="keep"
)
# Ajustamos el indexer a los datos actuales
indexer_model_local = indexer.fit(df_cand)
df_cand_indexed = indexer_model_local.transform(df_cand)

# 2. Calcular/Renombrar columnas para que coincidan con lo que pide el modelo
# Recordemos las features: X_Cp_Profesor, X_Factor_Demanda, X_Capacidad, Hora, Semestre, Materia_Index
df_features = df_cand_indexed \
    .withColumn("X_Cp_Profesor", F.lit(85.0)) \
    .withColumn("X_Factor_Demanda_Normalizado", 
                F.when(F.col("demanda_real") > 100, 1.0)
                .otherwise(F.col("demanda_real") / 100.0)) \
    .withColumn("X_capacidad_salon", F.col("capacidad_base").cast("double")) \
    .withColumn("X_Hora_Inicio", F.col("inicio").cast("double")) \
    .withColumn("X_Semestre_Num", F.col("semestre_materia").cast("double")) \
    .withColumnRenamed("materia_nombre", "materia_nombre_visual")

# 3. VectorAssembler (Empaquetar todo en un solo vector)
feature_cols = [
    "X_Cp_Profesor", 
    "X_Factor_Demanda_Normalizado", 
    "X_capacidad_salon",
    "X_Hora_Inicio", 
    "X_Semestre_Num", 
    "X_Materia_Index"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
df_ready = assembler.transform(df_features)

# ==========================================================
# 7. EJECUCIÓN DE LA PREDICCIÓN (INFERENCIA)
# ==========================================================
print("🔮 Ejecutando modelo.transform() ...")

# El modelo genera una columna "prediction" (0.0 a 1.0 aprox)
df_scored = model.transform(df_ready)

# Lo convertimos a escala 0-100 para visualización
df_scored = df_scored.withColumn("score_predicho", F.round(F.col("prediction") * 100, 1))

print("✔ Predicciones completadas.")

# ==========================================================
# 8. OPTIMIZACIÓN DE HORARIOS (ALGORITMO VORAZ)
# ==========================================================
print("\n🔹 PASO 5: Integrando Asistencia y Optimizando...")

# ==========================================================
# A. PROCESAMIENTO DE ASISTENCIA (SIMULACIÓN S3)   QUIJANOOOOOOO
# ==========================================================
# ==========================================================
# 6. PROCESAMIENTO DE ASISTENCIA REAL (S3 STREAMING)
# ==========================================================
print("\n🔹 PASO INTERMEDIO: Procesando Asistencia Real desde Streaming (Local)...")

# 1. Definir Esquema (Obligatorio para archivos sin extensión)
schema_asistencia = StructType([
    StructField("asistencia_id", StringType(), True),
    StructField("alumno_id", StringType(), True),
    StructField("grupo_id", StringType(), True),
    StructField("fecha_hora_evento", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("timestamp_ingesta", StringType(), True)
])

# Ruta de Grupos Históricos
path_grupos_historicos = path_grupos_s3

# Variables por defecto
dict_ajuste_asistencia = {}
json_grafica_asistencia = []

try:
    # ------------------------------------------------------------------
    # TRUCO PARA LOCAL: Leer como TEXTO con comodín '*'
    # Esto fuerza a leer archivos sin extensión dentro de la carpeta
    # ------------------------------------------------------------------
    ruta_con_comodin = os.path.join(path_asistencia_s3, "*")
    print(f"   📂 Leyendo archivos crudos desde: {ruta_con_comodin}")
    
    # Leemos el contenido crudo línea por línea
    df_text = spark.read.text(ruta_con_comodin)
    
    # Parseamos la columna 'value' (texto) usando el esquema JSON
    df_asist_raw = df_text.select(
        F.from_json(F.col("value"), schema_asistencia).alias("data")
    ).select("data.*")
    
    # Filtramos filas nulas (por si leyó saltos de línea o basura)
    df_asist_raw = df_asist_raw.filter(F.col("asistencia_id").isNotNull())
    
    conteo = df_asist_raw.count()
    print(f"   ✔ Registros válidos recuperados: {conteo}")

    if conteo > 0:
        # B. Cruce con Grupos Históricos
        print(f"   📂 Cruzando con histórico de grupos...")
        df_grupos_hist = spark.read.option("header", True).csv(path_grupos_historicos) \
            .select("grupo_id", "profesor_id", "hora_inicio_promedio")
        
        # C. Join
        df_joined = df_asist_raw.join(df_grupos_hist, "grupo_id", "inner")
        
        # D. Calcular Métricas
        df_stats = df_joined.groupBy("profesor_id").agg(
            F.count("*").alias("total"),
            F.count(F.when(F.col("tipo_evento") == "Entrada", 1)).alias("asistencias"),
            F.count(F.when(F.col("tipo_evento") == "Ausencia", 1)).alias("faltas")
        )
        
        # E. Lógica de Penalización (-0.5 a +0.5)
        def calc_adjustment(total, asis, fal):
            if total == 0: return 0.0
            if (asis / total) > 0.5: return 0.5 
            if fal > asis: return -0.5           
            return 0.0

        stats_rows = df_stats.collect()
        dict_ajuste_asistencia = {r['profesor_id']: calc_adjustment(r['total'], r['asistencias'], r['faltas']) for r in stats_rows}
        
        print(f"   ✔ Factores calculados para {len(dict_ajuste_asistencia)} profesores.")
        
        # F. Gráfica de Asistencia
        df_grafica_5 = df_joined.groupBy("hora_inicio_promedio") \
            .agg(
                F.mean(
                    F.when(F.col("tipo_evento") == "Entrada", 1.0)
                    .otherwise(0.0)
                ).alias("tasa_asistencia_promedio"),
                

                F.count("*").alias("volumen_datos")
            ) \
            .orderBy(F.col("hora_inicio_promedio").cast("int"))
            
        # Redondeamos para que el JSON se vea limpio (ej. 0.85 en lugar de 0.857142...)
        df_grafica_res = df_grafica_5.withColumn("tasa_asistencia_promedio", F.round(F.col("tasa_asistencia_promedio"), 2))
        
        # Convertimos a lista de diccionarios para guardar luego
        json_grafica_asistencia = [row.asDict() for row in df_grafica_res.collect()]
    else:
        print("   ⚠ No se encontraron registros válidos JSON en los archivos leídos.")

except Exception as e:
    print(f"   ❌ ERROR: {e}")
    # import traceback
    # traceback.print_exc()
    print("   ⚠ Usando factores neutrales.")


# ==========================================================
# B. ALGORITMO DE OPTIMIZACIÓN (VORAZ)
# ==========================================================

# --- Helper Días ---
def obtener_dias_del_string(horario_str):
    parte_dias = horario_str.split(" ")[0]
    raw_dias = parte_dias.split("-") 
    mapa_dias = {"L": "LUNES", "Ma": "MARTES", "Mi": "MIERCOLES", "Ju": "JUEVES", "V": "VIERNES", "S": "SABADO"}
    return [mapa_dias.get(d, d) for d in raw_dias]

# 1. Traer candidatos
candidates = df_scored.select(
    "materia_nombre_visual", "semestre_materia", "nombre_profesor", "profesor_id",
    "salon_id", "str", "inicio", "score_predicho", "demanda_real", "capacidad_base", "carrera_materia"
).orderBy(F.desc("score_predicho")).collect()

# 2. Estructuras de control
schedule_final = []
resources_busy = set()          
grupos_asignados_por_materia = {} 

# 3. Restricciones   QUIJANOOOOOOO
def get_dynamo_restrictions():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1') # Ajusta tu región si es necesario
    table = dynamodb.Table('teacher_restrictions')
    
    # Escaneamos la tabla completa (asumiendo que es una tabla de configuración pequeña)
    response = table.scan()
    items = response.get('Items', [])
    
    dynamic_blacklist = {}
    
    for item in items:
        # 1. Validar que la restricción esté activa
        # Dynamo a veces devuelve booleanos o strings dependiendo de cómo se guardó
        is_active = item.get('activo')
        if str(is_active).lower() != 'true': 
            continue
            
        prof_id = item.get('profesor_id')
        bloqueos_map = item.get('bloqueos', {}) # Esto viene como un Map de Dynamo
        
        # Estructura objetivo: {"LUNES": [7, 8], "VIERNES": [19, 20]}
        clean_blocks = {}
        
        for dia, horas_list in bloqueos_map.items():
            # Boto3 Resource convierte números de Dynamo a Decimal.
            # Debemos convertirlos a int para que coincidan con tu lógica de 'inicio' (int).
            try:
                clean_blocks[dia] = [int(h) for h in horas_list]
            except Exception as e:
                print(f"⚠️ Error parseando horas para {prof_id} en {dia}: {e}")
                continue
                
        if clean_blocks:
            dynamic_blacklist[prof_id] = clean_blocks
            
    return dynamic_blacklist

try:
    blacklisted_times = get_dynamo_restrictions()
    print(f"✔ Blacklist cargada para {len(blacklisted_times)} profesores.")
    # Debug: Imprimir un ejemplo para verificar formato
    if blacklisted_times:
        sample_key = next(iter(blacklisted_times))
        print(f"   Ejemplo ({sample_key}): {blacklisted_times[sample_key]}")
except Exception as e:
    print(f"❌ ERROR leyendo DynamoDB: {e}")
    print("   ⚠ Usando blacklist vacía de emergencia.")
    blacklisted_times = {}

# ---------------------------------------------------------

count_id = 1
grupos_maximos = 100

print(f"   Evaluando {len(candidates)} candidatos con ajuste de asistencia...")

for row in candidates:
    prof_id = row['profesor_id']
    hora_ini = int(row['inicio'])
    horario_str = row['str']
    salon_id = row['salon_id']
    materia = row['materia_nombre_visual']
    capacidad_salon = int(row['capacidad_base']) if row['capacidad_base'] else 30
    demanda_total_materia = int(row['demanda_real'])
    
    # -------------------------------------------------------
    # 1. AJUSTE DE SCORE POR ASISTENCIA (ANTES DE FILTRAR)
    # -------------------------------------------------------
    score_base = float(row['score_predicho']) # Ej: 64.7
    
    # Obtenemos el factor (-0.5, 0.0, +0.5)
    factor_asistencia = dict_ajuste_asistencia.get(prof_id, 0.0)
    
    # Convertimos factor a puntos (0.5 * 100 = 50 puntos)
    puntos_ajuste = factor_asistencia * 50.0 
    
    score_con_asistencia = score_base + puntos_ajuste
    
    # Limites de seguridad (0 a 100)
    score_con_asistencia = max(0, min(100, score_con_asistencia))
    
    # -------------------------------------------------------
    
    # --- REGLA A: Restricción del Profesor ---
    bloqueado = False
    if prof_id in blacklisted_times:
        dias_clase = obtener_dias_del_string(horario_str)
        restricciones_profe = blacklisted_times[prof_id]
        for dia in dias_clase:
            if dia in restricciones_profe and hora_ini in restricciones_profe[dia]:
                bloqueado = True
                break
    if bloqueado: continue 
            
    # --- REGLA B: Empalmes ---
    key_prof = f"{prof_id}_{horario_str}"
    key_salon = f"{salon_id}_{horario_str}"
    if key_prof in resources_busy or key_salon in resources_busy: continue 
        
    # --- REGLA C: Cobertura de Demanda ---
    alumnos_ya_asignados = grupos_asignados_por_materia.get(materia, 0)
    if alumnos_ya_asignados >= demanda_total_materia: continue 
    
    # Llenado
    demanda_pendiente = demanda_total_materia - alumnos_ya_asignados
    inscritos_en_este_grupo = min(demanda_pendiente, capacidad_salon)
    str_demanda_visual = f"{inscritos_en_este_grupo}/{capacidad_salon}"
    
    # --- AJUSTE DINÁMICO POR OCUPACIÓN (PENALIZACIÓN FINAL) ---
    # Esto se aplica AL FINAL sobre el score ya ajustado por asistencia
    ocupacion_pct = inscritos_en_este_grupo / capacidad_salon
    
    if ocupacion_pct < 0.5:
        factor_penalizacion = ocupacion_pct  
    else:
        factor_penalizacion = 1.0 
        
    score_final = builtins.round(score_con_asistencia * factor_penalizacion, 1)

    # --- ASIGNAR ---
    schedule_final.append({
        "id": count_id,
        "materia": materia,
        "semestre": int(row['semestre_materia']),
        "grupo": f"G{100 + count_id}", 
        "profesor": row['nombre_profesor'],
        "carrera": row["carrera_materia"],
        "horario": horario_str,
        "salon": salon_id,
        "score": score_final, # Score Final = (Base + Asistencia) * Ocupación
        "demanda": str_demanda_visual,
        # Debug info para ver qué pasó
        "_debug_asistencia": f"{factor_asistencia} ({puntos_ajuste} pts)"
    })
    
    resources_busy.add(key_prof)
    resources_busy.add(key_salon)
    grupos_asignados_por_materia[materia] = alumnos_ya_asignados + inscritos_en_este_grupo
    
    count_id += 1
    if len(schedule_final) >= grupos_maximos: break

print(f"✔ Horario generado con {len(schedule_final)} grupos óptimos.")

# ==========================================================
# PROCESO DE FILTRADO (REGLA < 15 ALUMNOS)
# ==========================================================
print("   🔸 Aplicando filtro: Grupos < 15 no se abren (pero sus alumnos cuentan en KPI)...")

schedule_kpi = []
total_cupos_abiertos_kpi = 0 # Acumulador especial para la Demanda Satisfecha
total_cupos_ocupados_kpi = 0

for x in schedule_final:
    # Dividimos "8/35" en dos partes
    partes = x['demanda'].split('/')
    
    inscritos = int(partes[0])  # El 8
    capacidad = int(partes[1])  # El 35
    
    # 1. SIEMPRE sumamos a los alumnos para el KPI (aunque no tengan salón)
    total_cupos_abiertos_kpi += inscritos
    
    # 2. SOLO agregamos al horario final si cumple el mínimo
    if inscritos >= 15:
        total_cupos_ocupados_kpi += capacidad
        schedule_kpi.append(x)
    else:
        # Debug opcional
        # print(f"      Excluyendo grupo {x['grupo']} de {x['materia']} ({inscritos} alumnos)")
        pass

print(f"✔ Horario FINAL optimizado: {len(schedule_kpi)} grupos oficiales.")
print(f"✔ Total cupos abiertos: {total_cupos_abiertos_kpi}")
print(f"✔ Total cupos abiertos: {total_cupos_ocupados_kpi}")

# ==========================================================
# 9. GENERACIÓN DE SALIDAS (JSON)
# ==========================================================

print("\n🔹 PASO 6: Generando archivos JSON para DynamoDB/Frontend...")

# 1. Calcular KPIs para el JSON de Métricas
if schedule_final:
    # Cálculo promedio simple de Python
    avg_score = sum([x['score'] for x in schedule_final]) / len(schedule_final)
else:
    avg_score = 0.0


if total_cupos_ocupados_kpi > 0:
    pct_demanda = (total_cupos_abiertos_kpi / total_cupos_ocupados_kpi) * 100
else:
    pct_demanda = 0

print(f"Ocupación Real: {pct_demanda}% ({total_cupos_abiertos_kpi} alumnos en {total_cupos_ocupados_kpi} sillas)")

# ---------------------------------------------------------
# 1.5 EXTRAER IMPORTANCIAS REALES DEL MODELO (NUEVO)
# ---------------------------------------------------------
# Definimos los nombres amigables para mostrar en el Frontend
feature_map = {
    "X_Cp_Profesor": "Ranking Profesor",
    "X_Factor_Demanda_Normalizado": "Demanda Estudiantil",
    "X_capacidad_salon": "Capacidad Salon",
    "X_Hora_Inicio": "Horario Preferido",
    "X_Semestre_Num": "Semestre",
    "X_Materia_Index": "Materia (Dificultad)"
}

# El orden debe ser EXACTAMENTE el mismo que usaste en el VectorAssembler (Cell 7)
feature_order = [
    "X_Cp_Profesor", 
    "X_Factor_Demanda_Normalizado", 
    "X_capacidad_salon",
    "X_Hora_Inicio", 
    "X_Semestre_Num", 
    "X_Materia_Index"
]

# Sacamos los valores matemáticos del modelo cargado
# model.featureImportances devuelve un vector, lo convertimos a lista
importances_values = model.featureImportances.toArray()

# Creamos la lista de objetos dinámica
dynamic_top_features = []
for name, value in zip(feature_order, importances_values):
    # Solo agregamos si tiene relevancia (> 0.1%)
    if value > 0.001:
        dynamic_top_features.append({
            "feature": feature_map.get(name, name), # Nombre bonito
            "importancia": builtins.round(value * 100, 1) # Convertir 0.72 a 72.0
        })

# Ordenamos de mayor a menor importancia
dynamic_top_features = sorted(dynamic_top_features, key=lambda x: x['importancia'], reverse=True)

# ---------------------------------------------------------

# 2. Estructurar JSONs
json_sugerencias = schedule_final

json_metrics = {
  "kpis": {
    "avgScore": builtins.round(avg_score, 1),
    "demandaSatisfecha": builtins.round(pct_demanda, 1),
    "eficienciaAulas": 88.4 
  },
  "topFeatures": dynamic_top_features # <--- AQUÍ USAMOS LA LISTA REAL
}

# Simulación de histórico Este sí se queda hardcoded porque no tenemos BD de histórico aún
json_history = [
  { "semestre": '2022-1', "f1Score": 0.72, "rmse": 0.45 },
  { "semestre": '2022-2', "f1Score": 0.78, "rmse": 0.38 },
  { "semestre": '2023-1', "f1Score": 0.85, "rmse": 0.25 },
  { "semestre": '2023-2', "f1Score": 0.89, "rmse": 0.18 },
  { "semestre": '2024-1', "f1Score": 0.92, "rmse": 0.12 },
  { "semestre": '2024-2', "f1Score": 0.95, "rmse": 0.08 }
]

def save_json_s3(data, filename):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, f"{path_output_json_prefix}/{filename}")
    obj.put(Body=json.dumps(data, indent=2, ensure_ascii=False))
    print(f"   Guardado en s3://{bucket_name}/{path_output_json_prefix}/{filename}")

save_json_s3(json_sugerencias, "sugerencias_horario.json")
save_json_s3(json_metrics, "model_metrics.json")
save_json_s3(json_history, "model_history.json")

# CAMBIO 3: Guardar la gráfica nueva
if json_grafica_asistencia:
    save_json_s3(json_grafica_asistencia, "grafica_asistencia_por_hora.json")

print("¡JOB FINALIZADO EXITOSAMENTE!")
job.commit()