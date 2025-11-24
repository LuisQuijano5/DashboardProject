import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql.functions import mean, col, when, countDistinct, count, desc, sum, lit
import json
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==========================================================
# CELL 2 — Definición de rutas y carga de datos desde S3
# ==========================================================

#Lectura forzada de los caracteres raros en los JSON de los CSV
csv_opts = {
    "header": True,
    "encoding": "UTF-8",
    "multiLine": True,
    "escape": "\"",
    "quote": "\"",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record"
}

# Ruta base del datalake (folder historical)
path_base = "s3://analitica-datalake/raw/historical/"

print("Leyendo datasets desde:", path_base)

df_salones = spark.read.options(**csv_opts).csv(path_base + "catalogo_salones.csv")
df_grupos = spark.read.options(**csv_opts).csv(path_base + "grupos_historicos.csv")
df_inscripciones = spark.read.options(**csv_opts).csv(path_base + "inscripciones.csv")
df_profesores = spark.read.options(**csv_opts).csv(path_base + "catalogo_profesores.csv")

print("CSV cargados: salones, grupos, inscripciones, profesores")

df_m_ind = spark.read.json(path_base + "materias_IND.json")
df_m_sis = spark.read.json(path_base + "materias_ISC.json")

print("JSON cargados: materias_IND, materias_ISC")

df_alumnos = (
    spark.read.options(**csv_opts)
        .csv(path_base + "snapshots/*.csv")
        .withColumn("file_name", input_file_name())
        .withColumn(
            "semestre_historico",
            regexp_extract(F.col("file_name"), r'(\d{4}-\d)', 1)
        )
        .drop("file_name")
)


print("Snapshots de alumnos cargados desde: snapshots/")
print("Total snapshots cargados:", df_alumnos.count())

# ==========================================================
# CELL 4 — Normalización avanzada de materias
# ==========================================================

print("Normalizando materias (nombres, mayúsculas, prerequisitos)...")

# ----------------------------------------------------------
# 1. Limpiar columnas básicas
# ----------------------------------------------------------

def clean_str_cols(df):
    for c in df.columns:
        df = df.withColumn(
            c,
            F.trim(F.regexp_replace(F.col(c).cast("string"), "\s+", " "))
        )
    return df

df_m_ind = clean_str_cols(df_m_ind)
df_m_sis = clean_str_cols(df_m_sis)

# ----------------------------------------------------------
# 2. Estándar de esquema
# ----------------------------------------------------------

def normalize_schema(df):
    cols = df.columns

    # NOMBRE
    if "nombre" in cols:
        df = df.withColumnRenamed("nombre", "materia_nombre")

    # SEMESTRE
    if "semestre" in cols:
        df = df.withColumnRenamed("semestre", "semestre_materia")

    # PRERREQUISITOS
    if "prerrequisito" in cols:
        df = df.withColumn("prerequisitos_raw", F.col("prerrequisito"))
    elif "prerequisitos" in cols:
        df = df.withColumn("prerequisitos_raw", F.col("prerequisitos"))
    elif "prerrequisitos" in cols:
        df = df.withColumn("prerequisitos_raw", F.col("prerrequisitos"))
    else:
        df = df.withColumn("prerequisitos_raw", F.lit(None))

    return df.select("materia_nombre", "semestre_materia", "prerequisitos_raw")

df_m_ind = normalize_schema(df_m_ind)
df_m_sis = normalize_schema(df_m_sis)


# ----------------------------------------------------------
# 3. Normalizar nombre y prerequisitos a MAYÚSCULAS
# ----------------------------------------------------------

def normalize_materia_names(df):
    return df.withColumn(
        "materia_nombre",
        F.upper(F.col("materia_nombre"))
    )

df_m_ind = normalize_materia_names(df_m_ind)
df_m_sis = normalize_materia_names(df_m_sis)

# ----------------------------------------------------------
# 4. Transformar prerrequisitos → array de nombres en mayúsculas
# ----------------------------------------------------------

def normalize_prereqs(df):
    
    # Define la lógica de limpieza de la cadena, incluyendo la eliminación de corchetes y comillas.
    prereqs_cleaned = F.upper(
        F.regexp_replace(
            F.col("prerequisitos_raw"),
            '[\\[\\]"]', ''
        )
    )
    
    # 1. Aplica la limpieza y divide la cadena por la coma.
    prereqs_split = F.split(prereqs_cleaned, ",")
    
    # 2. Usa F.transform para iterar sobre la lista resultante y aplicar F.trim() a cada elemento.
    #    Esto elimina los espacios en blanco alrededor de cada nombre de prerrequisito.
    prereqs_final = F.transform(
        prereqs_split,
        lambda x: F.trim(x) 
    )

    return (
        df
        .withColumn(
            "prerequisitos",
            F.when(
                F.col("prerequisitos_raw").isNotNull(),
                prereqs_final
            ).otherwise(F.array())
        )
        .drop("prerequisitos_raw")
    )

df_m_ind = normalize_prereqs(df_m_ind)
df_m_sis = normalize_prereqs(df_m_sis)

#Añadir columna carrera
df_m_ind = df_m_ind.withColumn("carrera", F.lit("IND"))
df_m_sis = df_m_sis.withColumn("carrera", F.lit("ISC"))

# ----------------------------------------------------------
# 5. Unir catálogos
# ----------------------------------------------------------

df_materias_raw = (
    df_m_ind.unionByName(df_m_sis, allowMissingColumns=True)
)

# ----------------------------------------------------------
# 6. Eliminar duplicados solo cuando:
#       mismo nombre + mismos prerrequisitos
# ----------------------------------------------------------

df_materias = (
    df_materias_raw
    .dropDuplicates(["materia_nombre", "prerequisitos", "carrera"])
    .orderBy("semestre_materia", "materia_nombre")
)

print("Normalización completada.")
print("Total materias:", df_materias.count())

df_materias.show(50, truncate=False)

# ----------------------------------------------------------
# 7. Exportar materias normalizadas a S3 en formato PARQUET
# ----------------------------------------------------------

s3_output_materias = "s3://analitica-datalake/processed/materias_normalizadas/"

(
    df_materias
    .write
    .mode("overwrite")   # sobrescribe porque es catálogo
    .parquet(s3_output_materias)
)

print("\n✔ Catálogo de materias normalizadas exportado correctamente.")
print("Ruta S3:", s3_output_materias)

# ==========================================================
# CELL 5 — Normalización de alumnos (materias_aprobadas_json)
# ==========================================================

print("Normalizando alumnos (materias_aprobadas_json → array limpio)...")

# ----------------------------------------------------------
# 1. UDF para convertir el JSON string en lista
# ----------------------------------------------------------

def parse_materias(x):
    if x is None:
        return []
    try:
        data = json.loads(x)
        if isinstance(data, list):
            return data
        return []
    except:
        # Si está mal formado, intentar separarlo por comas
        try:
            return [y.strip() for y in x.split(",") if y.strip()]
        except:
            return []

udf_parse_materias = F.udf(parse_materias, ArrayType(StringType()))

# ----------------------------------------------------------
# 2. Parsear + limpiar espacios + mayúsculas
# ----------------------------------------------------------

df_alumnos_clean = (
    df_alumnos
    .withColumn("materias_aprobadas_raw", udf_parse_materias(F.col("materias_aprobadas_json")))
    .withColumn(
        "materias_aprobadas",
        F.expr("""
            filter(
                transform(
                    materias_aprobadas_raw,
                    x -> regexp_replace(trim(upper(x)), '\\s+', ' ')
                ),
                x -> x != ''
            )
        """)
    )
    .drop("materias_aprobadas_json", "materias_aprobadas_raw")
)

print("Normalización de alumnos completada.")
df_alumnos_clean.show(10, truncate=False)

# ==========================================================
# CELL 6 — Normalización avanzada de horario_json
# ==========================================================

print("Normalizando horarios de grupos (horario_json → estructura limpia)...")

# -----------------------------------------------
# 1. UDF para transformar el JSON string en lista normalizada
# -----------------------------------------------

def parse_horario(json_str):
    if json_str is None:
        return []

    try:
        # Decodificar caracteres \u00e9 etc.
        data = json.loads(json_str)

        output = []

        for dia, rango in data.items():
            dia_norm = (
                dia.replace("\\u00e9", "é")
                   .replace("\\u00f3", "ó")
                   .replace("\\u00ed", "í")
                   .replace("\\u00fa", "ú")
                   .replace("\\u00e1", "á")
                   .upper()
            )

            if "-" in rango:
                try:
                    ini, fin = rango.split("-")
                    ini = int(ini.strip())
                    fin = int(fin.strip())
                except:
                    ini = None
                    fin = None
            else:
                ini = None
                fin = None

            output.append({
                "dia": dia_norm,
                "inicio": ini,
                "fin": fin
            })

        return output

    except Exception as e:
        return []

schema_horario = ArrayType(
    StructType([
        StructField("dia", StringType(), True),
        StructField("inicio", IntegerType(), True),
        StructField("fin", IntegerType(), True)
    ])
)

udf_parse_horario = F.udf(parse_horario, schema_horario)

# -----------------------------------------------
# 2. Aplicar normalización al DF de grupos
# -----------------------------------------------

df_grupos_clean = df_grupos.withColumn(
    "horario_limpio",
    udf_parse_horario(F.col("horario_json"))
)

print("Horario normalizado correctamente.")

df_grupos_clean.select("grupo_id","horario_json","horario_limpio").show(10, truncate=False)

# ==========================================================
# CELL 7 — Cálculo de features por grupo + TOO
# ==========================================================

print("Calculando features por grupo")

# ----------------------------------------------------------
# 1. Preparar DF de inscripciones (solo activos / no desertores)
# ----------------------------------------------------------

df_insc_clean = (
    df_inscripciones
    .filter((F.col("estado_desercion") == "false") | (F.col("estado_desercion") == False))
)

# Asegurar que calificacion_final sea numérica
df_insc_clean = df_insc_clean.withColumn(
    "calificacion_final", 
    F.col("calificacion_final").cast("double")
)

# ----------------------------------------------------------
# 2. Conteos por grupo
# ----------------------------------------------------------

# Alumnos inscritos por grupo
df_inscritos = (
    df_insc_clean
    .groupBy("grupo_id")
    .agg(F.count("*").alias("num_inscritos"))
)

# Alumnos aprobados (>=70)
df_aprobados = (
    df_insc_clean
    .filter(F.col("calificacion_final") >= 70)
    .groupBy("grupo_id")
    .agg(F.count("*").alias("num_aprobados"))
)

# ----------------------------------------------------------
# 3. Unir con grupos y calcular métricas
# ----------------------------------------------------------

df_g = (
    df_grupos_clean
    .join(df_inscritos, "grupo_id", "left")
    .join(df_aprobados, "grupo_id", "left")
    .withColumn("num_inscritos", F.coalesce(F.col("num_inscritos"), F.lit(0)))
    .withColumn("num_aprobados", F.coalesce(F.col("num_aprobados"), F.lit(0)))
)

# ----------------------------------------------------------
# 4. Utilización de Cupos UC_g
# ----------------------------------------------------------

df_g = df_g.withColumn(
    "UC_g",
    F.when(F.col("cupo_ofrecido") > 0,
           F.col("num_inscritos") / F.col("cupo_ofrecido"))
     .otherwise(F.lit(0))
)

# ----------------------------------------------------------
# 5. Tasa de Aprobación TA_g
# ----------------------------------------------------------

df_g = df_g.withColumn(
    "TA_g",
    F.when(F.col("num_inscritos") > 0,
           F.col("num_aprobados") / F.col("num_inscritos"))
     .otherwise(F.lit(0))
)

# ----------------------------------------------------------
# 6. Tasa de Conflictos de Cruces TC_g
# ----------------------------------------------------------

df_g = df_g.withColumn(
    "TC_g",
    F.when((F.col("conflicto_recurso_detectado") == "true") | 
           (F.col("conflicto_recurso_detectado") == True), 
           F.lit(1))
     .otherwise(F.lit(0))
)

# ----------------------------------------------------------
# 8. Resultado final
# ----------------------------------------------------------

df_features_grupos = df_g.select(
    "grupo_id",
    "materia_id",
    "materia_nombre",
    "profesor_id",
    "semestre_historico",
    "materia_semestre",
    "salon_id",
    "horario_limpio",
    "cupo_ofrecido",
    "num_inscritos",
    "num_aprobados",
    "UC_g",
    "TA_g",
    "TC_g"
)

print("Features por grupo calculadas correctamente.")
df_features_grupos.show(20, truncate=False)

#CELL 8
print("Calculando Demanda Potencial Histórica por materia y semestre...")

# ----------------------------------------------------------
# 1. Crear dict broadcast de prerrequisitos
# ----------------------------------------------------------
def extract_prereq_dict(df):
    """Extrae el nombre de la materia y sus prerrequisitos en un diccionario para broadcast."""
    # Asegurarse de manejar el caso donde prerequisitos es None
    rows = df.select("materia_nombre", "prerequisitos").collect()
    return {r["materia_nombre"]: (r["prerequisitos"] or []) for r in rows}

prereq_dict = extract_prereq_dict(df_materias)
bc_prereq = spark.sparkContext.broadcast(prereq_dict)

print("Prerrequisitos cargados en broadcast")

# ----------------------------------------------------------
# 2. UDF de elegibilidad REAL por semestre histórico
# ----------------------------------------------------------
def is_eligible(aprobadas, materia, semestre_actual, semestre_materia):
    """
    Determina si un alumno es elegible para una materia.
    
    Lógica de Elegibilidad:
    1. El semestre actual del alumno debe ser MAYOR O IGUAL al semestre de la materia.
    2. El alumno debe haber aprobado TODOS los prerrequisitos de la materia.
    """
    if aprobadas is None:
        aprobadas = []

    if semestre_actual < semestre_materia:
        return False

    # 2. Verificación de prerrequisitos
    prereqs = bc_prereq.value.get(materia, [])
    if not prereqs or (len(prereqs) == 1 and prereqs[0] == 'NULL'):
        return True # No tiene prerrequisitos

    aprob_set = set(aprobadas)
    # Debe tener APROBADO cada prerrequisito
    return all(p in aprob_set for p in prereqs if p and p != '')

udf_is_eligible = F.udf(is_eligible, "boolean")

# ----------------------------------------------------------
# 3. Preparar alumnos con upper case y renombrar semestre_historico
# ----------------------------------------------------------
df_alumnos_ready = (
    df_alumnos_clean
    .withColumn(
        "materias_aprobadas_upper",
        F.transform(
            F.col("materias_aprobadas"),
            lambda x: F.upper(F.trim(x))
        )
    )
    .withColumnRenamed("semestre_historico", "periodo_historico")
)

# ----------------------------------------------------------
# 4. Expandir alumno × materia × semestre histórico (Cross Join)
# ----------------------------------------------------------
df_materia_sem = (
    df_materias
        .select("materia_nombre", "semestre_materia", "carrera")
        .withColumnRenamed("carrera", "carrera_materia")
)

df_cross = (
    df_alumnos_ready
        .withColumnRenamed("carrera", "carrera_alumno")
        .crossJoin(df_materia_sem)
        .filter(F.col("carrera_alumno") == F.col("carrera_materia")) 
)

# ----------------------------------------------------------
# 5. Filtrar alumnos cuyo SEMESTRE ACTUAL es MAYOR O IGUAL al de la materia
# ----------------------------------------------------------
# semestre actual del alumno >= semestre teórico de la materia
df_cross = df_cross.filter(
    (F.col("semestre_actual") >= F.col("semestre_materia")) &
    (F.col("estado") == "Activo")
)

# ----------------------------------------------------------
# 6. Calcular elegibilidad (Verificación de Prerrequisitos)
# ----------------------------------------------------------
# Aquí se usa la UDF que verifica los prerrequisitos (el segundo requisito de elegibilidad)
df_cross = df_cross.withColumn(
    "es_elegible",
    udf_is_eligible(
        F.col("materias_aprobadas_upper"),
        F.col("materia_nombre"),
        F.col("semestre_actual"),
        F.col("semestre_materia")
    )
)

# ----------------------------------------------------------
# 7. AGRUPACIÓN HISTÓRICA (Demanda Potencial)
# ----------------------------------------------------------
df_demanda = (
    df_cross.groupBy("materia_nombre", "semestre_materia", "periodo_historico")
            # Suma de todos los 'es_elegible' = True para cada materia/periodo
            .agg(F.sum(F.col("es_elegible").cast("int")).alias("demanda_potencial"))
)

df_demanda_potencial = (
    df_demanda
    .select(
        "materia_nombre",
        "semestre_materia",
        "periodo_historico",
        "demanda_potencial"
    )
    .orderBy("periodo_historico", "semestre_materia", "materia_nombre")
)

print("Demanda potencial histórica calculada.")
print("=== DEMANDA HISTÓRICA LIMPIA (TOP 10) ===")
df_demanda_potencial.show(100, truncate=False)

# ==========================================================
# CELL 9-10 — Calidad/Éxito del Profesor (Cp)
# ==========================================================

print("Calculando Calidad/Éxito del Profesor (Cp)...")

# ----------------------------------------------------------
# 1. Unir inscripciones con grupos para obtener el profesor_id y calif.
# ----------------------------------------------------------

# df_insc_clean: inscripciones filtradas por no desertores y calif. final casteada a double
df_grupos_profesor = df_grupos_clean.select("grupo_id", "profesor_id")

df_calificaciones = (
    df_insc_clean
    .join(df_grupos_profesor, "grupo_id", "inner")
    .select("profesor_id", "grupo_id", "alumno_id", "calificacion_final")
)

# ----------------------------------------------------------
# 2. Sumatoria de Calificaciones y Conteo por Grupo (Σ calif_Final / No. alumnos en grupo)
# ----------------------------------------------------------

# a) Suma de calificaciones por grupo (numerador interno)
df_sum_calif_grupo = (
    df_calificaciones
    .groupBy("grupo_id", "profesor_id")
    .agg(
        F.sum("calificacion_final").alias("suma_calif_grupo"),
        F.count("alumno_id").alias("num_alumnos_grupo")
    )
)

# Cálculo de la Calidad/Éxito del Grupo (Cp_g): Promedio simple por grupo
# Cp_g = (Suma calif. grupo) / (No. alumnos grupo)
df_sum_calif_grupo = df_sum_calif_grupo.withColumn(
    "Cp_g",
    F.when(
        F.col("num_alumnos_grupo") > 0,
        F.col("suma_calif_grupo") / F.col("num_alumnos_grupo")
    ).otherwise(F.lit(0.0))
)


# ----------------------------------------------------------
# 3. Sumatoria total y Conteo total por Profesor (Fórmula General)
# ----------------------------------------------------------

# a) Sumatoria total por profesor (Σ calif_Final total, numerador externo)
df_sum_calif_profesor = (
    df_sum_calif_grupo
    .groupBy("profesor_id")
    .agg(
        F.sum("suma_calif_grupo").alias("suma_calif_profesor_total"), # Σ G∈P (Σ calif_Final)
        F.sum("num_alumnos_grupo").alias("num_alumnos_profesor_total") # No. alumnos en sus grupos
    )
)

# b) Cálculo de la Calidad/Éxito del Profesor (Cp)
df_calidad_profesor = df_sum_calif_profesor.withColumn(
    "Calidad_Profesor",
    F.when(
        F.col("num_alumnos_profesor_total") > 0,
        F.col("suma_calif_profesor_total") / F.col("num_alumnos_profesor_total")
    ).otherwise(F.lit(0.0))
)

# ----------------------------------------------------------
# 4. Resultado final: Unir la Calidad de vuelta a los grupos
# ----------------------------------------------------------

df_calidad_profesor_por_grupo = (
    df_grupos_clean
    .select("grupo_id", "profesor_id", "semestre_historico", "materia_nombre")
    .join(
        df_calidad_profesor.select("profesor_id", "Calidad_Profesor"),
        "profesor_id",
        "inner"
    )
    # Unir también el promedio por grupo (Cp_g) para un análisis más completo
    .join(
        df_sum_calif_grupo.select("grupo_id", "Cp_g", "num_alumnos_grupo"),
        "grupo_id",
        "left"
    )
    .withColumnRenamed("Cp_g", "Calidad_Grupo_Promedio")
    .withColumnRenamed("num_alumnos_grupo", "num_alumnos_curso")
    .select(
        "grupo_id",
        "semestre_historico",
        "materia_nombre",
        "profesor_id",
        F.round("Calidad_Profesor", 2).alias("Cp_Profesor"),
        F.round("Calidad_Grupo_Promedio", 2).alias("Cp_Grupo"),
        "num_alumnos_curso"
    )
    .orderBy("semestre_historico", "materia_nombre")
)


print("Calidad/Éxito del Profesor y del Grupo calculada correctamente.")
df_calidad_profesor_por_grupo.show(10, truncate=False)

# ==========================================================
# CELL 11 — Cálculo de features por grupo + TOO (MODIFICADO)
# ==========================================================

print("Calculando features por grupo (UC, TA, TC, TOO)...")

# ... (Pasos 1 a 6: Cálculo de UC_g, TA_g, TC_g, df_g) ...

# ----------------------------------------------------------
# 7. Integración de nuevos factores (Cp, Demanda, Asistencia)
# ----------------------------------------------------------

# a) Unir Demanda Potencial (DP)
# Renombrar semestre_historico en df_g para unirse con df_demanda_potencial
df_g_temp = df_g.withColumnRenamed("semestre_historico", "periodo_historico")

df_g_temp = (
    df_g_temp
    .join(
        df_demanda_potencial.select("materia_nombre", "periodo_historico", "demanda_potencial"),
        on=["materia_nombre", "periodo_historico"],
        how="left"
    )
    .withColumn("demanda_potencial", F.coalesce(F.col("demanda_potencial"), F.lit(0)))
)

# b) Unir Calidad Profesor (Cp_p)
# Usaremos el Cp_Profesor calculado en CELL 10
df_g_temp = (
    df_g_temp
    .join(
        # Usamos df_calidad_profesor, que tiene el promedio general del profesor (Calidad_Profesor)
        df_calidad_profesor.select("profesor_id", "Calidad_Profesor"), 
        "profesor_id",
        "left"
    )
    .withColumn("Calidad_Profesor", F.coalesce(F.col("Calidad_Profesor"), F.lit(0.0)))
)


# c) Asistencia (AS_g) - Imputación en 0.0 (futuro dato real)
df_g_temp = df_g_temp.withColumn("AS_g", F.lit(0.0)) # Imputado en 0 por ahora

# ----------------------------------------------------------
# 8. Normalización de Demanda Potencial (DPF_g)
# ----------------------------------------------------------

# La demanda es un conteo, debe normalizarse para sumarse al TOO
# DPF_g = (Demanda Potencial) / (Máxima Demanda Potencial en el histórico)
max_demanda = df_g_temp.agg(F.max("demanda_potencial")).collect()[0][0]
max_demanda = max_demanda if max_demanda and max_demanda > 0 else 1

df_g_temp = df_g_temp.withColumn(
    "DPF_g", # Factor de Demanda Potencial Normalizado
    F.when(F.col("demanda_potencial") > 0, F.col("demanda_potencial") / F.lit(max_demanda))
    .otherwise(F.lit(0.0))
)

# ----------------------------------------------------------
# 9. Tasa de Optimización Operacional (TOO_g) - NUEVA FÓRMULA
# ----------------------------------------------------------
# Usaremos pesos (W) iguales a 1.0 y normalizaremos la Calidad Profesor (Cp)
# Factores que SUMAN: UC_g, TA_g, DPF_g, Cp_Profesor/100, AS_g
# Factores que RESTAN: TC_g

num_factores = 5.0 # UC + TA + DPF + Cp + AS

df_g_final = df_g_temp.withColumn(
    "TOO_g",
    (
        F.col("UC_g") +
        F.col("TA_g") +
        F.col("DPF_g") +
        (F.col("Calidad_Profesor") / F.lit(100.0)) +
        F.col("AS_g") -
        F.col("TC_g")
    ) / F.lit(num_factores)
)

# ----------------------------------------------------------
# RECUPERACIÓN DE METADATA VISUAL (SOLUCIÓN AL PROBLEMA DE PROPAGACIÓN)
# ----------------------------------------------------------

# **RENOMBRAR Metadata visual para evitar ambigüedad en el JOIN**
df_metadata_visual_renamed = df_grupos_clean.select(
    "grupo_id",
    F.col("materia_semestre").alias("materia_semestre_m"),
    F.col("salon_id").alias("salon_id_m"),
    F.col("horario_limpio").alias("horario_limpio_m")
)

df_features_consolidado = (
    df_g_final
    .join(df_metadata_visual_renamed, "grupo_id", "inner")
)

# ----------------------------------------------------------
# 10. Resultado final (df_features_grupos - Ahora Completo y NO AMBIGUO)
# ----------------------------------------------------------

df_features_grupos = df_features_consolidado.select(
    # Claves y Metadata Original (Tomadas de df_g_final)
    "grupo_id",
    "materia_id",
    "materia_nombre",
    "profesor_id",
    F.col("periodo_historico").alias("semestre_historico"),

    #b Columnas de Metadata
    F.col("materia_semestre_m").alias("materia_semestre"),
    F.col("salon_id_m").alias("salon_id"),
    F.col("horario_limpio_m").alias("horario_limpio"),
    
    # Features Operacionales
    "cupo_ofrecido",
    "num_inscritos",
    "num_aprobados",
    "UC_g",
    "TA_g",
    "TC_g",
    
    # Features y Target Avanzados
    "Calidad_Profesor", 
    "demanda_potencial", 
    "DPF_g", 
    "AS_g", 
    "TOO_g"
)

print("Features por grupo calculadas correctamente y CONSOLIDADAS con metadata.")
df_features_grupos.show(10, truncate=False)

# ==========================================================
# CELL FINAL (COMPLETO) — Creación de la Tabla de Features para el Modelo
# ==========================================================

print("Consolidando todas las features, metadata y nombre del profesor para el modelo...")

# ----------------------------------------------------------
# 0. Preparación de DataFrames intermedios (Simulación de la salida del CELL 8)
#    Asumimos que df_g_final ya existe y contiene TOO_g, DPF_g, Calidad_Profesor, etc.
# ----------------------------------------------------------

# --- ⚠️ Inicio de la Integración (Reemplaza tu bloque de recuperación anterior) ⚠️ ---

# 1. Preparar DF de Grupos con Metadata (usando df_grupos_clean)
df_metadata_grupos = df_grupos_clean.select(
    "grupo_id",
    "materia_id",
    "materia_nombre",
    "profesor_id",
    "carrera",
    F.col("semestre_historico").alias("periodo_historico"),
    "materia_semestre", 
    "salon_id",
    "horario_limpio",
    "cupo_ofrecido", # Necesario para la unión de features base
    "conflicto_recurso_detectado" # Necesario para TC_g si no se calculó antes
)

# 2. Unir con Features Operacionales (UC, TA, TC)
df_features_base = df_g.select(
    "grupo_id",
    "UC_g",
    "TA_g",
    "TC_g",
    "num_inscritos",
    "num_aprobados"
)

# 3. Unir todo en una sola tabla de features final (df_features_parcial)
df_features_parcial = (
    df_metadata_grupos
    .join(df_features_base, "grupo_id", "inner")
    # Unir Demanda Potencial (DPF_g)
    .join(
        df_demanda_potencial.select("materia_nombre", "periodo_historico", "demanda_potencial"),
        on=["materia_nombre", "periodo_historico"],
        how="left"
    )
    # Unir Calidad Profesor (Cp_p)
    .join(
        df_calidad_profesor.select("profesor_id", "Calidad_Profesor"), 
        "profesor_id",
        "left"
    )
    # Rellenar nulos
    .withColumn("demanda_potencial", F.coalesce(F.col("demanda_potencial"), F.lit(0)))
    .withColumn("Calidad_Profesor", F.coalesce(F.col("Calidad_Profesor"), F.lit(0.0)))
)

# 4. Cálculo final de DPF_g, AS_g y TOO_g (Replicando el CELL 11)
max_demanda = df_features_parcial.agg(F.max("demanda_potencial")).collect()[0][0]
max_demanda = max_demanda if max_demanda and max_demanda > 0 else 1

df_g_final = (
    df_features_parcial
    .withColumn("AS_g", F.lit(0.0)) # Imputación de Asistencia
    .withColumn(
        "DPF_g", # Factor de Demanda Potencial Normalizado
        F.when(F.col("demanda_potencial") > 0, F.col("demanda_potencial") / F.lit(max_demanda))
        .otherwise(F.lit(0.0))
    )
    .withColumn("TOO_g", # Cálculo de TOO final
        (
            F.col("UC_g") + F.col("TA_g") + F.col("DPF_g") + 
            (F.col("Calidad_Profesor") / F.lit(100.0)) + F.col("AS_g") - 
            F.col("TC_g")
        ) / F.lit(5.0)
    )
)

# 5. UNIÓN CRÍTICA: Añadir el nombre del profesor
df_nombres_profesores = df_profesores.select("profesor_id", "nombre_profesor")

df_features_final_con_nombres = (
    df_g_final
    .join(
        df_nombres_profesores,
        "profesor_id",
        "left"
    )
    .withColumn("nombre_profesor", F.coalesce(F.col("nombre_profesor"), F.lit("Profesor Desconocido")))
)


# ----------------------------------------------------------
# 6. CREACIÓN DE LA TABLA FINAL DE FEATURES (df_modelo_final)
# ----------------------------------------------------------

df_modelo_final = (
    df_features_final_con_nombres
    .select(
        # Y (Target/Variable Objetivo)
        F.col("TOO_g").alias("Y_TOO_Historica"),
        
        # X (Features/Variables Predictoras)
        "grupo_id",
        F.col("materia_id").alias("X_materia_id"),
        F.col("carrera").alias("carrera_visual"),
        F.col("periodo_historico").alias("semestre_historico"),
        F.col("profesor_id").alias("X_profesor_id"),
        F.col("UC_g").alias("X_Utilizacion_Cupo"),
        F.col("TC_g").alias("X_Tasa_Conflicto"),
        F.col("TA_g").alias("X_Tasa_Aprobacion"),
        F.col("Calidad_Profesor").alias("X_Cp_Profesor"),
        F.col("DPF_g").alias("X_Factor_Demanda_Normalizado"),
        F.col("AS_g").alias("X_Asistencia_Imputada"),
        F.col("cupo_ofrecido").cast("int").alias("X_capacidad_salon"),
        
        # --- METADATA VISUAL CLAVE ---
        F.col("materia_nombre").alias("materia_nombre_visual"),
        F.col("materia_semestre").alias("semestre_visual"), 
        F.col("salon_id").alias("salon_id_visual"),
        F.col("horario_limpio").alias("horario_visual"),
        F.col("demanda_potencial").alias("demanda_raw_historica"),
        F.col("nombre_profesor").alias("profesor_nombre_visual")
    )
    .orderBy("semestre_historico", "grupo_id")
)

print("ETL FINALIZADO: df_modelo_final lista para la fase de entrenamiento.")
df_modelo_final.show(10, truncate=False)

# ----------------------------------------------------------
# 7. EXPORTAR A S3 EN FORMATO PARQUET
# ----------------------------------------------------------

s3_output_path = "s3://analitica-datalake/processed/model_ready/"

# Definir la ruta completa para el dataset final
output_file_path = s3_output_path

# Guardar el DataFrame final en formato Parquet
(
    df_modelo_final
    .write
    .mode("overwrite") # Sobrescribir si ya existe (típico en jobs de entrenamiento)
    .parquet(output_file_path)
)

print(f"\nETL FINALIZADO: Features de entrenamiento guardadas en PARQUET.")
print(f"Ruta de salida: {output_file_path}")

# ==========================================================
# CELL 12 — Cálculos de Insights Históricos y Exportación a S3
# ==========================================================

print("Iniciando cálculos de Insights Históricos y guardando en S3...")

# ----------------------------------------------------------
# CONFIGURACIÓN DE RUTA DE SALIDA S3
# ----------------------------------------------------------

s3_analytics_base_path = "s3://analitica-datalake/processed/graficas/"


# ----------------------------------------------------------
# BASE DF: Unir inscripciones con grupos (Necesario para 1, 3 y 4)
# ----------------------------------------------------------

df_cuellos_base = (
    df_inscripciones
    .join(df_grupos, "grupo_id", "inner")
    .withColumn("calificacion_final", col("calificacion_final").cast("double"))
)

# ----------------------------------------------------------
# 1. Gráfica: Cuellos de Botella Históricos (Top 5 Materias por Reprobación)
# ----------------------------------------------------------

df_reprobacion = (
    df_cuellos_base
    .groupBy("materia_nombre")
    .agg(
        mean(when(col("calificacion_final") < 70, 1).otherwise(0)).alias("tasa_reprobacion")
    )
    .orderBy(desc("tasa_reprobacion"))
    .limit(5)
)

# Exportar a S3
df_reprobacion.write.mode("overwrite").json(s3_analytics_base_path + "cuellos_de_botella/")
df_reprobacion.show(20, truncate=False)
print("1. Cuellos de Botella guardados en S3.")


# ----------------------------------------------------------
# 2. Gráfica: Evolución de la Matrícula (Alumnos Activos por Carrera)
# ----------------------------------------------------------

df_matricula_activa = (
    df_alumnos
    .filter(col("estado") == "Activo")
    .groupBy("semestre_historico", "carrera")
    .agg(
        countDistinct("alumno_id").alias("num_alumnos_activos")
    )
    .orderBy("semestre_historico", "carrera")
)

# Exportar a S3
df_matricula_activa.write.mode("overwrite").json(s3_analytics_base_path + "evolucion_matricula/")
df_matricula_activa.show(20, truncate=False)
print("2. Evolución de Matrícula guardada en S3.")


# ----------------------------------------------------------
# 3. Gráfica: Tasa de Aprobación por Franja Horaria (Sesgo de Horario)
# ----------------------------------------------------------

df_sesgo_horario = (
    df_cuellos_base
    .groupBy("hora_inicio_promedio")
    .agg(
        mean(when(col("calificacion_final") >= 70, 1).otherwise(0)).alias("tasa_aprobacion_promedio"),
        count("*").alias("total_inscripciones")
    )
    .orderBy("hora_inicio_promedio")
)

# Exportar a S3
df_sesgo_horario.write.mode("overwrite").json(s3_analytics_base_path + "sesgo_horario/")
df_sesgo_horario.show(20, truncate=False)
print("3. Tasa de Aprobación por Franja Horaria guardada en S3.")


# ----------------------------------------------------------
# 4. Gráfica: Calidad de Profesores (Top/Bottom 5)
# ----------------------------------------------------------

df_prof_calif = (
    df_cuellos_base
    .groupBy("profesor_id")
    .agg(
        mean("calificacion_final").alias("calificacion_promedio"),
        countDistinct("grupo_id").alias("total_grupos_impartidos")
    )
)

df_calidad_prof = (
    df_prof_calif
    .filter(col("total_grupos_impartidos") > 10) 
    .join(df_profesores.select("profesor_id", "nombre_profesor"), "profesor_id", "inner")
)

df_top_profes = (
    df_calidad_prof
    .orderBy(desc("calificacion_promedio"))
    .limit(5)
    .withColumn("ranking", lit("Top 5"))
)

df_bottom_profes = (
    df_calidad_prof
    .orderBy(col("calificacion_promedio"))
    .limit(5)
    .withColumn("ranking", lit("Bottom 5"))
)

df_calidad_profesores_final = (
    df_top_profes.unionByName(df_bottom_profes)
    .select("nombre_profesor", "calificacion_promedio", "total_grupos_impartidos", "ranking")
    .orderBy(desc("calificacion_promedio"))
)

# Exportar a S3
df_calidad_profesores_final.write.mode("overwrite").json(s3_analytics_base_path + "calidad_profesores/")
df_calidad_profesores_final.show(20, truncate=False)
print("4. Calidad de Profesores guardada en S3.")


print("\nTodos los Insights de Negocio calculados y exportados a S3 en JSON Lines.")

job.commit()