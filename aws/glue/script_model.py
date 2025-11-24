import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_input_features_path = "s3://analitica-datalake/processed/model_ready/"
s3_output_model_path   = "s3://analitica-datalake/models/v1/"

print(f"Leyendo dataset de features desde: {s3_input_features_path}")
print(f"El modelo entrenado se guardará en: {s3_output_model_path}")


# ==========================================================
# 1. LECTURA DEL DATASET LISTO PARA MODELADO
# ==========================================================

df = spark.read.parquet(s3_input_features_path)

print("\n=== Esquema del dataset de entrenamiento (model_ready) ===")
df.printSchema()


# ==========================================================
# 2. SELECCIÓN DE COLUMNAS Y LIMPIEZA INICIAL
# ==========================================================

label_col = "Y_TOO_Historica"

# Estas son las columnas numéricas que YA existen en el dataset
raw_numeric_cols = [
    "X_Cp_Profesor",
    "X_Factor_Demanda_Normalizado",
    "X_capacidad_salon"
]

# ==========================================================
# 3. CASTEO SEGURO A DOUBLE DE VARIABLES EXISTENTES
# ==========================================================

print("\nCasteando columnas numéricas base a DoubleType...")

cols_to_cast = [label_col] + raw_numeric_cols

df_casted = df
for c in cols_to_cast:
    if c in df_casted.columns:
        df_casted = df_casted.withColumn(c, F.col(c).cast(DoubleType()))
    else:
        raise Exception(f"❌ La columna requerida '{c}' no existe en el dataframe.")

print("✔ Casteo base completado.")


# ==========================================================
# 3.1 FEATURE ENGINEERING (ENRIQUECIMIENTO DE DATOS)
# ==========================================================
print("\n🔧 Generando nuevas features (Hora, Semestre, Materia)...")

# 1. EXTRAER HORA DE INICIO
df_enriched = df_casted.withColumn(
    "X_Hora_Inicio", 
    F.col("horario_visual")[0]["inicio"]
).na.fill(7, subset=["X_Hora_Inicio"])


# 2. CONVERTIR SEMESTRE A NÚMERO
df_enriched = df_enriched.withColumn(
    "X_Semestre_Num", 
    F.col("semestre_visual").cast("double")
).na.fill(1.0, subset=["X_Semestre_Num"])


# 3. INDEXAR NOMBRE DE MATERIA (StringIndexer)
indexer = StringIndexer(
    inputCol="materia_nombre_visual", 
    outputCol="X_Materia_Index", 
    handleInvalid="keep"
)

indexer_model = indexer.fit(df_enriched)
df_final_train = indexer_model.transform(df_enriched)

print("✔ Nuevas features generadas: X_Hora_Inicio, X_Semestre_Num, X_Materia_Index")


# ==========================================================
# 4. ARMADO DEL VECTOR DE FEATURES (DEFINICIÓN FINAL)
# ==========================================================

feature_cols = [
    "X_Cp_Profesor",
    "X_Factor_Demanda_Normalizado",
    "X_capacidad_salon",
    "X_Hora_Inicio",    
    "X_Semestre_Num",   
    "X_Materia_Index"
]

feature_cols_ordered = feature_cols[:] 

print("\nConstruyendo vector de features...")

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
)

df_ml = assembler.transform(df_final_train)

# Dataset final para MLlib: features + label
df_ml = df_ml.select("features", F.col(label_col).alias("label"))

print("Vector de features creado.")
df_ml.show(5, truncate=False)


# ==========================================================
# 5. TRAIN / TEST SPLIT
# ==========================================================

print("\nDividiendo en train / test (80 / 20)...")

train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)

print(f"   → Registros en train: {train_df.count()}")
print(f"   → Registros en test : {test_df.count()}")


# ==========================================================
# 6. ENTRENAMIENTO CON GRID SEARCH (OPTIMIZACIÓN)
# ==========================================================

print("\n🚀 Iniciando Búsqueda de Hiperparámetros (Grid Search)...")

# Definir modelo base
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="label",
    seed=42,
    maxBins=128 # Importante mantener esto
)

# Rejilla (reducida para que corra rápido esta vez si quieres)
paramGrid = (ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5])
    .addGrid(gbt.maxIter, [20, 60])
    .addGrid(gbt.stepSize, [0.1])
    .build())

evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")

cv = CrossValidator(
    estimator=gbt,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

print("⏳ Entrenando...")
cv_model = cv.fit(train_df)
print("✔ Búsqueda finalizada.")

# ==========================================================
# 7. EXTRAER PARÁMETROS Y GENERAR MODELO LIMPIO (¡EL FIX!)
# ==========================================================
# Aquí está la magia. En lugar de guardar cv_model.bestModel, 
# leemos sus "secretos" y creamos uno nuevo.

dirty_best_model = cv_model.bestModel

# Extraer los valores ganadores
best_depth = dirty_best_model.getMaxDepth()
best_iter = dirty_best_model.getMaxIter()
best_step = dirty_best_model.getStepSize()

print("\n🏆 GANADORES:")
print(f"   Depth: {best_depth}, Iter: {best_iter}, Step: {best_step}")

print("\n🧹 Generando MODELO LIMPIO (Sanitized)...")
# Creamos una instancia nueva, fresca, sin historial de CrossValidation
clean_gbt = GBTRegressor(
    featuresCol="features",
    labelCol="label",
    seed=42,
    maxBins=128,
    maxDepth=best_depth,
    maxIter=best_iter,
    stepSize=best_step
)

# Entrenamos este modelo limpio con TODOS los datos (Train + Test)
# Ya sabemos que es bueno, así que usamos toda la data para que sea más robusto
df_full_data = df_ml 
final_clean_model = clean_gbt.fit(df_full_data)

print("✔ Modelo limpio generado exitosamente.")

# ==========================================================
# 9. GUARDAR EL MODELO LIMPIO
# ==========================================================

print(f"\n💾 Guardando modelo saneado en: {s3_output_model_path}")
# Ahora sí, este guardado generará un JSON estándar que NO fallará al leer
final_clean_model.write().overwrite().save(s3_output_model_path)

print("✔ PROCESO TERMINADO. Ahora puedes correr el Job de Predicción.")

job.commit()