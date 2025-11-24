import json
import boto3
from decimal import Decimal

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = "analitica-datalake"

FILES_CONFIG = [
    { "prefix": "processed/graficas/calidad_profesores/", "table": "teacher_quality" },
    { "prefix": "processed/graficas/cuellos_de_botella/", "table": "bottleneck" },
    { "prefix": "processed/graficas/evolucion_matricula/", "table": "tuition_evolution" },
    { "prefix": "processed/graficas/sesgo_horario/", "table": "time_bias" }
]

def lambda_handler(event, context):
    resultados = []

    for config in FILES_CONFIG:
        prefix = config["prefix"]
        table_name = config["table"]
        
        
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' not in response:
            resultados.append(f"{table_name}: Sin archivos")
            continue

        json_key = None
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                json_key = obj['Key']
                break
        
        if not json_key:
            print(f"Hay carpeta pero no archivo .json en: {prefix}")
            continue
            
        print(f"📄 Archivo detectado: {json_key}")
        
        try:
            s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=json_key)
            file_content = s3_object['Body'].read().decode('utf-8')
            lines = file_content.strip().split('\n')
            print(f"Registros encontrados: {len(lines)}")
        except Exception as e:
            print(f"Error leyendo S3: {str(e)}")
            continue

        table = dynamodb.Table(table_name)
        
        count = 0
        with table.batch_writer() as batch:
            for i, line in enumerate(lines):
                if line.strip():
                    item = json.loads(line, parse_float=Decimal)
                
                    if table_name == "teacher_quality" and 'calificacion_promedio' in item:
                        item['calificacion_promedio'] = str(item['calificacion_promedio'])

                    if table_name == "tuition_evolution":
                        if 'semestre' in item: item['semestre'] = str(item['semestre'])
                        if 'anio' in item: item['anio'] = str(item['anio'])
                        if 'matricula' in item: item['matricula'] = int(item['matricula'])

                    if table_name == "time_bias":
                        if 'hora' in item: item['hora'] = str(item['hora'])
                        if 'dia' in item: item['dia'] = str(item['dia'])

                    try:
                        batch.put_item(Item=item)
                        count += 1
                    except Exception as e:
                        print(f"error insertando item en {table_name}: {str(e)}")
        
        resultados.append(f"{table_name}: OK ({count})")

    return {
        'statusCode': 200,
        'body': json.dumps(resultados)
    }