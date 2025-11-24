import json
import boto3
from decimal import Decimal

BUCKET_NAME = 'analitica-datalake'
BASE_PATH = 'processed/predictions_output/'

FILES_MAP = {
    'sugerencias': 'sugerencias_horario.json',
    'metricas':    'model_metrics.json',
    'evolucion':   'model_history.json',
    'asistencia':  'grafica_asistencia_por_hora.json'
}

TABLE_ANALITICA = 'model_analytics'
TABLE_SUGERENCIAS = 'optimal_suggestions' 

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def parse_float(obj):
    """Ayuda a DynamoDB con los decimales"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: parse_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [parse_float(v) for v in obj]
    return obj


def cargar_sugerencias(data):
    table = dynamodb.Table(TABLE_SUGERENCIAS)
    scan = table.scan(ProjectionExpression="semestre_id, grupo_id")
    items_a_borrar = scan.get('Items', [])
    
    if items_a_borrar:
        with table.batch_writer() as batch:
            for each in items_a_borrar:
                batch.delete_item(Key={
                    'semestre_id': each['semestre_id'],
                    'grupo_id': each['grupo_id']
                })
        print(f"Se eliminaron {len(items_a_borrar)} registros antiguos.")
    else:
        print("La tabla ya estaba vacía.")

    
    with table.batch_writer() as batch:
        for item in data:
            item_dynamo = parse_float(item)
            item_dynamo['semestre_id'] = str(item_dynamo.get('semestre', '2025-1'))
            
            if 'grupo' in item_dynamo:
                item_dynamo['grupo_id'] = str(item_dynamo['grupo']) # Asegura string
            else:
                item_dynamo['grupo_id'] = str(item_dynamo.get('id', 'UNKNOWN'))
            
            if 'materia' in item_dynamo:
                item_dynamo['materia_nombre'] = item_dynamo['materia']

            batch.put_item(Item=item_dynamo)
            
    print(f"Proceso finalizado: {len(data)} sugerencias activas.")

def cargar_metricas(data):
    table = dynamodb.Table(TABLE_ANALITICA)
    item = {
        'tipo_registro': 'DASHBOARD_ACTUAL', 
        'identificador': 'LATEST',           
        'kpis': parse_float(data.get('kpis', {})),
        'topFeatures': parse_float(data.get('topFeatures', []))
    }
    table.put_item(Item=item)
    print("--> Métricas (KPIs/Features) actualizadas.")

def cargar_evolucion(data):
    table = dynamodb.Table(TABLE_ANALITICA)
    with table.batch_writer() as batch:
        for row in data:
            item = parse_float(row)
            item['tipo_registro'] = 'EVOLUCION_MODELO' 
            item['identificador'] = str(row['semestre']) 
            batch.put_item(Item=item)
    print(f"--> Historial de evolución cargado: {len(data)} registros.")

def cargar_grafica_asistencia(data):
    table = dynamodb.Table(TABLE_ANALITICA)
    item = {
        'tipo_registro': 'GRAFICA_ASISTENCIA', 
        'identificador': 'LATEST',             
        'datos': parse_float(data)             
    }
    table.put_item(Item=item)
    print("--> Gráfica de asistencia cargada.")


def lambda_handler(event, context):
    tipo_carga = event.get('tipo', 'sugerencias') 
    
    file_name = FILES_MAP.get(tipo_carga)
    if not file_name:
        return {'statusCode': 400, 'body': json.dumps(f"Tipo no válido: {tipo_carga}")}
    
    full_key = f"{BASE_PATH}{file_name}"
    print(f"Iniciando carga. Tipo: {tipo_carga} | Archivo: s3://{BUCKET_NAME}/{full_key}")

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=full_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        if tipo_carga == 'sugerencias':
            cargar_sugerencias(data)
        elif tipo_carga == 'metricas':
            cargar_metricas(data)
        elif tipo_carga == 'evolucion':
            cargar_evolucion(data)
        elif tipo_carga == 'asistencia':
            cargar_grafica_asistencia(data)
        
        return {
            'statusCode': 200, 
            'body': json.dumps(f"Carga de '{tipo_carga}' completada exitosamente.")
        }

    except s3_client.exceptions.NoSuchKey:
        error_msg = f"El archivo {full_key} no existe en el bucket."
        print(error_msg)
        return {'statusCode': 404, 'body': json.dumps(error_msg)}
    except Exception as e:
        print(f"Error crítico: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f"Error: {str(e)}")}