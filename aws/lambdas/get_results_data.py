import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

TABLE_ANALITICA = 'model_analytics'
TABLE_SUGERENCIAS = 'optimal_suggestions'

dynamodb = boto3.resource('dynamodb')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }

def get_sugerencias():
    table = dynamodb.Table(TABLE_SUGERENCIAS)
    try:
        response = table.scan()
        items = response.get('Items', [])
        return items
    except Exception as e:
        print(f"Error sugerencias: {str(e)}")
        return []

def get_analytics_data():
    table = dynamodb.Table(TABLE_ANALITICA)
    try:
        resp_current = table.get_item(
            Key={'tipo_registro': 'DASHBOARD_ACTUAL', 'identificador': 'LATEST'}
        )
        current = resp_current.get('Item', {})
        
        resp_hist = table.query(
            KeyConditionExpression=Key('tipo_registro').eq('EVOLUCION_MODELO')
        )
        history = resp_hist.get('Items', [])
        history.sort(key=lambda x: x['identificador']) # Ordenar por semestre

        return {
            'kpis': current.get('kpis', {'avgScore': 0, 'demandaSatisfecha': 0, 'eficienciaAulas': 0}),
            'topFeatures': current.get('topFeatures', []),
            'modelHistory': history
        }
    except Exception as e:
        print(f"Error analytics: {str(e)}")
        return {'kpis': {}, 'topFeatures': [], 'modelHistory': []}

def get_asistencia_chart():
    table = dynamodb.Table(TABLE_ANALITICA)
    try:
        response = table.get_item(
            Key={'tipo_registro': 'GRAFICA_ASISTENCIA', 'identificador': 'LATEST'}
        )
        item = response.get('Item', {})
        return item.get('datos', [])
    except Exception as e:
        print(f"Error asistencia: {str(e)}")
        return []

def lambda_handler(event, context):
    
    if event.get('httpMethod') == 'OPTIONS':
        return build_response(200, 'CORS OK')
    path = event.get('path', '')
    resource = event.get('resource', '') 
    
    try:
        if '/sugerencias' in path or '/sugerencias' in resource:
            data = get_sugerencias()
            return build_response(200, data)
            
        elif '/analytics' in path or '/analytics' in resource:
            data = get_analytics_data()
            return build_response(200, data)
            
        elif '/asistencia' in path or '/asistencia' in resource:
            data = get_asistencia_chart()
            return build_response(200, data)
            
        else:
            return build_response(404, {'error': f'Ruta no encontrada: {path}'})

    except Exception as e:
        print(f"Error crítico: {str(e)}")
        return build_response(500, {'error': str(e)})