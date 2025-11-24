import json
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = 'teacher_restrictions'
table = dynamodb.Table(TABLE_NAME)

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
            'Access-Control-Allow-Origin': '*',  #
            'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }


def get_restrictions():
    try:
        response = table.scan()
        items = response.get('Items', [])
        return build_response(200, items)
    except Exception as e:
        return build_response(500, {'error': str(e)})

def save_restriction(body):
    # {
    #   "tipo": "BLOQUEO",
    #   "id": "P104",
    #   "nombre": "Dr. Turing",
    #   "bloqueos": { "LUNES": [7, 8] }
    # }
    try:
        data = json.loads(body)
        
        item = {
            'tipo_restriccion': data.get('tipo', 'BLOQUEO'), 
            'profesor_id': data.get('id'),                               
            'nombre_visual': data.get('nombre', 'Desconocido'),
            'bloqueos': data.get('bloqueos', {}),
            'activo': True
        }
        
        table.put_item(Item=item)
        return build_response(200, {'message': 'Restricción guardada exitosamente', 'item': item})
    except Exception as e:
        return build_response(500, {'error': str(e)})

def delete_restriction(body):
    # { "tipo": "BLOQUEo", "id": "P104" }
    try:
        data = json.loads(body)
        
        key = {
            'tipo_restriccion': data.get('tipo'),
            'profesor_id': data.get('id')
        }
        
        table.delete_item(Key=key)
        return build_response(200, {'message': 'Restricción eliminada', 'key': key})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def lambda_handler(event, context):    
    http_method = event.get('httpMethod')
    
    if http_method == 'GET':
        return get_restrictions()
    
    elif http_method == 'POST':
        return save_restriction(event.get('body'))
    
    elif http_method == 'DELETE':
        return delete_restriction(event.get('body'))
    
    elif http_method == 'OPTIONS':
        return build_response(200, 'CORS OK')
        
    else:
        return build_response(405, {'error': f'Método no permitido: {http_method}'})