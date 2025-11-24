import json
import boto3
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:        
        tables = {
            "teacherQuality": dynamodb.Table('teacher_quality').scan().get('Items', []),
            "bottlenecks": dynamodb.Table('bottleneck').scan().get('Items', []),
            "tuitionEvolution": dynamodb.Table('tuition_evolution').scan().get('Items', []),
            "timeBias": dynamodb.Table('time_bias').scan().get('Items', [])
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps(tables, cls=DecimalEncoder)
        }
        
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }