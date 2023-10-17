import os
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event: dict, context: dict) -> dict:
    # Função que repara a tabela 'telegram' com novas partições, com gatilho às 02:00 BRT pelo AWS Event Bridge
    
    # variáveis de ambiente
    ENRICHED_BUCKET = os.environ['AWS_S3_ENRICHED']
    
    # código principal
    client = boto3.client('athena')
    query = "MSCK REPAIR TABLE telegram"
    
    try:
        client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': f's3://{ENRICHED_BUCKET}/'
            }
        )
    except ClientError as exc:
        raise exc
        
    return dict(statusCode=200)
