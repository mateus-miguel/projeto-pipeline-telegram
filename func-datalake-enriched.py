import os
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
# O PyArrow é instalado numa Layer para poder ser usado pela função AWS Lambda, pois ela só contém poucas bibliotecas por padrão

def lambda_handler(event: dict, context: dict) -> bool:
    """
    Função que puxa os arquivos do dia anterior do raw bucket S3
    e realiza um data wrangling de todos os arquivos JSON para persistir
    como tabela do formato .parquet dentro do enriched bucket S3
    """
    
    # variáveis de ambiente
    
    RAW_BUCKET = os.environ['AWS_S3_RAW']
    ENRICHED_BUCKET = os.environ['AWS_S3_ENRICHED']
    
    # variáveis lógicas
    
    tzinfo = timezone(offset=timedelta(hours=-3))
    date = (datetime.now(tzinfo) - timedelta(days=1)).strftime('%Y-%m-%d') # dia anterior, com offset de timedelta(days=1)
    timestamp = datetime.now(tzinfo).strftime('%Y%m%d%H%M%S%f')
    
    # código principal
    
    table = None
    client = boto3.client('s3')
    
    try:
        # listando arquivos JSON do Raw Bucket pela pasta do dia anterior
        response = client.list_objects_v2(Bucket=RAW_BUCKET, Prefix=f'telegram/context_date={date}')
        
        for content in response['Contents']:
            key = content['Key']
            arquivo = key.split('/')[-1]
            client.download_file(RAW_BUCKET, key, f'/tmp/{arquivo}')
            
            with open(f'/tmp/{arquivo}', mode='r', encoding='utf8') as f:
                data = json.load(f)
                data = data['message']

            # É feito data wrangling para formato tabular, e então usado o PyArrow para criar uma table .parquet
            parsed_data = parse_data(data=data) # data wrangling
            iter_table = pa.Table.from_pydict(mapping=parsed_data)
            
            if table:
                table = pa.concat_tables([table, iter_table]) # concatenação dos arquivos JSON diários em forma tabular
            else:
                table = iter_table
                iter_table = None
                
        pq.write_table(table=table, where=f'/tmp/{timestamp}.parquet')
        client.upload_file(f'/tmp/{timestamp}.parquet', ENRICHED_BUCKET, f'context_date={date}/{timestamp}.parquet')
            
        return True
        
    except Exception as exc:
        logging.error(msg=exc)
        return False 
        

def parse_data(data: dict) -> dict:
    # Função que faz o data wrangling para ir dos arquivos .json para o .parquet diário
    parsed_data = dict()
    
    for key, value in data.items():
        if key == 'from':
            for k, v in data[key].items():
                if k in ['id', 'is_bot', 'first_name']:
                    parsed_data[f'user_{k}'] = [v]
        
        elif key == 'chat':
            for k, v in data[key].items():
                if k in ['id', 'type']:
                    parsed_data[f'chat_{k}'] = [v]
                    
        elif key in ['message_id', 'text']:
            parsed_data[key] = [value]
            
        elif key == 'date':
            tzinfo = timezone(offset=timedelta(hours=-3))
            parsed_data[key] = [value]
            parsed_data['timestamp'] = [datetime.fromtimestamp(value, tzinfo).strftime('%Y-%m-%d %H:%M:%S')]
            
    if not 'text' in parsed_data.keys():
        parsed_data['text'] = ['']
        
    return parsed_data
