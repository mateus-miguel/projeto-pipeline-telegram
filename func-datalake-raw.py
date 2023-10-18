import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3

def lambda_handler(event: dict, context: dict) -> dict:
  """
  Recebe uma mensagem do Telegram via AWS API Gateway, verifica
  se seu conteúdo foi produzido em determinado grupo e escreve
  em seu formato original JSON, em um bucket AWS S3
  """

  # variáveis de ambiente

  BUCKET = os.environ['AWS_S3_BUCKET']
  TELEGRAM_CHAT_ID = int(os.environ['TELEGRAM_CHAT_ID'])

  # variáveis lógicas

  tzinfo = timezone(offset=timedelta(hours=-3))
  date = datetime.now(tzinfo).strftime('%Y-%m-%d')
  timestamp = datetime.now(tzinfo).strftime('%Y%m%d%H%M%S%f')

  filename = f'{timestamp}.json'

  # código principal

  client = boto3.client('s3')

  try:
    message = json.loads(event['body'])
    # message = event
    chat_id = message['message']['chat']['id']

    if chat_id == TELEGRAM_CHAT_ID:
      with open(f'/tmp/{filename}', mode='w', encoding='utf8') as f:
        json.dump(message, f)
      client.upload_file(f'/tmp/{filename}', BUCKET, f'telegram/context_date={date}/{filename}')
      
  except Exception as exc:
    logging.error(msg=exc)
    return dict(statusCode='500')
    
  else:
    return dict(statusCode='200')
