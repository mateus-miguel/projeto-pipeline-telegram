from datetime import datetime, timezone, timedelta

def parse_data(data: dict) -> dict:
    # Função que faz o data wrangling dos arquivos JSON, usando o fuso horário local BRT
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
