# Projeto Pipeline de Dados Telegram
Este projeto visa relacionar o mundo de chatbots em aplicativos como o Telegram com computação em nuvem. No caso, é realizada uma etapa transacional para capturar dados da API do Telegram de bots pelo AWS API Gateway, e a partir disto uma etapa analítica realizada em nuvem na AWS com três etapas: ingestão, ETL e apresentação. Por fim, no AWS Athena são feitas consultas para estimar quantidade de mensagens por dia no grupo do Telegram, quantidade de mensagens por hora e dia da semana, e média de tamanho das mensagens.

<img src='https://github.com/mateus-miguel/projeto-pipeline-telegram/blob/main/imagens/aws_telegram_pipeline_v2.png?raw=true'/>
