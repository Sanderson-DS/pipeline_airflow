import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# intervalo de datas
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# formato de datas
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')
print(f'\nPeríodo: {data_inicio} a {data_fim}\n')

city = 'Boston'
key = os.getenv("KEY")
print(f'Cidade: {city}\n')
print('Autenticação realizada com sucesso! \n')


url = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')
print(f'leitura da URL realizada: {url}\n')

dados = pd.read_csv(url)    
print(dados.head())
print(f'Quantidade de linhas: {len(dados)}\n')

file_path = f'/home/sanderson/repos/pipeline_airflow/semana={data_inicio}/'
os.mkdir(file_path)

dados.to_csv(file_path + 'dados_brutos.csv')
print(f'Arquivo de dados brutos salvo em: {file_path}dados_brutos.csv \n')
dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatura.csv')
print(f'Arquivo de temperatura salvo em: {file_path}temperatura.csv \n')
dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
print(f'Arquivo de condições do tempo salvo em: {file_path}condicoes.csv \n')