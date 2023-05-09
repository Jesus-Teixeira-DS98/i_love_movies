#==================================================#
#                     Imports                      #
#==================================================#
import pandas as pd
import requests
import os
import dotenv
import json
from multiprocessing import Pool

#==================================================#
#                     Variáveis                    #
#==================================================#
dotenv.load_dotenv(dotenv.find_dotenv()) # Carregando variáveis de ambiente para memória
api_key = os.getenv('api_key')

#==================================================#
#                     Funções                     #
#==================================================#
def get_data(page, api_key):
    url = f'https://api.themoviedb.org/3/movie/popular?api_key={api_key}&language=en-US&page={page}'
    response = requests.get(url)
    if response.status_code != 200:
        return None
    data = json.loads(response.text)
    return data['results']

def extract(api_key):
    pages = range(1, 1001) # A documentação da API fala que o máximo de páginas é 1000
    with Pool(processes=8) as pool: # Define o número de processos a serem executados simultaneamente
        results = pool.starmap(get_data, [(page, api_key) for page in pages])
    df = pd.concat([pd.DataFrame(r) for r in results])  
    return df


if __name__ == '__main__':
    df = extract(api_key)