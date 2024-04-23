import os
import logging
import json
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename='logs/ingestion.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )

app_api = os.getenv('id_project')

url = "http://ws.audioscrobbler.com/2.0/"

metodos = ['user.getInfo','user.gettopartists','user.getRecentTracks','user.getTopTracks']



params = {
    'user':'ditopovoa',
    'api_key':app_api,
    'format':'json'
}

data = []

for metodo in metodos:
    logging.warning(f"Extracao de dados do Metodo {metodo} iniciado")
    params['method'] = metodo 
    params['page'] = 1

    try:
        req = requests.get(url,params=params)
        retorno_request = json.loads(req.content.decode('utf-8'))
    except Exception as ex:
        logging.error(f"Falha ao extrair os registros do metodo {metodo} - > Erro: {ex}")

    repon = req.json()
    find_first_key = iter(repon)
    first_key = next(find_first_key)
    
    try:
        first_key_return = repon.get(first_key,'0')
        second_key = first_key_return.get('@attr','0')
        total_pages = int(second_key.get('totalPages','0')) if second_key != '0' else 0
        actual = int(second_key.get('page','0')) if second_key != '0' else 0

    except Exception as ex:
        logging.error(f"Falha ao obter objetos no processo -> {ex}")

    if total_pages == actual:
        logging.info("Nao existe mais de uma pagina nesse metodo")
        try:
            name_file = f"data/in/{metodo.split('.')[1]}.json"
            with open(name_file, 'w') as arquivo:
                json.dump(retorno_request, arquivo)
        except Exception as ex:
            logging.error(f"Falha ao escrever o arquivo JSON {name_file} na pasta do projeto -> {ex}")

    else:
        logging.info(f"Foram encontradas {total_pages} de dados nesse metodo, dados da {actual} pagina extraido")
        data.append(retorno_request)
        
        actual += 1

        while actual <= total_pages:
            try:
                params['page'] = actual
                req = requests.get(url,params=params)
                retorno_request = json.loads(req.content.decode('utf-8'))
                data.append(retorno_request)
                logging.info(f"Dados da {actual} pagina extraido")
                actual += 1
            except Exception as ex:
                logging.error(f"Falha ao extrair os dados da pagina {actual} em um total de {total_pages} -> Erro: {ex}")

        try:
            name_file = f"data/in/{metodo.split('.')[1]}.json"
            with open(name_file, 'a') as arquivo:
                for position,dado in enumerate(data):
                    objeto = {"posicao":position, "corpo":dado}
                    json.dump(objeto, arquivo)
                    arquivo.write('\n')

        except Exception as ex:
            logging.error(f"Falha ao escrever o arquivo JSON {name_file} na pasta do projeto -> {ex}")

    
    logging.warning(f"Extraido todos os dados do Metodo {metodo}")


    




    
