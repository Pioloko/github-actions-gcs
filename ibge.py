from google.cloud import bigquery
import requests

# def captura_ibge():
#     # URL base da API do SIDRA para o IPCA
#     parametros = ['/t/1612/n2/all/v/all/p/last/c81/2715/f/u']
#     lista = []

#     for i in parametros:
#         base_url = f'https://apisidra.ibge.gov.br/values{i}'
        
#         # Faz a requisição GET para a API do SIDRA
#         response = requests.get(base_url)

#         # Verifica se a requisição foi bem-sucedida (status code 200)
#         if response.status_code == 200:
#             data = response.json()  # Converte a resposta para JSON

#             # Processa os dados e adiciona à lista
#             for idx, item in enumerate(data):
#                 # Ignorar a primeira linha (cabeçalho)
#                 if idx == 0:
#                     continue

#                 formatted_data = {
#                     "NC": item["NC"],
#                     "NN": item["NN"],
#                     "MN": item["MN"],
#                     "V": item["V"],
#                     "D1C": item["D1C"],
#                     "D1N": item["D1N"],
#                     "D2N": item["D2N"],
#                     "D3N": item["D3N"],
#                     "D4N": item["D4N"]
#                 }
#                 lista.append(formatted_data)
#         else:
#             print('Erro ao acessar a API do IBGE')

#     return lista

# def insert_into_bigquery(data, project_id, dataset_id, table_id):
#     # Configurar o cliente BigQuery
#     client = bigquery.Client(project=project_id)

#     # Referência para a tabela
#     table_ref = client.dataset(dataset_id).table(table_id)
#     table = client.get_table(table_ref)

#     # Preparar os dados para inserção
#     rows_to_insert = [list(item.values()) for item in data]

#     # Inserir os dados na tabela
#     errors = client.insert_rows(table, rows_to_insert)

#     if not errors:
#         print(f'Dados inseridos com sucesso na tabela {dataset_id}.{table_id}')
#     else:
#         print(f'Erro ao inserir dados na tabela {dataset_id}.{table_id}')
#         print(errors)


def captura_ibge():
    # URL base da API do SIDRA para o IPCA
    parametros = ['/t/1612/n2/all/v/all/p/all/c81/2715/f/u']

    lista = []

    for i in parametros:

        base_url = f'https://apisidra.ibge.gov.br/values{i}'
        
        # Faz a requisição GET para a API do SIDRA
        response = requests.get(base_url)

        # Verifica se a requisição foi bem-sucedida (status code 200)
        if response.status_code == 200:
            data = response.json()  # Converte a resposta para JSON

            # Processa os dados e adiciona à lista
            for idx, item in enumerate(data):
                # Ignorar a primeira linha (cabeçalho)
                if idx == 0:
                    continue

                formatted_data = {
                    "NC": item["NC"],
                    "NN": item["NN"],
                    "MN": item["MN"],
                    "V": item["V"],
                    "D1C": item["D1C"],
                    "D1N": item["D1N"],
                    "D2N": item["D2N"],
                    "D3N": item["D3N"],
                    "D4N": item["D4N"]
                }
                lista.append(formatted_data)
        else:
            print('Erro ao acessar a API do IBGE')

    return lista



import json
import os
import datetime
data = datetime.date.today()

def salva_em_json(data, nome_arquivo='dados_json'):

    # Obtém o diretório de trabalho atual
    diretorio_atual = os.getcwd()
    caminho_arquivo = os.path.join(diretorio_atual, nome_arquivo)

    with open(caminho_arquivo, 'w') as arquivo:
        json.dump(data, arquivo)




def ler_arquivo_json(nome_arquivo):
    with open(nome_arquivo, 'r') as arquivo:
        dados = json.load(arquivo)
        dados_filtrados = [item for item in dados if 'D3N' in item and item['D3N'] == '1979']
    return dados_filtrados

print(ler_arquivo_json('dados_ibge_2024-01-18'))