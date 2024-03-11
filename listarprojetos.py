import requests
import datetime
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build


def get_project_id():

    try:
        # Carregue as credenciais de serviço do Google Cloud
        # credentials = service_account.Credentials.from_service_account_file(
        #     credentials_path,
        #     scopes=['https://www.googleapis.com/auth/cloud-platform']
        # )

        # Crie um cliente para o serviço Cloud Resource Manager
        # crm = build('cloudresourcemanager', 'v1', credentials=credentials)
        crm = build('cloudresourcemanager', 'v1')

        # Liste os projetos
        projects = crm.projects().list().execute()
        if 'projects' in projects:
            return [project['projectId'] for project in projects['projects']]
        else:
            return []

    except Exception as e:
        print(f"Erro ao listar os projetos do Google Cloud: {e}")
        return None




print(get_project_id())