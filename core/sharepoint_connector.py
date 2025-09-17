import requests
import os
import logging
from urllib.parse import urlparse
from dotenv import load_dotenv

class SharePointConnector:
    def __init__(self):
        """Inicializar el conector con credenciales de Azure AD"""
        load_dotenv()
        
        self.env = {
            'tenant_id': os.getenv("tenant_id"),
            'client_id': os.getenv("client_id"),
            'client_secret': os.getenv("client_secret"),
        }
        
        # Validar credenciales
        if not all(self.env.values()):
            raise ValueError("Error: Faltan credenciales de Azure AD en .env")

    def get_azure_token(self):
        """Obtener token de acceso usando Azure AD"""
        url = f"https://login.microsoftonline.com/{self.env['tenant_id']}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.env['client_id'],
            "client_secret": self.env['client_secret'],
            "scope": "https://graph.microsoft.com/.default",
        }
        
        try:
            response = requests.post(url, data=data)
            response.raise_for_status()
            return response.json()["access_token"]
        except Exception as e:
            logging.error(f"Error al obtener token de Azure AD: {e}")
            raise

    def parse_site_url(self, site_url):
        """Parsear URL de SharePoint"""
        parsed = urlparse(site_url)
        hostname = parsed.hostname
        path = parsed.path.strip("/")
        return hostname, path

    def get_site_id(self, token, site_url):
        """Obtener ID del sitio de SharePoint"""
        hostname, path = self.parse_site_url(site_url)
        url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{path}?$select=id"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()["id"]
        except Exception as e:
            logging.error(f"Error al obtener Site ID: {e}")
            raise

    def get_list_id(self, token, site_id, list_name):
        """Obtener ID de una lista específica"""
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$filter=displayName eq '{list_name}'"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            lists = data.get('value', [])
            
            if lists:
                return lists[0]['id']
            else:
                logging.error(f"Lista '{list_name}' no encontrada")
                return None
        except Exception as e:
            logging.error(f"Error al obtener ID de lista '{list_name}': {e}")
            return None