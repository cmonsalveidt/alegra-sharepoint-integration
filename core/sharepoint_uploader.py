import requests
import os
import json
import logging
from io import BytesIO
from datetime import datetime
from typing import Optional, Dict, Any, Union
from core.sharepoint_connector import SharePointConnector

class SharePointUploader:
    """
    Clase para subir archivos a SharePoint de forma sencilla y reutilizable.
    
    Características:
    - Sube cualquier tipo de archivo (Excel, PDF, Word, imágenes, etc.)
    - Crea carpetas automáticamente si no existen
    - Maneja archivos en memoria o desde disco
    - Proporciona URLs de descarga
    - Manejo robusto de errores con logging
    """
    
    def __init__(self, site_url: str, logger: Optional[logging.Logger] = None):
        """
        Inicializar el uploader con la URL del sitio de SharePoint.
        
        Args:
            site_url: URL del sitio de SharePoint (ej: "https://company.sharepoint.com/sites/sitename")
            logger: Logger opcional. Si no se proporciona, se crea uno básico.
        """
        self.site_url = site_url
        self.sp_connector = SharePointConnector()
        self._token = None
        self._site_id = None
        
        # Configurar logger
        if logger is None:
            self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
        
    def _get_auth_info(self):
        """Obtener token de autenticación y site ID."""
        if not self._token or not self._site_id:
            self.logger.debug("Obteniendo información de autenticación")
            self._token = self.sp_connector.get_azure_token()
            self._site_id = self.sp_connector.get_site_id(self._token, self.site_url)
            self.logger.debug(f"Site ID obtenido: {self._site_id}")
        return self._token, self._site_id
    
    def upload_file_from_path(self, 
                             file_path: str, 
                             folder_path: str = "", 
                             new_filename: str = None,
                             overwrite: bool = True) -> Dict[str, Any]:
        """
        Subir archivo desde una ruta local.
        
        Args:
            file_path: Ruta al archivo local
            folder_path: Carpeta destino en SharePoint (ej: "Documentos compartidos/Reportes")
            new_filename: Nuevo nombre para el archivo (opcional)
            overwrite: Si sobrescribir archivo existente
            
        Returns:
            Dict con resultado de la operación
        """
        try:
            self.logger.info(f"Iniciando subida de archivo: {file_path}")
            
            if not os.path.exists(file_path):
                self.logger.error(f"Archivo no encontrado: {file_path}")
                return {'success': False, 'error': f'Archivo no encontrado: {file_path}'}
            
            file_size = os.path.getsize(file_path)
            self.logger.info(f"Tamaño del archivo: {file_size} bytes")
            
            with open(file_path, 'rb') as file:
                file_bytes = file.read()
            
            filename = new_filename or os.path.basename(file_path)
            self.logger.info(f"Nombre final del archivo: {filename}")
            
            return self.upload_file_from_bytes(file_bytes, filename, folder_path, overwrite)
            
        except Exception as e:
            self.logger.error(f"Error leyendo archivo {file_path}: {str(e)}")
            return {'success': False, 'error': f'Error leyendo archivo: {str(e)}'}
    
    def upload_file_from_bytes(self, 
                              file_bytes: bytes, 
                              filename: str, 
                              folder_path: str = "",
                              overwrite: bool = True) -> Dict[str, Any]:
        """
        Subir archivo desde bytes en memoria.
        
        Args:
            file_bytes: Contenido del archivo en bytes
            filename: Nombre del archivo
            folder_path: Carpeta destino en SharePoint
            overwrite: Si sobrescribir archivo existente
            
        Returns:
            Dict con resultado de la operación
        """
        try:
            self.logger.info(f"Subiendo archivo desde memoria: {filename}")
            self.logger.info(f"Tamaño: {len(file_bytes)} bytes, Carpeta: '{folder_path}'")
            
            token, site_id = self._get_auth_info()
            
            if not token or not site_id:
                self.logger.error("No se pudo autenticar con SharePoint")
                return {'success': False, 'error': 'No se pudo autenticar con SharePoint'}
            
            # Construir URL de subida
            upload_url = self._build_upload_url(site_id, folder_path, filename)
            self.logger.debug(f"URL de subida: {upload_url}")
            
            # Determinar content-type basado en la extensión
            content_type = self._get_content_type(filename)
            self.logger.debug(f"Content-Type: {content_type}")
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": content_type
            }
            
            # Si no queremos sobrescribir, verificar si el archivo existe
            if not overwrite and self._file_exists(site_id, folder_path, filename):
                base_name, extension = os.path.splitext(filename)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{base_name}_{timestamp}{extension}"
                upload_url = self._build_upload_url(site_id, folder_path, filename)
                self.logger.info(f"Archivo existe, usando nombre único: {filename}")
            
            # Subir archivo
            self.logger.debug("Ejecutando request PUT para subir archivo")
            response = requests.put(upload_url, headers=headers, data=file_bytes)
            
            if response.status_code in [200, 201]:
                file_info = response.json()
                self.logger.info(f"Archivo subido exitosamente: {file_info.get('name')}")
                self.logger.info(f"URL del archivo: {file_info.get('webUrl')}")
                
                return {
                    'success': True,
                    'file_id': file_info.get('id'),
                    'filename': file_info.get('name'),
                    'web_url': file_info.get('webUrl'),
                    'download_url': file_info.get('@microsoft.graph.downloadUrl'),
                    'size': file_info.get('size'),
                    'created_datetime': file_info.get('createdDateTime'),
                    'modified_datetime': file_info.get('lastModifiedDateTime')
                }
            else:
                # Si error 404, intentar crear la carpeta
                if response.status_code == 404 and folder_path:
                    self.logger.warning(f"Carpeta no existe, intentando crear: {folder_path}")
                    folder_created = self.create_folder(folder_path)
                    if folder_created['success']:
                        self.logger.info("Carpeta creada exitosamente, reintentando subida")
                        # Reintentar subida
                        response = requests.put(upload_url, headers=headers, data=file_bytes)
                        if response.status_code in [200, 201]:
                            file_info = response.json()
                            self.logger.info(f"Archivo subido exitosamente tras crear carpeta: {file_info.get('name')}")
                            
                            return {
                                'success': True,
                                'file_id': file_info.get('id'),
                                'filename': file_info.get('name'),
                                'web_url': file_info.get('webUrl'),
                                'download_url': file_info.get('@microsoft.graph.downloadUrl'),
                                'size': file_info.get('size'),
                                'created_datetime': file_info.get('createdDateTime'),
                                'modified_datetime': file_info.get('lastModifiedDateTime'),
                                'folder_created': True
                            }
                    else:
                        self.logger.error(f"No se pudo crear la carpeta: {folder_created.get('error')}")
                
                self.logger.error(f"Error HTTP {response.status_code} subiendo archivo: {response.text}")
                return {
                    'success': False, 
                    'error': f'Error HTTP {response.status_code}: {response.text}',
                    'status_code': response.status_code
                }
                
        except Exception as e:
            self.logger.error(f"Error durante la subida de {filename}: {str(e)}")
            return {'success': False, 'error': f'Error durante la subida: {str(e)}'}
    
    def upload_excel_from_dataframes(self, 
                                   dataframes: Dict[str, Any], 
                                   filename: str, 
                                   folder_path: str = "") -> Dict[str, Any]:
        """
        Crear y subir archivo Excel desde DataFrames de pandas.
        
        Args:
            dataframes: Dict con {"nombre_hoja": dataframe}
            filename: Nombre del archivo Excel
            folder_path: Carpeta destino
            
        Returns:
            Dict con resultado de la operación
        """
        try:
            import pandas as pd
            
            self.logger.info(f"Creando archivo Excel desde DataFrames: {filename}")
            self.logger.info(f"Hojas a crear: {list(dataframes.keys())}")
            
            buffer = BytesIO()
            
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                for sheet_name, df in dataframes.items():
                    self.logger.debug(f"Escribiendo hoja '{sheet_name}' con {len(df)} filas")
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            buffer.seek(0)
            file_bytes = buffer.read()
            buffer.close()
            
            if not filename.endswith('.xlsx'):
                filename += '.xlsx'
            
            self.logger.info(f"Excel creado en memoria. Tamaño: {len(file_bytes)} bytes")
            
            return self.upload_file_from_bytes(file_bytes, filename, folder_path)
            
        except ImportError:
            self.logger.error("pandas no está instalado")
            return {'success': False, 'error': 'pandas no está instalado'}
        except Exception as e:
            self.logger.error(f"Error creando Excel: {str(e)}")
            return {'success': False, 'error': f'Error creando Excel: {str(e)}'}
    
    def create_folder(self, folder_path: str) -> Dict[str, Any]:
        """
        Crear carpeta en SharePoint.
        
        Args:
            folder_path: Ruta de la carpeta a crear
            
        Returns:
            Dict con resultado de la operación
        """
        try:
            self.logger.info(f"Creando carpeta: {folder_path}")
            token, site_id = self._get_auth_info()
            
            # Dividir la ruta en partes
            path_parts = [part for part in folder_path.split('/') if part]
            current_path = ""
            
            for part in path_parts:
                current_path = f"{current_path}/{part}" if current_path else part
                
                self.logger.debug(f"Verificando carpeta: {current_path}")
                
                # Verificar si la carpeta existe
                if not self._folder_exists(site_id, current_path):
                    self.logger.debug(f"Creando carpeta: {part}")
                    
                    # Crear carpeta
                    parent_path = "/".join(current_path.split("/")[:-1]) if "/" in current_path else ""
                    folder_name = current_path.split("/")[-1]
                    
                    if parent_path:
                        parent_encoded = parent_path.replace(" ", "%20")
                        create_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parent_encoded}:/children"
                    else:
                        create_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
                    
                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json"
                    }
                    
                    folder_data = {
                        "name": folder_name,
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "rename"
                    }
                    
                    response = requests.post(create_url, headers=headers, json=folder_data)
                    
                    if response.status_code not in [200, 201]:
                        self.logger.error(f"Error creando carpeta {folder_name}: {response.status_code} - {response.text}")
                        return {
                            'success': False, 
                            'error': f'Error creando carpeta {folder_name}: {response.text}'
                        }
                    else:
                        self.logger.info(f"Carpeta creada: {folder_name}")
                else:
                    self.logger.debug(f"Carpeta ya existe: {current_path}")
            
            self.logger.info(f"Estructura de carpetas verificada/creada: {folder_path}")
            return {'success': True, 'folder_path': folder_path}
            
        except Exception as e:
            self.logger.error(f"Error creando carpeta {folder_path}: {str(e)}")
            return {'success': False, 'error': f'Error creando carpeta: {str(e)}'}
    
    def list_files(self, folder_path: str = "") -> Dict[str, Any]:
        """
        Listar archivos en una carpeta de SharePoint.
        
        Args:
            folder_path: Ruta de la carpeta (vacío para raíz)
            
        Returns:
            Dict con lista de archivos
        """
        try:
            self.logger.info(f"Listando contenido de carpeta: '{folder_path}'")
            token, site_id = self._get_auth_info()
            
            if folder_path:
                folder_encoded = folder_path.replace(" ", "%20")
                list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_encoded}:/children"
            else:
                list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(list_url, headers=headers)
            
            if response.status_code == 200:
                items = response.json().get('value', [])
                files = []
                folders = []
                
                for item in items:
                    if 'file' in item:
                        files.append({
                            'name': item.get('name'),
                            'id': item.get('id'),
                            'size': item.get('size'),
                            'web_url': item.get('webUrl'),
                            'created_datetime': item.get('createdDateTime'),
                            'modified_datetime': item.get('lastModifiedDateTime')
                        })
                    elif 'folder' in item:
                        folders.append({
                            'name': item.get('name'),
                            'id': item.get('id'),
                            'web_url': item.get('webUrl'),
                            'created_datetime': item.get('createdDateTime')
                        })
                
                self.logger.info(f"Encontrados {len(files)} archivos y {len(folders)} carpetas")
                
                return {
                    'success': True,
                    'files': files,
                    'folders': folders,
                    'total_files': len(files),
                    'total_folders': len(folders)
                }
            else:
                self.logger.error(f"Error listando archivos: {response.status_code} - {response.text}")
                return {
                    'success': False,
                    'error': f'Error listando archivos: {response.text}'
                }
                
        except Exception as e:
            self.logger.error(f"Error listando archivos en '{folder_path}': {str(e)}")
            return {'success': False, 'error': f'Error listando archivos: {str(e)}'}
    
    def _build_upload_url(self, site_id: str, folder_path: str, filename: str) -> str:
        """Construir URL para subir archivo."""
        if folder_path:
            folder_encoded = folder_path.replace(" ", "%20")
            return f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_encoded}/{filename}:/content"
        else:
            return f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children/{filename}/content"
    
    def _get_content_type(self, filename: str) -> str:
        """Determinar content-type basado en la extensión del archivo."""
        extension = filename.lower().split('.')[-1]
        
        content_types = {
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xls': 'application/vnd.ms-excel',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'doc': 'application/msword',
            'pdf': 'application/pdf',
            'txt': 'text/plain',
            'csv': 'text/csv',
            'json': 'application/json',
            'png': 'image/png',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'gif': 'image/gif',
            'zip': 'application/zip'
        }
        
        return content_types.get(extension, 'application/octet-stream')
    
    def _file_exists(self, site_id: str, folder_path: str, filename: str) -> bool:
        """Verificar si un archivo existe."""
        try:
            token = self._token
            
            if folder_path:
                folder_encoded = folder_path.replace(" ", "%20")
                check_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_encoded}/{filename}"
            else:
                check_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children/{filename}"
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(check_url, headers=headers)
            exists = response.status_code == 200
            
            if exists:
                self.logger.debug(f"Archivo existe: {filename}")
            
            return exists
            
        except Exception as e:
            self.logger.debug(f"Error verificando existencia de archivo: {str(e)}")
            return False
    
    def _folder_exists(self, site_id: str, folder_path: str) -> bool:
        """Verificar si una carpeta existe."""
        try:
            token = self._token
            
            folder_encoded = folder_path.replace(" ", "%20")
            check_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_encoded}"
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(check_url, headers=headers)
            exists = response.status_code == 200
            
            if exists:
                self.logger.debug(f"Carpeta existe: {folder_path}")
            
            return exists
            
        except Exception as e:
            self.logger.debug(f"Error verificando existencia de carpeta: {str(e)}")
            return False

# Función de conveniencia para uso rápido
def quick_upload(file_path: str, 
                site_url: str, 
                folder_path: str = "",
                new_filename: str = None,
                logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Función rápida para subir un archivo sin crear instancia de clase.
    
    Args:
        file_path: Ruta al archivo local
        site_url: URL del sitio de SharePoint
        folder_path: Carpeta destino (opcional)
        new_filename: Nuevo nombre (opcional)
        logger: Logger opcional
        
    Returns:
        Dict con resultado de la operación
    """
    uploader = SharePointUploader(site_url, logger)
    return uploader.upload_file_from_path(file_path, folder_path, new_filename)