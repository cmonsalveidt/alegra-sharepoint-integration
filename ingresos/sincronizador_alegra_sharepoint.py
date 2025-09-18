import requests
import base64
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Agregar el directorio padre al path para importaciones
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sharepoint_connector import SharePointConnector

def setup_logging():
    """Configurar el sistema de logging"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/sincronizacion_alegra_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
        ]
    )
    
    return log_filename

class SincronizadorAlegra:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()
        
        # Credenciales Alegra
        username = os.getenv("email")
        password = os.getenv("password")
        credentials = f"{username}:{password}"
        self.encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Configuración SharePoint
        self.site_url = os.getenv("site_url")
        self.list_name_pagos = os.getenv("list_pagos")
        self.list_name_facturas = os.getenv("list_facturas") 
        self.list_name_items = os.getenv("list_items")
        
        # Inicializar conector SharePoint
        self.sp_connector = SharePointConnector()
        
        # Contadores para estadísticas
        self.stats = {
            'pagos_revisados': 0,
            'pagos_recreados': 0,
            'pagos_sin_cambios': 0,
            'pagos_error': 0,
            'facturas_recreadas': 0,
            'facturas_sin_cambios': 0,
            'facturas_error': 0,
            'items_recreados': 0,
            'items_eliminados': 0,
            'registros_eliminados': 0
        }

    def main(self):
        """Función principal del sincronizador"""
        self.logger.info("="*60)
        self.logger.info("SINCRONIZACIÓN ALEGRA-SHAREPOINT - ESTRATEGIA DELETE+CREATE")
        self.logger.info("="*60)
        
        try:
            # Paso 1: Obtener pagos sin cliente de SharePoint
            pagos_sin_cliente = self.obtener_pagos_sin_cliente()
            
            if not pagos_sin_cliente:
                self.logger.info("No se encontraron pagos sin cliente asignado")
                return True
            
            self.logger.info(f"Encontrados {len(pagos_sin_cliente)} pagos sin cliente para revisar")
            
            # Paso 2: Procesar cada pago
            for pago_sp in pagos_sin_cliente:
                try:
                    self.procesar_pago_sin_cliente_delete_create(pago_sp)
                except Exception as e:
                    self.logger.error(f"Error procesando pago {pago_sp.get('Title', 'N/A')}: {str(e)}")
                    self.stats['pagos_error'] += 1
                    continue
            
            # Resumen final
            self.mostrar_resumen_final()
            return True
            
        except Exception as e:
            self.logger.error(f"Error crítico en sincronización: {str(e)}")
            return False

    def obtener_pagos_sin_cliente(self):
        """Obtener pagos de SharePoint que no tienen cliente asignado"""
        try:
            self.logger.info(" Obteniendo pagos sin cliente de SharePoint...")
            
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_pagos)
            
            if not list_id:
                self.logger.error(f"No se pudo obtener ID de la lista {self.list_name_pagos}")
                return []
            
            # Obtener todos los pagos y filtrar en Python
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"

            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }

            all_pagos = []
            next_url = url

            # Manejar paginación
            while next_url:
                response = requests.get(next_url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    all_pagos.extend(data.get('value', []))
                    next_url = data.get('@odata.nextLink')
                    if next_url:
                        self.logger.info(f" Obteniendo siguiente página de pagos...")
                else:
                    self.logger.error(f"Error obteniendo pagos: {response.status_code} - {response.text}")
                    break

            self.logger.info(f" Total de pagos obtenidos de SharePoint: {len(all_pagos)}")

            if all_pagos:
                pagos_sin_cliente = []
                
                for item in all_pagos:
                    fields = item.get('fields', {})
                    # Solo incluir si realmente no tiene cliente
                    if (not fields.get('ID_x0020_Cliente') or 
                        not fields.get('Nombre_x0020_Cliente') or
                        fields.get('ID_x0020_Cliente', '').strip() == '' or
                        fields.get('Nombre_x0020_Cliente', '').strip() == ''):
                        
                        pago_data = {
                            'SharePoint_ID': item.get('id'),
                            'Title': fields.get('Title', ''),
                            'Pago_ID': fields.get('Title', ''),
                            'Numero_Pago': fields.get('Numero_x0020_Pago', ''),
                            'ID_Cliente_Actual': fields.get('ID_x0020_Cliente', ''),
                            'Nombre_Cliente_Actual': fields.get('Nombre_x0020_Cliente', ''),
                            'ID_Factura': fields.get('ID_x0020_Factura', ''),
                            'Numero_Factura': fields.get('Numero_x0020_Factura', ''),
                            'fields': fields
                        }
                        pagos_sin_cliente.append(pago_data)
                
                self.logger.info(f" Encontrados {len(pagos_sin_cliente)} pagos sin cliente en SharePoint")
                return pagos_sin_cliente
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Error obteniendo pagos sin cliente: {str(e)}")
            return []

    def diagnosticar_pago(self, pago_id):
        """Diagnosticar todos los registros de un pago específico ANTES de eliminar"""
        try:
            self.logger.info(f" DIAGNÓSTICO COMPLETO DEL PAGO {pago_id}")
            self.logger.info("="*50)
            
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_pagos)
            
            if not list_id:
                self.logger.error("No se pudo obtener ID de lista de pagos")
                return
            
            # Obtener TODOS los registros
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            all_items = []
            next_url = url
            
            while next_url:
                response = requests.get(next_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_items.extend(data.get('value', []))
                    next_url = data.get('@odata.nextLink')
                else:
                    break
            
            # Filtrar registros de este pago
            registros_pago = []
            for item in all_items:
                fields = item.get('fields', {})
                title = fields.get('Title', '')
                if str(title) == str(pago_id):
                    registros_pago.append(item)
            
            self.logger.info(f" ENCONTRADOS {len(registros_pago)} REGISTROS PARA EL PAGO {pago_id}:")
            
            for i, item in enumerate(registros_pago, 1):
                fields = item.get('fields', {})
                sharepoint_id = item.get('id')
                numero_pago = fields.get('Numero_x0020_Pago', 'N/A')
                cliente_id = fields.get('ID_x0020_Cliente', '')
                cliente_nombre = fields.get('Nombre_x0020_Cliente', '')
                factura_id = fields.get('ID_x0020_Factura', '')
                monto = fields.get('Monto_x0020_Total', 0)
                
                self.logger.info(f"  {i}. ID SharePoint: {sharepoint_id}")
                self.logger.info(f"     Número Pago: {numero_pago}")
                self.logger.info(f"     Cliente ID: '{cliente_id}' | Nombre: '{cliente_nombre}'")
                self.logger.info(f"     Factura ID: '{factura_id}' | Monto: ${monto}")
                self.logger.info(f"     Estado Cliente: {'CON CLIENTE' if cliente_id and cliente_id.strip() else 'SIN CLIENTE'}")
                self.logger.info(f"     ---")
            
            return registros_pago
            
        except Exception as e:
            self.logger.error(f"Error en diagnóstico: {str(e)}")
            return []

    def procesar_pago_sin_cliente_delete_create(self, pago_sp):
        """Procesar un pago sin cliente usando estrategia DELETE + CREATE"""
        pago_id = pago_sp['Pago_ID']
        numero_pago = pago_sp['Numero_Pago']
        sharepoint_id = pago_sp['SharePoint_ID']
        
        self.logger.info(f" Procesando pago {numero_pago} (ID: {pago_id})")
        self.stats['pagos_revisados'] += 1
        
        # DIAGNÓSTICO INICIAL
        self.diagnosticar_pago(pago_id)
        
        # PASO 1: Obtener datos actuales del pago desde Alegra
        pago_alegra = self.obtener_pago_desde_alegra(pago_id)
        
        if not pago_alegra:
            self.logger.warning(f" No se pudo obtener pago {pago_id} desde Alegra")
            self.stats['pagos_error'] += 1
            return
        
        # PASO 2: Verificar si ahora tiene cliente
        cliente_alegra = self.safe_get_nested(pago_alegra, 'client', 'id', default='')
        
        if not cliente_alegra:
            self.logger.info(f" Pago {numero_pago} sigue sin cliente en Alegra")
            self.stats['pagos_sin_cambios'] += 1
            return
        
        self.logger.info(f" Pago {numero_pago} ahora tiene cliente: {self.safe_get_nested(pago_alegra, 'client', 'name', default='N/A')}")
        
        # PASO 3: Recopilar facturas que se verán afectadas
        facturas_a_recrear = set()
        
        # Facturas del pago original
        factura_original = pago_sp.get('ID_Factura', '')
        if factura_original and factura_original.strip():
            facturas_a_recrear.add(factura_original)
            self.logger.info(f" Factura original detectada: {factura_original}")
        
        # Facturas del pago actualizado en Alegra
        invoices = pago_alegra.get('invoices', [])
        for invoice in invoices:
            if invoice and invoice.get('id'):
                facturas_a_recrear.add(str(invoice.get('id')))
                self.logger.info(f" Factura en Alegra detectada: {invoice.get('id')}")
        
        # PASO 4: ELIMINAR registros viejos del pago
        self.logger.info(f" Eliminando registros viejos del pago {numero_pago}...")
        registros_eliminados = self.eliminar_registros_pago(pago_id)
        self.stats['registros_eliminados'] += registros_eliminados
        
        # VERIFICACIÓN POST-ELIMINACIÓN
        self.logger.info(f" VERIFICACIÓN POST-ELIMINACIÓN:")
        registros_restantes = self.diagnosticar_pago(pago_id)
        if len(registros_restantes) > 0:
            self.logger.warning(f" AÚN QUEDAN {len(registros_restantes)} REGISTROS - REINTENTANDO ELIMINACIÓN")
            # Reintentar eliminación
            for registro in registros_restantes:
                sharepoint_id = registro.get('id')
                if self.eliminar_item_sharepoint(sharepoint_id, self.list_name_pagos):
                    self.logger.info(f"   Eliminado registro restante {sharepoint_id}")
                    registros_eliminados += 1
        
        # PASO 5: CREAR registros nuevos del pago
        self.logger.info(f" Creando registros nuevos del pago {numero_pago}...")
        pagos_unificados = self.procesar_pago_alegra_unificado(pago_alegra)
        
        registros_creados = 0
        for pago_data in pagos_unificados:
            if self.crear_pago_sharepoint(pago_data):
                registros_creados += 1
        
        if registros_creados > 0:
            self.stats['pagos_recreados'] += 1
            self.logger.info(f" Pago recreado: {registros_eliminados} eliminados → {registros_creados} creados")
            
            # VERIFICACIÓN FINAL
            self.logger.info(f" VERIFICACIÓN FINAL:")
            self.diagnosticar_pago(pago_id)
            
            # PASO 6: RECREAR facturas afectadas
            for factura_id in facturas_a_recrear:
                if factura_id and factura_id.strip():
                    self.recrear_factura_completa(factura_id)
        else:
            self.stats['pagos_error'] += 1
            self.logger.error(f" Error recreando pago {numero_pago}")

    def eliminar_registros_pago(self, pago_id):
        """Eliminar TODOS los registros de un pago específico en SharePoint"""
        try:
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_pagos)
            
            if not list_id:
                return 0
            
            # Buscar TODOS los registros con este pago_id - SIN FILTRO para obtener TODO
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            all_items = []
            next_url = url
            
            # Obtener TODOS los registros con paginación
            while next_url:
                response = requests.get(next_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_items.extend(data.get('value', []))
                    next_url = data.get('@odata.nextLink')
                else:
                    self.logger.error(f"Error obteniendo registros: {response.status_code}")
                    break
            
            # Filtrar en Python los registros que corresponden a este pago_id
            registros_pago = []
            for item in all_items:
                fields = item.get('fields', {})
                title = fields.get('Title', '')
                if str(title) == str(pago_id):
                    registros_pago.append(item)
            
            self.logger.info(f" Encontrados {len(registros_pago)} registros del pago {pago_id} para eliminar")
            
            eliminados = 0
            for item in registros_pago:
                try:
                    item_id = item.get('id')
                    sharepoint_id = item.get('id')
                    
                    # Información del registro que se va a eliminar
                    fields = item.get('fields', {})
                    cliente_actual = fields.get('Nombre_x0020_Cliente', 'SIN CLIENTE')
                    numero_pago = fields.get('Numero_x0020_Pago', 'N/A')
                    
                    self.logger.info(f"   Eliminando registro ID {sharepoint_id}: {numero_pago} - Cliente: {cliente_actual}")
                    
                    if self.eliminar_item_sharepoint(sharepoint_id, self.list_name_pagos):
                        eliminados += 1
                        self.logger.info(f"   Eliminado registro {sharepoint_id}")
                    else:
                        self.logger.warning(f"   Error eliminando registro {sharepoint_id}")
                        
                except Exception as e:
                    self.logger.error(f"   Error procesando registro: {str(e)}")
                    continue
            
            self.logger.info(f" TOTAL ELIMINADOS: {eliminados} registros del pago {pago_id}")
            
            # Pausa para asegurar que las eliminaciones se procesen
            if eliminados > 0:
                import time
                time.sleep(2)
                self.logger.info(" Pausa para asegurar procesamiento de eliminaciones")
            
            return eliminados
                
        except Exception as e:
            self.logger.error(f"Error eliminando registros del pago {pago_id}: {str(e)}")
            return 0

    def recrear_factura_completa(self, factura_id):
        """Recrear completamente una factura (DELETE + CREATE)"""
        try:
            self.logger.info(f" Recreando factura completa: {factura_id}")
            
            # PASO 1: Obtener datos actuales de Alegra
            factura_alegra = self.obtener_factura_desde_alegra(factura_id)
            
            if not factura_alegra:
                self.logger.warning(f" No se pudo obtener factura {factura_id} desde Alegra")
                self.stats['facturas_error'] += 1
                return
            
            # PASO 2: ELIMINAR TODAS las instancias de la factura y items
            self.logger.info(f" Eliminando TODAS las instancias de factura {factura_id}...")
            
            # Eliminar items primero
            items_eliminados = self.eliminar_items_factura(factura_id)
            self.stats['items_eliminados'] += items_eliminados
            
            # Eliminar TODAS las facturas con este ID
            facturas_eliminadas = self.eliminar_todas_facturas_por_id(factura_id)
            
            # PASO 3: CREAR factura nueva
            self.logger.info(f" Creando factura nueva...")
            factura_data = self.procesar_factura_alegra(factura_alegra)
            items_data = self.procesar_items_factura_alegra(factura_alegra)
            
            nuevo_id_factura = self.crear_factura_sharepoint(factura_data)
            
            if nuevo_id_factura:
                self.logger.info(f" Factura recreada con ID: {nuevo_id_factura}")
                
                # PASO 4: CREAR items nuevos
                items_creados = 0
                for item_data in items_data:
                    if self.crear_item_factura_sharepoint(item_data, nuevo_id_factura):
                        items_creados += 1
                
                self.stats['facturas_recreadas'] += 1
                self.stats['items_recreados'] += items_creados
                
                self.logger.info(f" Factura {factura_id} recreada completamente:")
                self.logger.info(f"    Eliminados: {facturas_eliminadas} facturas + {items_eliminados} items")
                self.logger.info(f"    Creados: 1 factura + {items_creados} items")
            else:
                self.logger.error(f" Error creando nueva factura {factura_id}")
                self.stats['facturas_error'] += 1
                
        except Exception as e:
            self.logger.error(f"Error recreando factura {factura_id}: {str(e)}")
            self.stats['facturas_error'] += 1

    def verificar_eliminacion_factura(self, factura_id):
        """Verificar que una factura fue completamente eliminada"""
        try:
            self.logger.info(f" Verificando eliminación completa de factura {factura_id}...")
            
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_facturas)
            
            if not list_id:
                return False
            
            # Buscar si aún existen instancias
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            all_facturas = []
            next_url = url
            
            while next_url:
                response = requests.get(next_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_facturas.extend(data.get('value', []))
                    next_url = data.get('@odata.nextLink')
                else:
                    break
            
            # Contar instancias restantes
            instancias_restantes = 0
            for item in all_facturas:
                fields = item.get('fields', {})
                title = fields.get('Title', '')
                if str(title) == str(factura_id):
                    instancias_restantes += 1
            
            if instancias_restantes > 0:
                self.logger.warning(f" AÚN QUEDAN {instancias_restantes} instancias de factura {factura_id}")
                return False
            else:
                self.logger.info(f" Factura {factura_id} completamente eliminada")
                return True
                
        except Exception as e:
            self.logger.error(f"Error verificando eliminación: {str(e)}")
            return False

    def eliminar_todas_facturas_por_id(self, factura_id):
        """Eliminar TODAS las instancias de una factura específica en SharePoint"""
        try:
            self.logger.info(f" Eliminando TODAS las instancias de factura {factura_id}...")
            
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_facturas)
            
            if not list_id:
                return 0
            
            # Obtener TODAS las facturas
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            all_facturas = []
            next_url = url
            
            while next_url:
                response = requests.get(next_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_facturas.extend(data.get('value', []))
                    next_url = data.get('@odata.nextLink')
                else:
                    break
            
            # Filtrar facturas con el ID específico
            facturas_a_eliminar = []
            for item in all_facturas:
                fields = item.get('fields', {})
                title = fields.get('Title', '')
                if str(title) == str(factura_id):
                    facturas_a_eliminar.append(item)
            
            self.logger.info(f" Encontradas {len(facturas_a_eliminar)} instancias de factura {factura_id} para eliminar")
            
            eliminadas = 0
            for i, item in enumerate(facturas_a_eliminar, 1):
                try:
                    sharepoint_id = item.get('id')
                    fields = item.get('fields', {})
                    cliente = fields.get('Cliente_x0020_Nombre', 'N/A')
                    total = fields.get('Total', 0)
                    estado = fields.get('Estado', 'N/A')
                    
                    self.logger.info(f"   Eliminando instancia {i}: ID {sharepoint_id} | Cliente: {cliente} | Total: ${total} | Estado: {estado}")
                    
                    if self.eliminar_item_sharepoint(sharepoint_id, self.list_name_facturas):
                        eliminadas += 1
                        self.logger.info(f"   Eliminada instancia {sharepoint_id}")
                    else:
                        self.logger.warning(f"   Error eliminando instancia {sharepoint_id}")
                        
                except Exception as e:
                    self.logger.error(f"   Error procesando instancia: {str(e)}")
                    continue
            
            self.logger.info(f" TOTAL FACTURAS ELIMINADAS: {eliminadas}")
            
            # Pausa para asegurar procesamiento
            if eliminadas > 0:
                import time
                time.sleep(2)
                self.logger.info(" Pausa para asegurar procesamiento de eliminaciones")
            
            # Verificar que la eliminación fue exitosa
            if not self.verificar_eliminacion_factura(factura_id):
                self.logger.warning(" Reintentando eliminación de instancias restantes...")
                # Reintentar con instancias que pudieron quedar
                eliminadas_adicionales = self.eliminar_todas_facturas_por_id(factura_id)
                eliminadas += eliminadas_adicionales
            
            return eliminadas
                
        except Exception as e:
            self.logger.error(f"Error eliminando facturas: {str(e)}")
            return 0

    def obtener_pago_desde_alegra(self, pago_id):
        """Obtener un pago específico desde la API de Alegra"""
        try:
            url = f"https://api.alegra.com/api/v1/payments/{pago_id}"
            
            headers = {
                "accept": "application/json",
                "authorization": f"Basic {self.encoded_credentials}"
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                self.logger.warning(f"Pago {pago_id} no encontrado en Alegra")
                return None
            else:
                self.logger.error(f"Error obteniendo pago {pago_id}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error consultando pago {pago_id} en Alegra: {str(e)}")
            return None

    def obtener_factura_desde_alegra(self, factura_id):
        """Obtener una factura específica desde la API de Alegra"""
        try:
            url = f"https://api.alegra.com/api/v1/invoices/{factura_id}"
            
            headers = {
                "accept": "application/json",
                "authorization": f"Basic {self.encoded_credentials}"
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                self.logger.warning(f"Factura {factura_id} no encontrada en Alegra")
                return None
            else:
                self.logger.error(f"Error obteniendo factura {factura_id}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error consultando factura {factura_id} en Alegra: {str(e)}")
            return None

    def obtener_factura_sharepoint(self, factura_id):
        """Obtener factura de SharePoint por su ID de Alegra"""
        try:
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_facturas)
            
            if not list_id:
                return None
            
            # Obtener TODAS las facturas sin filtro para buscar en Python
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            all_facturas = []
            next_url = url
            
            # Obtener todas las facturas con paginación
            while next_url:
                response = requests.get(next_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_facturas.extend(data.get('value', []))
                    next_url = data.get('@odata.nextLink')
                else:
                    self.logger.error(f"Error obteniendo facturas: {response.status_code}")
                    break
            
            # Buscar factura por Title (que contiene el ID de Alegra)
            facturas_encontradas = []
            for item in all_facturas:
                fields = item.get('fields', {})
                title = fields.get('Title', '')
                if str(title) == str(factura_id):
                    facturas_encontradas.append({
                        'SharePoint_ID': item.get('id'),
                        'fields': fields
                    })
            
            self.logger.info(f" Factura {factura_id}: Encontradas {len(facturas_encontradas)} instancias en SharePoint")
            
            if facturas_encontradas:
                # Si hay múltiples, devolver la primera pero log todas
                if len(facturas_encontradas) > 1:
                    self.logger.warning(f" Múltiples instancias de factura {factura_id} encontradas:")
                    for i, factura in enumerate(facturas_encontradas, 1):
                        sp_id = factura['SharePoint_ID']
                        cliente = factura['fields'].get('Cliente_x0020_Nombre', 'N/A')
                        total = factura['fields'].get('Total', 0)
                        self.logger.warning(f"  {i}. ID SharePoint: {sp_id} | Cliente: {cliente} | Total: ${total}")
                
                return facturas_encontradas[0]
            
            return None
                
        except Exception as e:
            self.logger.error(f"Error obteniendo factura {factura_id} de SharePoint: {str(e)}")
            return None

    def eliminar_items_factura(self, factura_id):
        """Eliminar todos los items de una factura específica"""
        try:
            self.logger.info(f" Eliminando items de factura {factura_id}...")
            
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_items)
            
            if not list_id:
                return 0
            
            # Obtener TODOS los items sin filtro inicial
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            all_items = []
            next_url = url
            
            # Obtener todos los items con paginación
            while next_url:
                response = requests.get(next_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_items.extend(data.get('value', []))
                    next_url = data.get('@odata.nextLink')
                else:
                    break
            
            # Filtrar en Python los items que pertenecen a esta factura
            items_factura = []
            for item in all_items:
                fields = item.get('fields', {})
                title = fields.get('Title', '')
                # Buscar por Title que contiene el número de factura o ID
                if str(title) == str(factura_id):
                    items_factura.append(item)
            
            self.logger.info(f" Encontrados {len(items_factura)} items de factura {factura_id} para eliminar")
            
            items_eliminados = 0
            for item in items_factura:
                try:
                    item_id = item.get('id')
                    fields = item.get('fields', {})
                    nombre_item = fields.get('Nombre', 'Item sin nombre')
                    
                    self.logger.info(f"   Eliminando item: {nombre_item} (ID: {item_id})")
                    
                    if self.eliminar_item_sharepoint(item_id, self.list_name_items):
                        items_eliminados += 1
                        self.logger.info(f"   Item eliminado: {nombre_item}")
                    else:
                        self.logger.warning(f"   Error eliminando item: {nombre_item}")
                        
                except Exception as e:
                    self.logger.error(f"   Error procesando item: {str(e)}")
                    continue
            
            self.logger.info(f" TOTAL ITEMS ELIMINADOS: {items_eliminados}")
            
            # Pausa para asegurar procesamiento
            if items_eliminados > 0:
                import time
                time.sleep(1)
            
            return items_eliminados
                
        except Exception as e:
            self.logger.error(f"Error eliminando items de factura: {str(e)}")
            return 0

    def eliminar_item_sharepoint(self, item_id, list_name):
        """Eliminar un item específico de SharePoint"""
        try:
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, list_name)
            
            if not list_id:
                return False
            
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}"
            headers = {
                "Authorization": f"Bearer {token}"
            }
            
            response = requests.delete(url, headers=headers)
            
            return response.status_code == 204
                
        except Exception as e:
            self.logger.error(f"Error eliminando item {item_id}: {str(e)}")
            return False

    def procesar_pago_alegra_unificado(self, payment):
        """Procesar un pago de Alegra en estructura unificada"""
        pagos_unificados = []
        
        try:
            # Registro base del pago
            pago_base = {
                'Pago_ID': payment.get('id'),
                'Fecha': payment.get('date'),
                'Numero_Pago': self.safe_get_nested(payment, 'numberTemplate', 'fullNumber', default=''),
                'Numero_Interno': payment.get('number'),
                'Monto_Total': payment.get('amount', 0),
                'Tipo_Pago': payment.get('type'),
                'Metodo_Pago': payment.get('paymentMethod'),
                'Estado_Pago': payment.get('status'),
                'Observaciones_Pago': payment.get('observations', ''),
                'Anotaciones_Pago': payment.get('anotation', ''),
                
                # Cuenta bancaria
                'Cuenta_ID': self.safe_get_nested(payment, 'bankAccount', 'id', default=''),
                'Cuenta_Nombre': self.safe_get_nested(payment, 'bankAccount', 'name', default=''),
                'Cuenta_Tipo': self.safe_get_nested(payment, 'bankAccount', 'type', default=''),
                
                # Cliente
                'Cliente_ID': self.safe_get_nested(payment, 'client', 'id', default=''),
                'Cliente_Nombre': self.safe_get_nested(payment, 'client', 'name', default=''),
                'Cliente_Telefono': self.safe_get_nested(payment, 'client', 'phone', default=''),
                'Cliente_Identificacion': self.safe_get_nested(payment, 'client', 'identification', default=''),
                
                # Centro de costo
                'Centro_Costo_ID': self.safe_get_nested(payment, 'costCenter', 'id', default=''),
                'Centro_Costo_Codigo': self.safe_get_nested(payment, 'costCenter', 'code', default=''),
                'Centro_Costo_Nombre': self.safe_get_nested(payment, 'costCenter', 'name', default=''),
                
                # Campos para facturas (vacíos por defecto)
                'Factura_ID': '',
                'Factura_Numero': '',
                'Factura_Fecha': None,
                'Factura_Monto_Pagado': 0,
                'Factura_Total': 0,
                'Factura_Saldo': 0,
                
                # Campos para categorías (vacíos por defecto)
                'Categoria_ID': '',
                'Categoria_Nombre': '',
                'Categoria_Precio': 0,
                'Categoria_Cantidad': 0,
                'Categoria_Total': 0,
                'Categoria_Observaciones': '',
                'Categoria_Comportamiento': '',
                
                # Tipo de registro
                'Tipo_Registro': 'PAGO_SIMPLE'
            }
            
            # Verificar facturas y categorías
            invoices = payment.get('invoices', [])
            categories = payment.get('categories', [])
            
            if not invoices and not categories:
                # Pago simple
                pagos_unificados.append(pago_base)
            else:
                # Si tiene facturas
                if invoices:
                    for invoice in invoices:
                        if invoice is not None:
                            pago_con_factura = pago_base.copy()
                            pago_con_factura.update({
                                'Factura_ID': invoice.get('id'),
                                'Factura_Numero': invoice.get('number'),
                                'Factura_Fecha': invoice.get('date'),
                                'Factura_Monto_Pagado': invoice.get('amount', 0),
                                'Factura_Total': invoice.get('total', 0),
                                'Factura_Saldo': invoice.get('balance', 0),
                                'Tipo_Registro': 'PAGO_CON_FACTURA'
                            })
                            pagos_unificados.append(pago_con_factura)
                
                # Si tiene categorías
                if categories:
                    for category in categories:
                        if category is not None:
                            pago_con_categoria = pago_base.copy()
                            pago_con_categoria.update({
                                'Categoria_ID': category.get('id'),
                                'Categoria_Nombre': category.get('name'),
                                'Categoria_Precio': category.get('price', 0),
                                'Categoria_Cantidad': category.get('quantity', 0),
                                'Categoria_Total': category.get('total', 0),
                                'Categoria_Observaciones': category.get('observations', ''),
                                'Categoria_Comportamiento': category.get('behavior', ''),
                                'Tipo_Registro': 'PAGO_CON_CATEGORIA'
                            })
                            pagos_unificados.append(pago_con_categoria)
            
            return pagos_unificados
            
        except Exception as e:
            self.logger.error(f"Error procesando pago unificado: {str(e)}")
            return []

    def procesar_factura_alegra(self, factura_alegra):
        """Procesar datos de factura desde Alegra"""
        return {
            'ID': factura_alegra.get('id'),
            'Fecha': factura_alegra.get('date'),
            'Fecha_Vencimiento': factura_alegra.get('dueDate'),
            'Numero_Factura': self.safe_get_nested(factura_alegra, 'numberTemplate', 'fullNumber', default=''),
            'Estado': factura_alegra.get('status'),
            'Subtotal': factura_alegra.get('subtotal', 0),
            'Descuento': factura_alegra.get('discount', 0),
            'Impuestos': factura_alegra.get('tax', 0),
            'Total': factura_alegra.get('total', 0),
            'Total_Pagado': factura_alegra.get('totalPaid', 0),
            'Saldo': factura_alegra.get('balance', 0),
            'Termino_Pago': factura_alegra.get('term', ''),
            'Forma_Pago': factura_alegra.get('paymentForm', ''),
            
            # Datos del cliente
            'Cliente_ID': self.safe_get_nested(factura_alegra, 'client', 'id', default=''),
            'Cliente_Nombre': self.safe_get_nested(factura_alegra, 'client', 'name', default=''),
            'Cliente_Identificacion': self.safe_get_nested(factura_alegra, 'client', 'identification', default=''),
            'Cliente_Email': self.safe_get_nested(factura_alegra, 'client', 'email', default=''),
            'Cliente_Telefono': self.safe_get_nested(factura_alegra, 'client', 'phonePrimary', default=''),
            'Cliente_Ciudad': self.safe_get_nested(factura_alegra, 'client', 'address', 'city', default=''),
            'Cliente_Departamento': self.safe_get_nested(factura_alegra, 'client', 'address', 'department', default=''),
            'Cliente_Direccion': self.safe_get_nested(factura_alegra, 'client', 'address', 'address', default=''),
            
            # Datos del vendedor
            'Vendedor_Nombre': self.safe_get_nested(factura_alegra, 'seller', 'name', default=''),
            'Vendedor_ID': self.safe_get_nested(factura_alegra, 'seller', 'identification', default=''),
            
            # Datos adicionales
            'Observaciones': factura_alegra.get('observations', ''),
            'Anotacion': factura_alegra.get('anotation', ''),
            'Almacen': self.safe_get_nested(factura_alegra, 'warehouse', 'name', default=''),
            'Centro_Costo': self.safe_get_nested(factura_alegra, 'costCenter', 'name', default=''),
            
            # CUFE
            'CUFE': self.safe_get_nested(factura_alegra, 'stamp', 'cufe', default=''),
            'Estado_DIAN': self.safe_get_nested(factura_alegra, 'stamp', 'legalStatus', default=''),
            
            # Número de items
            'Cantidad_Items': len(factura_alegra.get('items', [])) if factura_alegra.get('items') else 0,
        }

    def procesar_items_factura_alegra(self, factura_alegra):
        """Procesar items de factura desde Alegra"""
        items_data = []
        
        factura_id = factura_alegra.get('id')
        factura_numero = self.safe_get_nested(factura_alegra, 'numberTemplate', 'fullNumber', default='')
        
        items = factura_alegra.get('items', [])
        for item in items:
            if item is not None:
                item_data = {
                    'Factura_ID': factura_id,
                    'Numero_Factura': factura_numero,
                    'Item_Nombre': item.get('name', ''),
                    'Item_Descripcion': item.get('description', ''),
                    'Item_Precio': item.get('price', 0),
                    'Item_Cantidad': item.get('quantity', 0),
                    'Item_Descuento': item.get('discount', 0),
                    'Item_Total': item.get('total', 0),
                    'Item_Referencia': item.get('reference', ''),
                    'Item_Unidad': item.get('unit', ''),
                }
                items_data.append(item_data)
        
        return items_data

    def crear_pago_sharepoint(self, pago_data):
        """Crear un nuevo registro de pago en SharePoint"""
        try:
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_pagos)
            
            if not list_id:
                return False
            
            item_data = {
                'fields': {
                    "Title": str(pago_data.get("Pago_ID", "")),
                    "Fecha": pago_data.get("Fecha", ""),
                    "Numero_x0020_Pago": pago_data.get("Numero_Pago", ""),
                    "Numero_x0020_Interno": pago_data.get("Numero_Interno", ""),
                    "Monto_x0020_Total": pago_data.get("Monto_Total", 0),
                    "Tipo_x0020_Pago": pago_data.get("Tipo_Pago", ""),
                    "Metodo_x0020_Pago": pago_data.get("Metodo_Pago", ""),
                    "Estado_x0020_Pago": pago_data.get("Estado_Pago", ""),
                    "Observaciones": pago_data.get("Observaciones_Pago", "") or "",
                    "Cuenta_x0020_Nombre": pago_data.get("Cuenta_Nombre", ""),
                    "ID_x0020_Cuenta": pago_data.get("Cuenta_ID", ""),
                    "Cuenta_x0020_Tipo": pago_data.get("Cuenta_Tipo", ""),
                    "ID_x0020_Cliente": pago_data.get("Cliente_ID", ""),
                    "Nombre_x0020_Cliente": pago_data.get("Cliente_Nombre", ""),
                    "Identificacion_x0020_Cliente": pago_data.get("Cliente_Identificacion", ""),
                    "ID_x0020_Factura": pago_data.get("Factura_ID", ""),
                    "Numero_x0020_Factura": pago_data.get("Factura_Numero", ""),
                    "Factura_x0020_Monto_x0020_Pagado": pago_data.get("Factura_Monto_Pagado", 0),
                    "Total_x0020_Factura": pago_data.get("Factura_Total", 0),
                    "Saldo_x0020_Factura": pago_data.get("Factura_Saldo", 0),
                    "Nombre_x0020_Categoria": pago_data.get("Categoria_Nombre", ""),
                    "Precio_x0020_Categoria": pago_data.get("Categoria_Precio", 0),
                    "Cantidad_x0020_Categoria": pago_data.get("Categoria_Cantidad", 0),
                    "Total_x0020_Categoria": pago_data.get("Categoria_Total", 0),
                    "Observaciones_x0020_Categoria": pago_data.get("Categoria_Observaciones", ""),
                }
            }
            
            # Solo agregar fecha de factura si tiene valor válido
            fecha_factura = pago_data.get("Factura_Fecha")
            if fecha_factura and str(fecha_factura).strip():
                item_data['fields']["Fecha_x0020_Factura"] = fecha_factura
            
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, headers=headers, json=item_data)
            
            if response.status_code == 201:
                return True
            else:
                self.logger.error(f"Error creando pago: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error creando pago en SharePoint: {str(e)}")
            return False

    def crear_factura_sharepoint(self, factura_data):
        """Crear una nueva factura en SharePoint"""
        try:
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_facturas)
            
            if not list_id:
                return None
            
            item_data = {
                'fields': {
                    "Title": str(factura_data.get("ID", "")),
                    "Fecha": factura_data.get("Fecha", ""),
                    "Fecha_x0020_Vencimiento": factura_data.get("Fecha_Vencimiento", ""),
                    "Numero_x0020_Factura": factura_data.get("Numero_Factura", ""),
                    "Subtotal": factura_data.get("Subtotal", 0),
                    "Descuento": factura_data.get("Descuento", 0),
                    "Impuestos": factura_data.get("Impuestos", 0),
                    "Total": factura_data.get("Total", 0),
                    "Total_x0020_Pagado": factura_data.get("Total_Pagado", 0),
                    "Saldo": factura_data.get("Saldo", 0),
                    "Cliente_x0020_Nombre": factura_data.get("Cliente_Nombre", ""),
                    "Estado": factura_data.get("Estado", ""),
                }
            }
            
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, headers=headers, json=item_data)
            
            if response.status_code == 201:
                created_item = response.json()
                
                # Obtener ID del item creado
                item_id = None
                if 'id' in created_item:
                    item_id = created_item['id']
                elif 'fields' in created_item and 'id' in created_item['fields']:
                    item_id = created_item['fields']['id']
                elif 'fields' in created_item and 'ID' in created_item['fields']:
                    item_id = created_item['fields']['ID']
                
                return item_id
            else:
                self.logger.error(f"Error creando factura: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creando factura en SharePoint: {str(e)}")
            return None

    def crear_item_factura_sharepoint(self, item_data, factura_lookup_id):
        """Crear un item de factura en SharePoint"""
        try:
            token = self.sp_connector.get_azure_token()
            site_id = self.sp_connector.get_site_id(token, self.site_url)
            list_id = self.sp_connector.get_list_id(token, site_id, self.list_name_items)
            
            if not list_id:
                return False
            
            # Intentar con diferentes variaciones de campo lookup
            lookup_variations = [
                "Factura_x0020_de_x0020_VentaLookupId",
                "Factura_x0020_de_x0020_Venta",
                "FacturadeVentaLookupId", 
                "FacturadeVenta"
            ]
            
            for lookup_field in lookup_variations:
                item_fields = {
                    'fields': {
                        lookup_field: str(factura_lookup_id),
                        "Title": item_data.get("Numero_Factura", ""),
                        "Nombre": item_data.get("Item_Nombre", ""),
                        "Precio": item_data.get("Item_Precio", 0),
                        "Cantidad": item_data.get("Item_Cantidad", 0),
                        "Descuento": item_data.get("Item_Descuento", 0),
                        "Total": item_data.get("Item_Total", 0),
                    }
                }
                
                url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                
                response = requests.post(url, headers=headers, json=item_fields)
                
                if response.status_code == 201:
                    return True
            
            self.logger.warning(f"No se pudo crear item con ningún campo lookup")
            return False
                
        except Exception as e:
            self.logger.error(f"Error creando item en SharePoint: {str(e)}")
            return False

    def safe_get_nested(self, obj, *keys, default=''):
        """Función helper para obtener valores anidados de forma segura"""
        for key in keys:
            if isinstance(obj, dict) and key in obj and obj[key] is not None:
                obj = obj[key]
            else:
                return default
        return obj if obj is not None else default

    def mostrar_resumen_final(self):
        """Mostrar resumen final de la sincronización"""
        self.logger.info("="*60)
        self.logger.info("RESUMEN FINAL - ESTRATEGIA DELETE + CREATE")
        self.logger.info("="*60)
        self.logger.info(f"PAGOS:")
        self.logger.info(f"   Revisados: {self.stats['pagos_revisados']}")
        self.logger.info(f"   Recreados: {self.stats['pagos_recreados']}")
        self.logger.info(f"   Sin cambios: {self.stats['pagos_sin_cambios']}")
        self.logger.info(f"   Con errores: {self.stats['pagos_error']}")
        
        self.logger.info(f"FACTURAS:")
        self.logger.info(f"   Recreadas: {self.stats['facturas_recreadas']}")
        self.logger.info(f"   Con errores: {self.stats['facturas_error']}")
        
        self.logger.info(f"ITEMS:")
        self.logger.info(f"   Recreados: {self.stats['items_recreados']}")
        self.logger.info(f"   Eliminados: {self.stats['items_eliminados']}")
        
        self.logger.info(f"TOTALES:")
        self.logger.info(f"   Registros eliminados: {self.stats['registros_eliminados']}")
        
        # Calcular eficiencia
        total_operaciones = (self.stats['pagos_recreados'] + 
                           self.stats['facturas_recreadas'] + 
                           self.stats['pagos_error'] + 
                           self.stats['facturas_error'])
        
        if total_operaciones > 0:
            eficiencia = ((self.stats['pagos_recreados'] + self.stats['facturas_recreadas']) / total_operaciones) * 100
            self.logger.info(f"   Eficiencia: {eficiencia:.1f}%")
        
        # También mostrar en consola
        print(f" Sincronización DELETE+CREATE completada:")
        print(f"   Pagos recreados: {self.stats['pagos_recreados']}")
        print(f"   Facturas recreadas: {self.stats['facturas_recreadas']}")
        print(f"   Items recreados: {self.stats['items_recreados']}")
        print(f"   Registros eliminados: {self.stats['registros_eliminados']}")

def main():
    """Función principal para ejecutar el sincronizador"""
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Iniciando sincronización Alegra-SharePoint...")
        
        sincronizador = SincronizadorAlegra()
        success = sincronizador.main()
        
        if success:
            print("Sincronizacion completada exitosamente")
            logger.info(f"Log detallado: {log_file}")
        else:
            print("Sincronizacion completada con errores")
            logger.info(f"Revisar log: {log_file}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error critico en sincronizacion: {str(e)}")
        print(f"ERROR: {str(e)}. Ver log: {log_file}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)