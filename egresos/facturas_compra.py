import requests
import base64
import os
import sys
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sharepoint_connector import SharePointConnector

def setup_logging():
    """Configurar el sistema de logging"""
    
    # Crear carpeta de logs si no existe
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Nombre del archivo de log con timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d')
    log_filename = f"logs/facturas_compra_{timestamp}.log"
    
    # Configurar el logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # Filtro para mostrar solo errores críticos en consola, pero DEBUG en archivo
    console_handler = logging.getLogger().handlers[1]
    console_handler.setLevel(logging.ERROR)
    
    return log_filename

def obtener_ultimo_id_sharepoint(sp_connector, site_url, list_name, logger):
    """
    Obtener el ID más reciente (mayor) de facturas de compra en SharePoint
    
    Returns:
        int: El ID más alto encontrado, o 0 si no hay facturas
    """
    try:
        logger.info("Buscando el ID más reciente en SharePoint...")
        
        # Obtener token y site_id
        token = sp_connector.get_azure_token()
        if not token:
            logger.error("No se pudo obtener el token de acceso")
            return 0
        
        site_id = sp_connector.get_site_id(token, site_url)
        if not site_id:
            logger.error("No se pudo obtener el site_id")
            return 0
        
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        if not list_id:
            logger.error(f"No se pudo obtener el ID de la lista {list_name}")
            return 0
        
        # Obtener todos los items (solo el campo Title)
        # No podemos usar orderby porque Title no está indexado
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        url += "?$select=fields&$expand=fields($select=Title)&$top=5000"
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        all_items = []
        next_link = url
        
        # Obtener todos los items con paginación
        while next_link:
            response = requests.get(next_link, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('value', [])
                all_items.extend(items)
                
                # Verificar si hay más páginas
                next_link = data.get('@odata.nextLink', None)
                
                logger.debug(f"Obtenidos {len(items)} items, total acumulado: {len(all_items)}")
            else:
                logger.error(f"Error al consultar SharePoint: {response.status_code} - {response.text}")
                break
        
        if not all_items:
            logger.info("No se encontraron facturas en SharePoint, comenzando desde ID 0")
            return 0
        
        # Encontrar el ID máximo en memoria
        max_id = 0
        for item in all_items:
            fields = item.get('fields', {})
            title = fields.get('Title', '0')
            
            try:
                id_int = int(title) if title else 0
                if id_int > max_id:
                    max_id = id_int
            except (ValueError, TypeError):
                continue
        
        logger.info(f"✓ Último ID encontrado en SharePoint: {max_id} (de {len(all_items)} facturas)")
        return max_id
            
    except Exception as e:
        logger.error(f"Error obteniendo último ID de SharePoint: {str(e)}")
        return 0

def obtener_facturas_desde_id(encoded_credentials, id_inicial, logger):
    """
    Obtener todas las facturas de compra desde Alegra con ID mayor al especificado
    Usa el ordenamiento por ID de la API de Alegra directamente
    
    Args:
        encoded_credentials: Credenciales codificadas en base64
        id_inicial: ID desde el cual comenzar a buscar (se traen IDs mayores a este)
        logger: Logger para registrar eventos
        
    Returns:
        list: Lista de facturas con ID mayor al inicial
    """
    try:
        logger.info(f"Consultando facturas de compra con ID > {id_inicial}...")
        
        # Usar la API de Alegra con ordenamiento por ID descendente
        url = "https://api.alegra.com/api/v1/bills?metadata=false&order_direction=DESC&order_field=id&type=bill"
        
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {encoded_credentials}"
        }
        
        logger.info("Obteniendo facturas ordenadas por ID descendente desde Alegra...")
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Error consultando API Alegra: {response.status_code} - {response.text}")
            return []
        
        todas_facturas = response.json()
        logger.info(f"Total de facturas obtenidas de Alegra: {len(todas_facturas)}")
        
        # Filtrar facturas con ID mayor al inicial
        # Como vienen ordenadas DESC, en cuanto encontremos una <= id_inicial, podemos parar
        facturas_nuevas = []
        for factura in todas_facturas:
            factura_id = factura.get('id')
            if factura_id:
                try:
                    factura_id_int = int(factura_id) if isinstance(factura_id, str) else factura_id
                    
                    # Si encontramos un ID menor o igual, ya no hay más facturas nuevas
                    if factura_id_int <= id_inicial:
                        break
                    
                    facturas_nuevas.append(factura)
                    
                except (ValueError, TypeError):
                    logger.warning(f"No se pudo convertir ID de factura: {factura_id}")
                    continue
        
        logger.info(f"Facturas nuevas encontradas (ID > {id_inicial}): {len(facturas_nuevas)}")
        
        return facturas_nuevas
        
    except Exception as e:
        logger.error(f"Error obteniendo facturas desde ID: {str(e)}")
        return []

def main():
    # Configurar logging
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("INICIO DEL PROCESO DE FACTURAS DE COMPRA ALEGRA (POR ID)")
    logger.info("="*60)
    
    try:
        # Cargar variables de entorno
        load_dotenv()
        logger.info("Variables de entorno cargadas")
        
        # Credenciales Alegra
        username = os.getenv("email")
        password = os.getenv("password")
        
        # Configuración SharePoint
        site_url = os.getenv("site_url")
        list_name_facturas_compra = os.getenv("list_facturas_compra", "Facturas de Compra")
        list_name_categorias_compra = os.getenv("list_categorias_compra", "Categorias Facturas Compra")
        list_name_retenciones_compra = os.getenv("list_retenciones_compra", "Retenciones Facturas de Compra")
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista Facturas: {list_name_facturas_compra}")
        logger.info(f"Lista Categorías: {list_name_categorias_compra}")
        logger.info(f"Lista Retenciones: {list_name_retenciones_compra}")
        
        # Verificar credenciales
        if not username or not password:
            logger.error("Credenciales de Alegra no encontradas en variables de entorno")
            return False
            
        if not site_url:
            logger.error("URL de SharePoint no encontrada en variables de entorno")
            return False
        
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Inicializar conector de SharePoint
        logger.info("Inicializando conector de SharePoint...")
        sp_connector = SharePointConnector()
        
        # NUEVO: Obtener el último ID de SharePoint
        ultimo_id = obtener_ultimo_id_sharepoint(sp_connector, site_url, list_name_facturas_compra, logger)
        logger.info(f"Buscando facturas con ID > {ultimo_id}")
        
        # MODIFICADO: Obtener facturas desde Alegra con ID mayor al último encontrado
        logger.info("Iniciando consulta a API de Alegra...")
        data = obtener_facturas_desde_id(encoded_credentials, ultimo_id, logger)
        
        if len(data) == 0:
            logger.info("No se encontraron facturas nuevas - PROCESO EXITOSO")
            print(f"✓ No hay facturas nuevas (último ID en SharePoint: {ultimo_id})")
            return True
        
        logger.info(f"Obtenidas {len(data)} facturas de compra nuevas de Alegra")
        
        # Procesar facturas de compra (IGUAL QUE ANTES)
        facturas_list = []
        categorias_list = []
        retenciones_list = []
        facturas_procesadas = 0
        facturas_con_error = 0
        
        for i, bill in enumerate(data):
            try:
                if bill is None:
                    logger.warning(f"Factura de compra {i+1} es None, saltando...")
                    facturas_con_error += 1
                    continue
                
                # Datos básicos de la factura de compra
                factura_data = {
                    'ID_Factura': bill.get('id'),
                    'Fecha': bill.get('date'),
                    'Fecha_Vencimiento': bill.get('dueDate'),
                    'Numero_Factura': safe_get_nested(bill, 'numberTemplate', 'fullNumber', default=''),
                    'Estado': bill.get('status'),
                    'Total': bill.get('total', 0),
                    'Total_Pagado': bill.get('totalPaid', 0),
                    'Saldo': bill.get('balance', 0),
                    'Tipo_Factura': bill.get('type', ''),
                    'Observaciones': bill.get('observations', ''),
                    
                    # Datos del proveedor
                    'ID_Proveedor': safe_get_nested(bill, 'provider', 'id', default=''),
                    'Nombre_Proveedor': safe_get_nested(bill, 'provider', 'name', default=''),
                    'Identificacion_Proveedor': safe_get_nested(bill, 'provider', 'identification', default=''),
                    'Email_Proveedor': safe_get_nested(bill, 'provider', 'email', default=''),
                    'Telefono_Proveedor': safe_get_nested(bill, 'provider', 'phonePrimary', default=''),
                    
                    # Datos del almacén
                    'Nombre_Almacen': safe_get_nested(bill, 'warehouse', 'name', default=''),
                    
                    # Centro de costo
                    'Centro_de_Costo': safe_get_nested(bill, 'costCenter', 'name', default=''),
                    'Codigo_Unico': safe_get_nested(bill, 'costCenter', 'code', default=''),
                    
                    # Contadores
                    'Cantidad_Retenciones': len(bill.get('retentions', [])) if bill.get('retentions') else 0,
                    'Cantidad_Categorias': len(safe_get_nested(bill, 'purchases', 'categories', default=[])),
                }
                
                facturas_list.append(factura_data)
                
                # Procesar categorías de la factura de compra
                bill_id = bill.get('id')
                bill_number = safe_get_nested(bill, 'numberTemplate', 'fullNumber', default='')
                
                categories = safe_get_nested(bill, 'purchases', 'categories', default=[])
                if categories:
                    for categoria in categories:
                        if categoria is not None:
                            # Procesar impuestos de la categoría
                            impuestos_info = procesar_impuestos_categoria(categoria.get('tax', []))
                            
                            categoria_data = {
                                'Numero_Factura': bill_number,
                                'Categoria_ID': categoria.get('id', ''),
                                'Categoria_Nombre': categoria.get('name', ''),
                                'Precio_Unitario': categoria.get('price', 0),
                                'Cantidad': categoria.get('quantity', 0),
                                'Descuento': categoria.get('discount', 0),
                                'Observaciones': categoria.get('observations', ''),
                                'Subtotal': categoria.get('subtotal', 0),
                                'Total_Categoria': categoria.get('total', 0),
                                'Impuestos': impuestos_info['total_impuestos'],
                                'Detalle_Impuestos': impuestos_info['detalle_impuestos'],
                                'IVA_Porcentaje': impuestos_info['iva_porcentaje'],
                                'IVA_Monto': impuestos_info['iva_monto'],
                            }
                            categorias_list.append(categoria_data)
                
                # Procesar retenciones de la factura
                retentions = bill.get('retentions', [])
                if retentions:
                    for retencion in retentions:
                        if retencion is not None:
                            retencion_data = {
                                'ID_Retencion': retencion.get('id', ''),
                                'Nombre': retencion.get('name', ''),
                                'Porcentaje': retencion.get('percentage', 0),
                                'Monto': retencion.get('amount', 0),
                                'Factura_de_Compra': bill_number,
                                'Retencion_Tipo': retencion.get('type', ''),
                                'Calculado_Por': retencion.get('calculatedBy', ''),
                                'Es_Asumida': retencion.get('isAssumed', False),
                                # Nuevos campos agregados a la lista
                                'Tipo_de_Cambio': str(retencion.get('exchangeRate', '')),
                                'Asumido_Por': 'Empresa' if retencion.get('isAssumed', False) else 'Proveedor',
                            }
                            retenciones_list.append(retencion_data)
                
                facturas_procesadas += 1
                
            except Exception as e:
                logger.error(f"Error procesando factura de compra {i+1}: {str(e)}")
                facturas_con_error += 1
                continue
        
        logger.info(f"Procesamiento completado: {facturas_procesadas} exitosas, {facturas_con_error} con errores")
        
        # Crear DataFrames
        df_facturas = pd.DataFrame(facturas_list)
        df_categorias = pd.DataFrame(categorias_list)
        df_retenciones = pd.DataFrame(retenciones_list)
        
        logger.info(f"DataFrames creados - Facturas: {len(df_facturas)}, Categorías: {len(df_categorias)}, Retenciones: {len(df_retenciones)}")
        
        # Subir a listas de SharePoint
        logger.info("INICIANDO SUBIDA A LISTAS DE SHAREPOINT")
        success_listas = subir_facturas_compra_sharepoint(
            df_facturas, df_categorias, df_retenciones, 
            site_url, list_name_facturas_compra, list_name_categorias_compra, list_name_retenciones_compra, 
            logger
        )
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO")
        logger.info("="*60)
        logger.info(f"Facturas de compra procesadas: {len(df_facturas)}")
        logger.info(f"Categorías procesadas: {len(df_categorias)}")
        logger.info(f"Retenciones procesadas: {len(df_retenciones)}")
        logger.info(f"Datos subidos a listas: {'SI' if success_listas else 'NO'}")
        logger.info(f"Archivo de log: {log_file}")
        
        # Solo mostrar en consola el resumen final
        print(f"Proceso completado. Facturas: {len(df_facturas)}, Categorías: {len(df_categorias)}, Retenciones: {len(df_retenciones)}")
        print(f"Log guardado en: {log_file}")
        
        return success_listas
        
    except Exception as e:
        logger.error(f"Error crítico en el proceso principal: {str(e)}")
        logger.error("Detalles del error:", exc_info=True)
        print(f"ERROR: {str(e)}. Ver detalles en: {log_file}")
        return False

def safe_get_nested(obj, *keys, default=''):
    """Función helper para obtener valores anidados de forma segura"""
    for key in keys:
        if isinstance(obj, dict) and key in obj and obj[key] is not None:
            obj = obj[key]
        else:
            return default
    return obj if obj is not None else default

def procesar_impuestos_categoria(impuestos):
    """Procesar información de impuestos de una categoría"""
    total_impuestos = 0
    detalles = []
    iva_porcentaje = 0
    iva_monto = 0
    
    if impuestos:
        for impuesto in impuestos:
            if impuesto is not None:
                nombre = impuesto.get('name', '')
                porcentaje = float(impuesto.get('percentage', 0))
                monto = impuesto.get('amount', 0)
                tipo = impuesto.get('type', '')
                
                total_impuestos += monto
                detalles.append(f"{nombre}: {porcentaje}% = ${monto}")
                
                # Capturar específicamente IVA
                if tipo.upper() == 'IVA':
                    iva_porcentaje = porcentaje
                    iva_monto = monto
    
    return {
        'total_impuestos': total_impuestos,
        'detalle_impuestos': ' | '.join(detalles),
        'iva_porcentaje': iva_porcentaje,
        'iva_monto': iva_monto
    }

def subir_facturas_compra_sharepoint(df_facturas, df_categorias, df_retenciones, site_url, 
                                   list_name_facturas, list_name_categorias, list_name_retenciones, logger):
    """Subir facturas de compra y datos relacionados a listas de SharePoint"""
    try:
        logger.info("Iniciando subida a listas de SharePoint...")
        
        sp_connector = SharePointConnector()
        
        facturas_success = 0
        facturas_error = 0
        categorias_success = 0
        categorias_error = 0
        retenciones_success = 0
        retenciones_error = 0
        
        # 1. Subir facturas de compra y procesar sus elementos relacionados
        logger.info("Subiendo facturas de compra...")
        for index, factura_row in df_facturas.iterrows():
            try:
                numero_factura = factura_row['Numero_Factura']
                factura_id_alegra = factura_row['ID_Factura']
                logger.info(f"Procesando factura de compra {index + 1}/{len(df_facturas)}: {numero_factura}")
                
                datos_factura = factura_row.to_dict()
                factura_sharepoint_id = send_factura_compra_sharepoint(sp_connector, datos_factura, site_url, list_name_facturas, logger)
                
                if factura_sharepoint_id:
                    facturas_success += 1
                    logger.info(f"Factura de compra {numero_factura} subida con ID: {factura_sharepoint_id}")
                    
                    # 2. Procesar categorías de esta factura
                    factura_categorias = df_categorias[df_categorias['Numero_Factura'] == numero_factura]
                    if not factura_categorias.empty:
                        logger.info(f"Procesando {len(factura_categorias)} categorías de la factura {numero_factura}")
                        
                        for _, categoria_row in factura_categorias.iterrows():
                            categoria_dict = categoria_row.to_dict()
                            categoria_id = send_categoria_compra_sharepoint(
                                sp_connector, categoria_dict, factura_sharepoint_id, site_url, list_name_categorias, logger
                            )
                            if categoria_id:
                                categorias_success += 1
                            else:
                                categorias_error += 1
                    else:
                        logger.info(f"No hay categorías para la factura {numero_factura}")
                    
                    # 3. Procesar retenciones de esta factura
                    if not df_retenciones.empty:
                        factura_retenciones = df_retenciones[df_retenciones['Factura_de_Compra'] == numero_factura]
                    else:
                        factura_retenciones = df_retenciones  # DataFrame vacío
                    
                    if not factura_retenciones.empty:
                        logger.info(f"Procesando {len(factura_retenciones)} retenciones de la factura {numero_factura}")
                        
                        for _, retencion_row in factura_retenciones.iterrows():
                            retencion_dict = retencion_row.to_dict()
                            retencion_id = send_retencion_compra_sharepoint(
                                sp_connector, retencion_dict, factura_sharepoint_id, site_url, list_name_retenciones, logger
                            )
                            if retencion_id:
                                retenciones_success += 1
                            else:
                                retenciones_error += 1
                    else:
                        logger.info(f"No hay retenciones para la factura {numero_factura}")
                        
                else:
                    facturas_error += 1
                    logger.error(f"Error subiendo factura de compra {numero_factura}")
                    
            except Exception as e:
                facturas_error += 1
                logger.error(f"Error procesando factura de compra {index + 1}: {str(e)}")
                continue
        
        logger.info("RESUMEN DE SUBIDA A LISTAS:")
        logger.info(f"Facturas exitosas: {facturas_success}, errores: {facturas_error}")
        logger.info(f"Categorías exitosas: {categorias_success}, errores: {categorias_error}")
        logger.info(f"Retenciones exitosas: {retenciones_success}, errores: {retenciones_error}")
        
        return facturas_success > 0
        
    except Exception as e:
        logger.error(f"Error en subida a listas: {str(e)}")
        return False

def send_factura_compra_sharepoint(sp_connector, datos_factura, site_url, list_name, logger):
    """Subir datos de factura de compra a lista de SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener el ID de la lista {list_name}")
            return None
        
        item_data = {
            'fields': {
                "Title": str(datos_factura.get("ID_Factura", "")),
                "Fecha": datos_factura.get("Fecha", ""),
                "Fecha_x0020_Vencimiento": datos_factura.get("Fecha_Vencimiento", ""),
                "Numero_x0020_Factura": datos_factura.get("Numero_Factura", ""),
                "Estado": datos_factura.get("Estado", ""),
                "Total": datos_factura.get("Total", 0),
                "Total_x0020_Pagado": datos_factura.get("Total_Pagado", 0),
                "Saldo": datos_factura.get("Saldo", 0),
                "Tipo_x0020_Factura": datos_factura.get("Tipo_Factura", ""),
                "Observaciones": datos_factura.get("Observaciones", ""),
                "ID_x0020_Proveedor": datos_factura.get("ID_Proveedor", ""),
                "Nombre_x0020_Proveedor": datos_factura.get("Nombre_Proveedor", ""),
                "Identificacion_x0020_Proveedor": datos_factura.get("Identificacion_Proveedor", ""),
                "Nombre_x0020_Almacen": datos_factura.get("Nombre_Almacen", ""),
                "Centro_x0020_de_x0020_Costo": datos_factura.get("Centro_de_Costo", ""),
                "Codigo_x0020_Unico": datos_factura.get("Codigo_Unico", ""),
                "Cantidad_x0020_Retenciones": datos_factura.get("Cantidad_Retenciones", 0),
                "Cantidad_x0020_Categorias": datos_factura.get("Cantidad_Categorias", 0),
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
            
            item_id = None
            if 'id' in created_item:
                item_id = created_item['id']
            elif 'fields' in created_item and 'id' in created_item['fields']:
                item_id = created_item['fields']['id']
            elif 'fields' in created_item and 'ID' in created_item['fields']:
                item_id = created_item['fields']['ID']
            
            return item_id
        else:
            logger.error(f"Error HTTP subiendo factura de compra: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        logger.error(f"Error subiendo factura de compra: {str(e)}")
        return None

def send_categoria_compra_sharepoint(sp_connector, datos_categoria, factura_lookup_id, site_url, list_name, logger):
    """Subir categoría de factura de compra a lista de SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            return None
        
        # Intentar con diferentes variaciones de campo lookup
        lookup_variations = [
            "Factura_x0020_de_x0020_CompraLookupId",
            "Factura_x0020_de_x0020_Compra",
            "FacturadeCompraLookupId", 
            "FacturadeCompra"
        ]
        
        for lookup_field in lookup_variations:
            item_data = {
                'fields': {
                    lookup_field: str(factura_lookup_id),
                    "Title": datos_categoria.get("Numero_Factura", ""),
                    "Categoria_x0020_ID": datos_categoria.get("Categoria_ID", ""),
                    "Categoria_x0020_Nombre": datos_categoria.get("Categoria_Nombre", ""),
                    "Precio_x0020_Unitario": datos_categoria.get("Precio_Unitario", 0),
                    "Cantidad": datos_categoria.get("Cantidad", 0),
                    "Descuento": datos_categoria.get("Descuento", 0),
                    "Observaciones": datos_categoria.get("Observaciones", ""),
                    "Subtotal": datos_categoria.get("Subtotal", 0),
                    "Total_x0020_Categoria": datos_categoria.get("Total_Categoria", 0),
                    "Impuestos": datos_categoria.get("Impuestos", 0),
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
                return created_item.get('id')
        
        logger.warning(f"No se pudo subir categoría con ningún campo lookup")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo categoría: {str(e)}")
        return None

def send_retencion_compra_sharepoint(sp_connector, datos_retencion, factura_lookup_id, site_url, list_name, logger):
    """Subir retención de factura de compra a lista de SharePoint"""
    try:
        logger.debug(f"Subiendo retención: {datos_retencion.get('ID_Retencion', 'N/A')}")
        
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener ID de la lista {list_name}")
            return None
        
        # Usar el campo lookup correcto identificado en las pruebas
        lookup_field = "Factura_x0020_de_x0020_CompraLookupId"
        
        # Preparar todos los campos según la nueva lista completa
        item_data = {
            'fields': {
                "Title": str(datos_retencion.get("ID_Retencion", "")),
                lookup_field: str(factura_lookup_id),
                "Nombre": str(datos_retencion.get("Nombre", "")),
                "Monto": float(datos_retencion.get("Monto", 0)),
                "Retencion_x0020_Tipo": str(datos_retencion.get("Retencion_Tipo", "")),
                "Calculado_x0020_Por": str(datos_retencion.get("Calculado_Por", "")),
                "Tipo_x0020_de_x0020_Cambio": str(datos_retencion.get("Tipo_de_Cambio", "")),
                "Asumido_x0020_Por": str(datos_retencion.get("Asumido_Por", "")),
            }
        }
        
        # Manejar porcentaje que puede venir como string con decimales
        try:
            porcentaje_str = str(datos_retencion.get("Porcentaje", "0"))
            # Remover cualquier carácter no numérico excepto punto decimal
            porcentaje_clean = ''.join(c for c in porcentaje_str if c.isdigit() or c == '.')
            porcentaje_float = float(porcentaje_clean) if porcentaje_clean else 0.0
            item_data['fields']["Porcentaje"] = porcentaje_float
        except (ValueError, TypeError) as e:
            logger.warning(f"Error procesando porcentaje '{datos_retencion.get('Porcentaje')}': {e}, usando 0")
            item_data['fields']["Porcentaje"] = 0.0
        
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        logger.debug(f"Enviando datos: {item_data}")
        response = requests.post(url, headers=headers, json=item_data)
        
        if response.status_code == 201:
            created_item = response.json()
            logger.info(f"Retención {datos_retencion.get('ID_Retencion', 'N/A')} creada exitosamente")
            return created_item.get('id')
        else:
            logger.error(f"Error HTTP subiendo retención: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        logger.error(f"Error subiendo retención: {str(e)}")
        return None

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)