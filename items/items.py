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
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/items_alegra_{timestamp}.log"
    
    # Configurar el logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # Filtro para mostrar solo errores críticos en consola
    console_handler = logging.getLogger().handlers[1]
    console_handler.setLevel(logging.ERROR)
    
    return log_filename

def main():
    # Configurar logging
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("INICIO DEL PROCESO DE ITEMS ALEGRA A SHAREPOINT")
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
        list_name_items = os.getenv("list_items_products", "Items")  # Nombre de tu lista
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista destino: {list_name_items}")
        
        # Verificar credenciales
        if not username or not password:
            logger.error("Credenciales de Alegra no encontradas en variables de entorno")
            return False
            
        if not site_url:
            logger.error("URL de SharePoint no encontrada en variables de entorno")
            return False
        
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Obtener datos de Alegra
        logger.info("Iniciando consulta a API de Alegra para items...")
        items_data = obtener_todos_los_items_alegra(encoded_credentials, logger)
        
        if not items_data:
            logger.error("No se pudieron obtener items de Alegra")
            return False
            
        logger.info(f"Obtenidos {len(items_data)} items de Alegra")
        
        # Procesar items
        items_procesados = []
        items_con_error = 0
        
        for i, item in enumerate(items_data):
            try:
                if item is None:
                    logger.warning(f"Item {i+1} es None, saltando...")
                    items_con_error += 1
                    continue
                
                # Procesar item
                item_procesado = procesar_item_alegra(item, logger)
                if item_procesado:
                    items_procesados.append(item_procesado)
                else:
                    items_con_error += 1
                    
            except Exception as e:
                logger.error(f"Error procesando item {i+1}: {str(e)}")
                items_con_error += 1
                continue
        
        logger.info(f"Procesamiento completado: {len(items_procesados)} exitosos, {items_con_error} con errores")
        
        if not items_procesados:
            logger.warning("No hay items para subir a SharePoint")
            return True
        
        # Subir a SharePoint
        logger.info("INICIANDO SUBIDA A SHAREPOINT")
        success = subir_items_sharepoint(items_procesados, site_url, list_name_items, logger)
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO")
        logger.info("="*60)
        logger.info(f"Items procesados desde Alegra: {len(items_procesados)}")
        logger.info(f"Items con errores: {items_con_error}")
        logger.info(f"Datos subidos a SharePoint: {'SI' if success else 'NO'}")
        logger.info(f"Archivo de log: {log_file}")
        
        # Solo mostrar en consola el resumen final
        print(f"Proceso completado. Items: {len(items_procesados)}")
        print(f"Log guardado en: {log_file}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error crítico en el proceso principal: {str(e)}")
        logger.error("Detalles del error:", exc_info=True)
        print(f"ERROR: {str(e)}. Ver detalles en: {log_file}")
        return False

def obtener_todos_los_items_alegra(encoded_credentials, logger):
    """Obtener todos los items de la API de Alegra usando paginación"""
    base_url = "https://api.alegra.com/api/v1/items"
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {encoded_credentials}"
    }
    
    todos_los_items = []
    start = 0
    limit = 30
    
    while True:
        params = {'start': start, 'limit': limit}
        
        try:
            logger.info(f"Obteniendo items {start + 1} - {start + limit}...")
            response = requests.get(base_url, headers=headers, params=params)
            
            if response.status_code == 200:
                items_pagina = response.json()
                
                if not items_pagina or len(items_pagina) == 0:
                    logger.info("No hay más items para obtener")
                    break
                
                todos_los_items.extend(items_pagina)
                logger.info(f"Obtenidos {len(items_pagina)} items en esta página")
                
                if len(items_pagina) < limit:
                    logger.info("Última página alcanzada")
                    break
                
                start += limit
                
                # Pausa para no sobrecargar la API
                import time
                time.sleep(0.5)
                
            elif response.status_code == 429:
                logger.warning("Rate limit alcanzado, esperando...")
                import time
                time.sleep(2)
                continue
            else:
                logger.error(f"Error en API Alegra: {response.status_code} - {response.text}")
                break
                
        except Exception as e:
            logger.error(f"Error consultando API Alegra: {str(e)}")
            break
    
    return todos_los_items

def procesar_item_alegra(item, logger):
    """Procesar un item de Alegra para prepararlo para SharePoint"""
    try:
        # Obtener precio principal
        precio_principal = 0
        moneda = ""
        lista_precios = ""
        
        precios = item.get('price', [])
        if precios:
            precio_main = next((p for p in precios if p.get('main')), precios[0])
            precio_principal = precio_main.get('price', 0)
            moneda = precio_main.get('currency', {}).get('code', '')
            lista_precios = precio_main.get('name', '')
        
        # Obtener información de impuestos
        impuestos_info = procesar_impuestos_item(item.get('tax', []))
        
        # Datos procesados del item
        item_data = {
            'ID_Item': item.get('id'),
            'Nombre': item.get('name', ''),
            'Descripcion': item.get('description', ''),
            'Referencia': item.get('reference', ''),
            'Estado': item.get('status', ''),
            'Tipo': item.get('type', ''),
            'Tipo_Item': item.get('itemType', ''),
            'Clave_Producto': item.get('productKey', ''),
            
            # Categoría
            'Categoria_ID': safe_get_nested(item, 'category', 'id', default=''),
            'Categoria_Nombre': safe_get_nested(item, 'category', 'name', default=''),
            
            # Categoría de Item
            'Item_Categoria_ID': safe_get_nested(item, 'itemCategory', 'id', default=''),
            'Item_Categoria_Nombre': safe_get_nested(item, 'itemCategory', 'name', default=''),
            'Item_Categoria_Descripcion': safe_get_nested(item, 'itemCategory', 'description', default=''),
            
            # Precio
            'Precio_Principal': precio_principal,
            'Moneda': moneda,
            'Lista_Precios': lista_precios,
            
            # Inventario
            'Unidad_Medida': safe_get_nested(item, 'inventory', 'unit', default=''),
            'Cantidad_Inicial': safe_get_nested(item, 'inventory', 'initialQuantity', default=0),
            'Cantidad_Disponible': safe_get_nested(item, 'inventory', 'availableQuantity', default=0),
            'Costo_Unitario': safe_get_nested(item, 'inventory', 'unitCost', default=0),
            'Fecha_Cantidad_Inicial': safe_get_nested(item, 'inventory', 'initialQuantityDate', default=''),
            
            # Configuración
            'Escala_Calculo': item.get('calculationScale', 0),
            'Dias_Sin_IVA': item.get('hasNoIvaDays', False),
            
            # Impuestos
            'IVA_Porcentaje': impuestos_info['iva_porcentaje'],
            'IVA_Tipo': impuestos_info['iva_tipo'],
            'Otros_Impuestos': impuestos_info['otros_impuestos'],
            'Total_Impuestos': len(item.get('tax', [])),
            
            # Contadores
            'Cantidad_Precios': len(item.get('price', [])),
            'Cantidad_Campos_Personalizados': len(item.get('customFields', [])),
        }
        
        return item_data
        
    except Exception as e:
        logger.error(f"Error procesando item {item.get('id', 'N/A')}: {str(e)}")
        return None

def procesar_impuestos_item(impuestos):
    """Procesar información de impuestos de un item"""
    iva_porcentaje = 0
    iva_tipo = ""
    otros_impuestos = []
    
    for impuesto in impuestos:
        if impuesto is not None:
            tipo = impuesto.get('type', '').upper()
            nombre = impuesto.get('name', '')
            porcentaje = float(impuesto.get('percentage', 0))
            
            if tipo == 'IVA':
                iva_porcentaje = porcentaje
                iva_tipo = nombre
            else:
                otros_impuestos.append(f"{nombre}: {porcentaje}%")
    
    return {
        'iva_porcentaje': iva_porcentaje,
        'iva_tipo': iva_tipo,
        'otros_impuestos': ' | '.join(otros_impuestos)
    }

def safe_get_nested(obj, *keys, default=''):
    """Función helper para obtener valores anidados de forma segura"""
    for key in keys:
        if isinstance(obj, dict) and key in obj and obj[key] is not None:
            obj = obj[key]
        else:
            return default
    return obj if obj is not None else default

def subir_items_sharepoint(items_procesados, site_url, list_name, logger):
    """Subir items a SharePoint"""
    try:
        logger.info("Iniciando subida a SharePoint...")
        
        sp_connector = SharePointConnector()
        
        success_count = 0
        error_count = 0
        
        for i, item_data in enumerate(items_procesados):
            try:
                nombre_item = item_data.get('Nombre', f"Item-{item_data.get('ID_Item')}")
                logger.info(f"Subiendo item {i + 1}/{len(items_procesados)}: {nombre_item}")
                
                item_id = send_item_sharepoint(sp_connector, item_data, site_url, list_name, logger)
                
                if item_id:
                    success_count += 1
                    logger.info(f"Item {nombre_item} subido con ID: {item_id}")
                else:
                    error_count += 1
                    logger.error(f"Error subiendo item {nombre_item}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error procesando item {i + 1}: {str(e)}")
                continue
        
        logger.info("RESUMEN DE SUBIDA A SHAREPOINT:")
        logger.info(f"Items exitosos: {success_count}")
        logger.info(f"Items con errores: {error_count}")
        
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error durante subida a SharePoint: {str(e)}")
        return False

def send_item_sharepoint(sp_connector, item_data, site_url, list_name, logger):
    """Subir un item individual a lista de SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener el ID de la lista {list_name}")
            return None
        
        # Preparar datos para SharePoint según las columnas de tu lista
        sharepoint_data = {
            'fields': {
                "Title": item_data.get("Nombre", ""),
                "Categoria": item_data.get("Item_Categoria_Nombre", ""),
                "Precio_x0020_Principal": item_data.get("Precio_Principal", 0),
                # Mapear otros campos según las columnas que veo en tu captura:
                # - Modificado
                # - Creado  
                # - Creado por
                # - Modificado por
                # Estos campos se llenan automáticamente por SharePoint
            }
        }
        
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, headers=headers, json=sharepoint_data)
        
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
            logger.error(f"Error HTTP subiendo item: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        logger.error(f"Error subiendo item: {str(e)}")
        return None

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)