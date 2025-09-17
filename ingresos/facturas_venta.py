import requests
import base64
import os
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from ..core.sharepoint_connector import SharePointConnector

def setup_logging():
    """Configurar el sistema de logging"""
    
    # Calcular fecha (ayer)
    ayer = date.today() - timedelta(days=1)
    ayer_str = ayer.strftime('%Y-%m-%d')
    
    # Crear carpeta de logs si no existe
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Nombre del archivo de log con timestamp
    log_filename = f"logs/facturas_venta_{ayer_str}.log"
    
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
    logger.info("INICIO DEL PROCESO DE FACTURAS ALEGRA")
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
        list_name_facturas = os.getenv("list_facturas")
        list_name_items = os.getenv("list_items")
        
        logger.info(f"Site URL: {site_url}")
        
        # Verificar credenciales
        if not username or not password:
            logger.error("Credenciales de Alegra no encontradas en variables de entorno")
            return False
            
        if not site_url:
            logger.error("URL de SharePoint no encontrada en variables de entorno")
            return False
        
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Calcular fecha (ayer)
        ayer = date.today() - timedelta(days=1)
        ayer_str = ayer.strftime('%Y-%m-%d')
        logger.info(f"Procesando facturas del día: {ayer_str}")
        
        # Obtener datos de Alegra
        logger.info("Iniciando consulta a API de Alegra...")
        url = f"https://api.alegra.com/api/v1/invoices?date={ayer_str}"
        
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {encoded_credentials}"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Error consultando API Alegra: {response.status_code} - {response.text}")
            return False
            
        data = response.json()
        logger.info(f"Obtenidas {len(data)} facturas de Alegra")
        
        # Procesar facturas
        invoices_list = []
        items_list = []
        facturas_procesadas = 0
        facturas_con_error = 0
        
        for i, invoice in enumerate(data):
            try:
                if invoice is None:
                    logger.warning(f"Factura {i+1} es None, saltando...")
                    facturas_con_error += 1
                    continue
                
                # Datos básicos de la factura
                invoice_data = {
                    'ID': invoice.get('id'),
                    'Fecha': invoice.get('date'),
                    'Fecha_Vencimiento': invoice.get('dueDate'),
                    'Numero_Factura': safe_get_nested(invoice, 'numberTemplate', 'fullNumber', default=''),
                    'Estado': invoice.get('status'),
                    'Subtotal': invoice.get('subtotal', 0),
                    'Descuento': invoice.get('discount', 0),
                    'Impuestos': invoice.get('tax', 0),
                    'Total': invoice.get('total', 0),
                    'Total_Pagado': invoice.get('totalPaid', 0),
                    'Saldo': invoice.get('balance', 0),
                    'Termino_Pago': invoice.get('term', ''),
                    'Forma_Pago': invoice.get('paymentForm', ''),
                    
                    # Datos del cliente
                    'Cliente_ID': safe_get_nested(invoice, 'client', 'id', default=''),
                    'Cliente_Nombre': safe_get_nested(invoice, 'client', 'name', default=''),
                    'Cliente_Identificacion': safe_get_nested(invoice, 'client', 'identification', default=''),
                    'Cliente_Email': safe_get_nested(invoice, 'client', 'email', default=''),
                    'Cliente_Telefono': safe_get_nested(invoice, 'client', 'phonePrimary', default=''),
                    'Cliente_Ciudad': safe_get_nested(invoice, 'client', 'address', 'city', default=''),
                    'Cliente_Departamento': safe_get_nested(invoice, 'client', 'address', 'department', default=''),
                    'Cliente_Direccion': safe_get_nested(invoice, 'client', 'address', 'address', default=''),
                    
                    # Datos del vendedor
                    'Vendedor_Nombre': safe_get_nested(invoice, 'seller', 'name', default=''),
                    'Vendedor_ID': safe_get_nested(invoice, 'seller', 'identification', default=''),
                    
                    # Datos adicionales
                    'Observaciones': invoice.get('observations', ''),
                    'Anotacion': invoice.get('anotation', ''),
                    'Almacen': safe_get_nested(invoice, 'warehouse', 'name', default=''),
                    'Centro_Costo': safe_get_nested(invoice, 'costCenter', 'name', default=''),
                    
                    # CUFE
                    'CUFE': safe_get_nested(invoice, 'stamp', 'cufe', default=''),
                    'Estado_DIAN': safe_get_nested(invoice, 'stamp', 'legalStatus', default=''),
                    
                    # Número de items
                    'Cantidad_Items': len(invoice.get('items', [])) if invoice.get('items') else 0,
                }
                
                invoices_list.append(invoice_data)
                
                # Procesar items de la factura
                invoice_id = invoice.get('id')
                invoice_number = safe_get_nested(invoice, 'numberTemplate', 'fullNumber', default='')
                
                items = invoice.get('items', [])
                if items:
                    for item in items:
                        if item is not None:
                            item_data = {
                                'Factura_ID': invoice_id,
                                'Numero_Factura': invoice_number,
                                'Item_Nombre': item.get('name', ''),
                                'Item_Descripcion': item.get('description', ''),
                                'Item_Precio': item.get('price', 0),
                                'Item_Cantidad': item.get('quantity', 0),
                                'Item_Descuento': item.get('discount', 0),
                                'Item_Total': item.get('total', 0),
                                'Item_Referencia': item.get('reference', ''),
                                'Item_Unidad': item.get('unit', ''),
                            }
                            items_list.append(item_data)
                
                facturas_procesadas += 1
                
            except Exception as e:
                logger.error(f"Error procesando factura {i+1}: {str(e)}")
                facturas_con_error += 1
                continue
        
        logger.info(f"Procesamiento completado: {facturas_procesadas} exitosas, {facturas_con_error} con errores")
        
        # Crear DataFrames
        df_invoices = pd.DataFrame(invoices_list)
        df_items = pd.DataFrame(items_list)
        
        logger.info(f"DataFrames creados - Facturas: {len(df_invoices)}, Items: {len(df_items)}")
        
        # Subir solo a listas de SharePoint
        logger.info("INICIANDO SUBIDA A LISTAS DE SHAREPOINT")
        success_listas = subir_facturas_a_sharepoint(df_invoices, df_items, site_url, list_name_facturas, list_name_items, logger)
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO")
        logger.info("="*60)
        logger.info(f"Facturas procesadas desde Alegra: {len(df_invoices)}")
        logger.info(f"Items procesados: {len(df_items)}")
        logger.info(f"Datos subidos a listas: {'SI' if success_listas else 'NO'}")
        logger.info(f"Archivo de log: {log_file}")
        
        # Solo mostrar en consola el resumen final
        print(f"Proceso completado. Facturas: {len(df_invoices)}, Items: {len(df_items)}")
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

def subir_facturas_a_sharepoint(df_invoices, df_items, site_url, list_name_facturas, list_name_items, logger):
    """Subir facturas y items a listas de SharePoint"""
    try:
        logger.info("Iniciando subida a listas de SharePoint...")
        
        sp_connector = SharePointConnector()
        
        success_count = 0
        error_count = 0
        items_success_total = 0
        items_error_total = 0
        
        for index, factura_row in df_invoices.iterrows():
            try:
                numero_factura = factura_row['Numero_Factura']
                logger.info(f"Procesando factura {index + 1}/{len(df_invoices)}: {numero_factura}")
                
                datos_factura = factura_row.to_dict()
                factura_sharepoint_id = send_factura_sharepoint(sp_connector, datos_factura, site_url, list_name_facturas, logger)
                
                if factura_sharepoint_id:
                    success_count += 1
                    logger.info(f"Factura {numero_factura} subida con ID: {factura_sharepoint_id}")
                    
                    # Procesar items de esta factura
                    factura_items = df_items[df_items['Factura_ID'] == factura_row['ID']]
                    if not factura_items.empty:
                        logger.info(f"Procesando {len(factura_items)} items de la factura {numero_factura}")
                        
                        for _, item_row in factura_items.iterrows():
                            item_dict = item_row.to_dict()
                            item_id = send_item_factura_sharepoint(
                                sp_connector, item_dict, factura_sharepoint_id, site_url, list_name_items, logger
                            )
                            if item_id:
                                items_success_total += 1
                            else:
                                items_error_total += 1
                    else:
                        logger.info(f"No hay items para la factura {numero_factura}")
                else:
                    error_count += 1
                    logger.error(f"Error subiendo factura {numero_factura}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error procesando factura {index + 1}: {str(e)}")
                continue
        
        logger.info("RESUMEN DE SUBIDA A LISTAS:")
        logger.info(f"Facturas exitosas: {success_count}")
        logger.info(f"Facturas con errores: {error_count}")
        logger.info(f"Items exitosos: {items_success_total}")
        logger.info(f"Items con errores: {items_error_total}")
        
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error en subida a listas: {str(e)}")
        return False

def send_factura_sharepoint(sp_connector, datos_factura, site_url, list_name, logger):
    """Subir datos de factura a lista de SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener el ID de la lista {list_name}")
            return None
        
        item_data = {
            'fields': {
                "Title": str(datos_factura.get("ID", "")),
                "Fecha": datos_factura.get("Fecha", ""),
                "Fecha_x0020_Vencimiento": datos_factura.get("Fecha_Vencimiento", ""),
                "Numero_x0020_Factura": datos_factura.get("Numero_Factura", ""),
                "Subtotal": datos_factura.get("Subtotal", 0),
                "Descuento": datos_factura.get("Descuento", 0),
                "Impuestos": datos_factura.get("Impuestos", 0),
                "Total": datos_factura.get("Total", 0),
                "Total_x0020_Pagado": datos_factura.get("Total_Pagado", 0),
                "Saldo": datos_factura.get("Saldo", 0),
                "Cliente_x0020_Nombre": datos_factura.get("Cliente_Nombre", ""),
                "Estado": datos_factura.get("Estado", ""),
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
            logger.error(f"Error HTTP subiendo factura: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        logger.error(f"Error subiendo factura: {str(e)}")
        return None

def send_item_factura_sharepoint(sp_connector, datos_item, factura_lookup_id, site_url, list_name, logger):
    """Subir item de factura a lista de SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            return None
        
        lookup_variations = [
            "Factura_x0020_de_x0020_VentaLookupId",
            "Factura_x0020_de_x0020_Venta",
            "FacturadeVentaLookupId", 
            "FacturadeVenta"
        ]
        
        for lookup_field in lookup_variations:
            item_data = {
                'fields': {
                    lookup_field: str(factura_lookup_id),
                    "Title": datos_item.get("Numero_Factura", ""),
                    "Nombre": datos_item.get("Item_Nombre", ""),
                    "Precio": datos_item.get("Item_Precio", 0),
                    "Cantidad": datos_item.get("Item_Cantidad", 0),
                    "Descuento": datos_item.get("Item_Descuento", 0),
                    "Total": datos_item.get("Item_Total", 0),
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
        
        logger.warning(f"No se pudo subir item con ningún campo lookup")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo item: {str(e)}")
        return None

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)