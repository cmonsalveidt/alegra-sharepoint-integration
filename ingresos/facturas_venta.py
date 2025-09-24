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
    
    # Solo errores críticos en consola
    console_handler = logging.getLogger().handlers[1]
    console_handler.setLevel(logging.ERROR)
    
    return log_filename

def main():
    # Configurar logging
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("INICIO DEL PROCESO DE FACTURAS ALEGRA CON RETENCIONES Y CENTRO DE COSTOS")
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
        list_name_retenciones = os.getenv("list_retenciones_facturas", "Retenciones Facturas Venta")
        list_name_retenciones_sugeridas = os.getenv("list_retenciones_sugeridas", "Retenciones Sugerida Factura Venta")
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista facturas: {list_name_facturas}")
        logger.info(f"Lista items: {list_name_items}")
        logger.info(f"Lista retenciones: {list_name_retenciones}")
        logger.info(f"Lista retenciones sugeridas: {list_name_retenciones_sugeridas}")
        
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
        
        if len(data) == 0:
            logger.info("No se encontraron facturas para la fecha especificada - PROCESO EXITOSO")
            print(f"Sin facturas para {ayer_str}. Log en: {log_file}")
            return True
        
        # Procesar facturas
        invoices_list = []
        items_list = []
        retenciones_list = []
        retenciones_sugeridas_list = []
        
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
                    
                    # NUEVO: Datos del centro de costos
                    'Centro_Costo_ID': safe_get_nested(invoice, 'costCenter', 'id', default=''),
                    'Centro_Costo_Nombre': safe_get_nested(invoice, 'costCenter', 'name', default=''),
                    'Centro_Costo_Codigo': safe_get_nested(invoice, 'costCenter', 'code', default=''),
                    'Centro_Costo_Descripcion': safe_get_nested(invoice, 'costCenter', 'description', default=''),
                    
                    # Datos adicionales
                    'Observaciones': invoice.get('observations', ''),
                    'Anotacion': invoice.get('anotation', ''),
                    'Almacen': safe_get_nested(invoice, 'warehouse', 'name', default=''),
                    
                    # CUFE
                    'CUFE': safe_get_nested(invoice, 'stamp', 'cufe', default=''),
                    'Estado_DIAN': safe_get_nested(invoice, 'stamp', 'legalStatus', default=''),
                    
                    # Contadores
                    'Cantidad_Items': len(invoice.get('items', [])) if invoice.get('items') else 0,
                    'Cantidad_Retenciones': len(invoice.get('retentions', [])) if invoice.get('retentions') else 0,
                    'Cantidad_Retenciones_Sugeridas': len(invoice.get('retentionsSuggested', [])) if invoice.get('retentionsSuggested') else 0,
                }
                
                invoices_list.append(invoice_data)
                
                # Log información del centro de costos si existe
                if invoice_data['Centro_Costo_ID']:
                    logger.info(f"Factura {invoice_data['Numero_Factura']}: Centro de Costo ID={invoice_data['Centro_Costo_ID']}, Nombre='{invoice_data['Centro_Costo_Nombre']}'")
                
                # Datos comunes para referencia
                invoice_id = invoice.get('id')
                invoice_number = safe_get_nested(invoice, 'numberTemplate', 'fullNumber', default='')
                
                # Procesar items de la factura
                items = invoice.get('items', [])
                if items:
                    logger.info(f"Procesando {len(items)} items para factura {invoice_number}")
                    for item in items:
                        if item is not None:
                            # Procesar impuestos del item
                            tax_amount = 0
                            tax_info = item.get('tax', [])
                            if tax_info:
                                for tax in tax_info:
                                    if tax is not None:
                                        tax_amount += tax.get('amount', 0)
                            
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
                                'Item_Tax_Amount': tax_amount,  # NUEVO: Monto total de impuestos
                            }
                            items_list.append(item_data)
                
                # Procesar retenciones aplicadas
                retenciones = invoice.get('retentions', [])
                if retenciones:
                    logger.info(f"Procesando {len(retenciones)} retenciones aplicadas para factura {invoice_number}")
                    for retencion in retenciones:
                        if retencion is not None:
                            retencion_data = {
                                'Factura_ID': invoice_id,
                                'Numero_Factura': invoice_number,
                                'Retencion_ID': retencion.get('id'),
                                'Nombre': retencion.get('name', ''),
                                'Porcentaje': retencion.get('percentage', 0),
                                'Valor': retencion.get('amount', 0),
                                'Clave_Referencia': retencion.get('referenceKey', ''),
                                'Base': retencion.get('base', 0),
                                'Tipo': 'APLICADA'
                            }
                            retenciones_list.append(retencion_data)
                
                # Procesar retenciones sugeridas
                retenciones_sugeridas = invoice.get('retentionsSuggested', [])
                if retenciones_sugeridas:
                    logger.info(f"Procesando {len(retenciones_sugeridas)} retenciones sugeridas para factura {invoice_number}")
                    for retencion_sug in retenciones_sugeridas:
                        if retencion_sug is not None:
                            retencion_sugerida_data = {
                                'Factura_ID': invoice_id,
                                'Numero_Factura': invoice_number,
                                'Retencion_ID': retencion_sug.get('id'),
                                'Nombre': retencion_sug.get('name', ''),
                                'Porcentaje': retencion_sug.get('percentage', 0),
                                'Valor_Sugerido': float(retencion_sug.get('amount', 0)) if retencion_sug.get('amount') else 0,
                                'Clave_Referencia': retencion_sug.get('referenceKey', ''),
                                'Base': retencion_sug.get('base', 0) if retencion_sug.get('base') else 0,
                                'Tipo': 'SUGERIDA'
                            }
                            retenciones_sugeridas_list.append(retencion_sugerida_data)
                
                facturas_procesadas += 1
                
            except Exception as e:
                logger.error(f"Error procesando factura {i+1}: {str(e)}")
                facturas_con_error += 1
                continue
        
        logger.info(f"Procesamiento completado: {facturas_procesadas} exitosas, {facturas_con_error} con errores")
        
        # Crear DataFrames
        df_invoices = pd.DataFrame(invoices_list)
        df_items = pd.DataFrame(items_list)
        df_retenciones = pd.DataFrame(retenciones_list)
        df_retenciones_sugeridas = pd.DataFrame(retenciones_sugeridas_list)
        
        logger.info(f"DataFrames creados:")
        logger.info(f"  - Facturas: {len(df_invoices)}")
        logger.info(f"  - Items: {len(df_items)}")
        logger.info(f"  - Retenciones aplicadas: {len(df_retenciones)}")
        logger.info(f"  - Retenciones sugeridas: {len(df_retenciones_sugeridas)}")
        
        # Mostrar resumen de centros de costos procesados
        if len(df_invoices) > 0:
            facturas_con_centro = df_invoices[df_invoices['Centro_Costo_ID'] != '']
            if len(facturas_con_centro) > 0:
                logger.info(f"Facturas con centro de costos: {len(facturas_con_centro)}")
                centros_unicos = facturas_con_centro[['Centro_Costo_ID', 'Centro_Costo_Nombre']].drop_duplicates()
                logger.info("Centros de costos encontrados:")
                for _, centro in centros_unicos.iterrows():
                    logger.info(f"  - ID: {centro['Centro_Costo_ID']}, Nombre: '{centro['Centro_Costo_Nombre']}'")
            else:
                logger.info("Ninguna factura tiene centro de costos asignado")
        
        # Mostrar resumen de retenciones procesadas
        if len(df_retenciones) > 0:
            logger.info("Retenciones aplicadas encontradas:")
            for _, ret in df_retenciones.iterrows():
                logger.info(f"  - {ret['Nombre']}: {ret['Porcentaje']}% = ${ret['Valor']}")
        
        if len(df_retenciones_sugeridas) > 0:
            logger.info("Retenciones sugeridas encontradas:")
            for _, ret in df_retenciones_sugeridas.iterrows():
                logger.info(f"  - {ret['Nombre']}: {ret['Porcentaje']}% = ${ret['Valor_Sugerido']}")
        
        # Subir a listas de SharePoint
        logger.info("INICIANDO SUBIDA A LISTAS DE SHAREPOINT")
        success_listas = subir_facturas_completas_sharepoint(
            df_invoices, 
            df_items, 
            df_retenciones, 
            df_retenciones_sugeridas, 
            site_url, 
            list_name_facturas, 
            list_name_items, 
            list_name_retenciones,
            list_name_retenciones_sugeridas,
            logger
        )
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO CON RETENCIONES Y CENTRO DE COSTOS")
        logger.info("="*60)
        logger.info(f"Facturas procesadas desde Alegra: {len(df_invoices)}")
        logger.info(f"Items procesados: {len(df_items)}")
        logger.info(f"Retenciones aplicadas procesadas: {len(df_retenciones)}")
        logger.info(f"Retenciones sugeridas procesadas: {len(df_retenciones_sugeridas)}")
        logger.info(f"Facturas con centro de costos: {len(df_invoices[df_invoices['Centro_Costo_ID'] != ''])}")
        logger.info(f"Datos subidos a listas: {'SI' if success_listas else 'NO'}")
        logger.info(f"Archivo de log: {log_file}")
        
        # Solo mostrar en consola el resumen final
        print(f"Proceso completado:")
        print(f"  Facturas: {len(df_invoices)}")
        print(f"  Items: {len(df_items)}")
        print(f"  Retenciones aplicadas: {len(df_retenciones)}")
        print(f"  Retenciones sugeridas: {len(df_retenciones_sugeridas)}")
        print(f"  Facturas con centro de costos: {len(df_invoices[df_invoices['Centro_Costo_ID'] != ''])}")
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

def subir_facturas_completas_sharepoint(df_invoices, df_items, df_retenciones, df_retenciones_sugeridas, 
                                       site_url, list_name_facturas, list_name_items, list_name_retenciones,
                                       list_name_retenciones_sugeridas, logger):
    """Subir facturas completas con items y retenciones a SharePoint"""
    try:
        logger.info("Iniciando subida completa a listas de SharePoint...")
        
        sp_connector = SharePointConnector()
        
        # Contadores
        success_count = 0
        error_count = 0
        items_success_total = 0
        items_error_total = 0
        retenciones_success_total = 0
        retenciones_error_total = 0
        retenciones_sug_success_total = 0
        retenciones_sug_error_total = 0
        
        # Procesar cada factura individual
        for index, factura_row in df_invoices.iterrows():
            try:
                # Obtener datos de la factura actual
                factura_alegra_id = factura_row['ID']  # ID de Alegra
                numero_factura = factura_row['Numero_Factura']
                
                logger.info(f"Procesando factura {index + 1}/{len(df_invoices)}: {numero_factura}")
                
                # 1. SUBIR FACTURA A SHAREPOINT
                datos_factura = factura_row.to_dict()
                factura_sharepoint_id = send_factura_sharepoint(sp_connector, datos_factura, site_url, list_name_facturas, logger)
                
                if factura_sharepoint_id:
                    success_count += 1
                    logger.info(f"Factura {numero_factura} subida con ID: {factura_sharepoint_id}")
                    
                    # 2. PROCESAR ITEMS DE ESTA FACTURA
                    if not df_items.empty and 'Factura_ID' in df_items.columns:
                        items_de_esta_factura = df_items[df_items['Factura_ID'] == factura_alegra_id]
                        
                        if not items_de_esta_factura.empty:
                            logger.info(f"Procesando {len(items_de_esta_factura)} items de la factura {numero_factura}")
                            
                            for _, item_row in items_de_esta_factura.iterrows():
                                try:
                                    item_dict = item_row.to_dict()
                                    item_id = send_item_factura_sharepoint(
                                        sp_connector, item_dict, factura_sharepoint_id, site_url, list_name_items, logger
                                    )
                                    if item_id:
                                        items_success_total += 1
                                    else:
                                        items_error_total += 1
                                except Exception as e:
                                    items_error_total += 1
                                    logger.error(f"Error procesando item: {str(e)}")
                    
                    # 3. PROCESAR RETENCIONES APLICADAS DE ESTA FACTURA
                    if not df_retenciones.empty and 'Factura_ID' in df_retenciones.columns:
                        retenciones_de_esta_factura = df_retenciones[df_retenciones['Factura_ID'] == factura_alegra_id]
                        
                        if not retenciones_de_esta_factura.empty:
                            logger.info(f"Procesando {len(retenciones_de_esta_factura)} retenciones aplicadas de la factura {numero_factura}")
                            
                            for _, retencion_row in retenciones_de_esta_factura.iterrows():
                                try:
                                    retencion_dict = retencion_row.to_dict()
                                    retencion_id = send_retencion_factura_sharepoint(
                                        sp_connector, retencion_dict, factura_sharepoint_id, site_url, list_name_retenciones, logger
                                    )
                                    if retencion_id:
                                        retenciones_success_total += 1
                                        logger.info(f"Retención aplicada '{retencion_dict.get('Nombre')}' subida con ID: {retencion_id}")
                                    else:
                                        retenciones_error_total += 1
                                except Exception as e:
                                    retenciones_error_total += 1
                                    logger.error(f"Error procesando retención aplicada: {str(e)}")
                    
                    # 4. PROCESAR RETENCIONES SUGERIDAS DE ESTA FACTURA
                    if not df_retenciones_sugeridas.empty and 'Factura_ID' in df_retenciones_sugeridas.columns:
                        retenciones_sug_de_esta_factura = df_retenciones_sugeridas[df_retenciones_sugeridas['Factura_ID'] == factura_alegra_id]
                        
                        if not retenciones_sug_de_esta_factura.empty:
                            logger.info(f"Procesando {len(retenciones_sug_de_esta_factura)} retenciones sugeridas de la factura {numero_factura}")
                            
                            for _, retencion_sug_row in retenciones_sug_de_esta_factura.iterrows():
                                try:
                                    retencion_sug_dict = retencion_sug_row.to_dict()
                                    retencion_sug_id = send_retencion_sugerida_factura_sharepoint(
                                        sp_connector, retencion_sug_dict, factura_sharepoint_id, site_url, list_name_retenciones_sugeridas, logger
                                    )
                                    if retencion_sug_id:
                                        retenciones_sug_success_total += 1
                                        logger.info(f"Retención sugerida '{retencion_sug_dict.get('Nombre')}' subida con ID: {retencion_sug_id}")
                                    else:
                                        retenciones_sug_error_total += 1
                                except Exception as e:
                                    retenciones_sug_error_total += 1
                                    logger.error(f"Error procesando retención sugerida: {str(e)}")
                    
                else:
                    error_count += 1
                    logger.error(f"Error subiendo factura {numero_factura}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error procesando factura {index + 1}: {str(e)}")
                continue
        
        # RESUMEN FINAL
        logger.info("RESUMEN DE SUBIDA COMPLETA A LISTAS:")
        logger.info(f"Facturas exitosas: {success_count}")
        logger.info(f"Facturas con errores: {error_count}")
        logger.info(f"Items exitosos: {items_success_total}")
        logger.info(f"Items con errores: {items_error_total}")
        logger.info(f"Retenciones aplicadas exitosas: {retenciones_success_total}")
        logger.info(f"Retenciones aplicadas con errores: {retenciones_error_total}")
        logger.info(f"Retenciones sugeridas exitosas: {retenciones_sug_success_total}")
        logger.info(f"Retenciones sugeridas con errores: {retenciones_sug_error_total}")
        
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error crítico en subida completa a listas: {str(e)}")
        return False

def send_factura_sharepoint(sp_connector, datos_factura, site_url, list_name, logger):
    """Subir datos de factura a lista de SharePoint - ACTUALIZADO CON CENTRO DE COSTOS"""
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
                "ID_x0020_Centro_x0020_de_x0020_C": datos_factura.get("Centro_Costo_ID", ""),
                "Centro_x0020_de_x0020_Costos": datos_factura.get("Centro_Costo_Nombre", ""),
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
            try:
                item_data = {
                    'fields': {
                        lookup_field: int(factura_lookup_id),
                        "Title": datos_item.get("Numero_Factura", ""),
                        "Nombre": datos_item.get("Item_Nombre", ""),
                        "Precio": datos_item.get("Item_Precio", 0),
                        "Cantidad": datos_item.get("Item_Cantidad", 0),
                        "Descuento": datos_item.get("Item_Descuento", 0),
                        "Total": datos_item.get("Item_Total", 0),
                        "ID_x0020_Factura": datos_item.get("Factura_ID", ""),  # NUEVO: ID de la factura
                        "Impuestos": datos_item.get("Item_Tax_Amount", 0),     # NUEVO: Monto de impuestos
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
                else:
                    continue
                    
            except Exception as e:
                continue
        
        logger.warning(f"No se pudo subir item con ningún campo lookup")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo item: {str(e)}")
        return None

def send_retencion_factura_sharepoint(sp_connector, datos_retencion, factura_lookup_id, site_url, list_name, logger):
    """Subir retención aplicada de factura a lista de SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener el ID de la lista {list_name}")
            return None
        
        # Intentar diferentes variaciones de campo lookup
        lookup_variations = [
            "Factura_x0020_de_x0020_VentaLookupId",
            "Factura_x0020_de_x0020_Venta",
            "FacturadeVentaLookupId", 
            "FacturadeVenta"
        ]
        
        for lookup_field in lookup_variations:
            try:
                item_data = {
                    'fields': {
                        lookup_field: int(factura_lookup_id),
                        "Title": str(datos_retencion.get("Retencion_ID", "")),
                        "Nombre": datos_retencion.get("Nombre", ""),
                        "Porcentaje": float(datos_retencion.get("Porcentaje", 0)),
                        "Monto": datos_retencion.get("Valor", 0),
                        "Clave_x0020_Referencia": datos_retencion.get("Clave_Referencia", ""),
                        "Base": datos_retencion.get("Base", 0),
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
                else:
                    continue
                    
            except Exception as e:
                continue
        
        logger.error(f"No se pudo subir retención con ningún campo lookup")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo retención: {str(e)}")
        return None

def send_retencion_sugerida_factura_sharepoint(sp_connector, datos_retencion_sugerida, factura_lookup_id, site_url, list_name, logger):
    """Subir retención sugerida de factura a lista de SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener el ID de la lista {list_name}")
            return None
        
        # Intentar diferentes variaciones de campo lookup
        lookup_variations = [
            "Factura_x0020_de_x0020_VentaLookupId",
            "Factura_x0020_de_x0020_Venta",
            "FacturadeVentaLookupId", 
            "FacturadeVenta"
        ]
        
        for lookup_field in lookup_variations:
            try:
                item_data = {
                    'fields': {
                        lookup_field: int(factura_lookup_id),
                        "Title": str(datos_retencion_sugerida.get("Retencion_ID", "")),
                        "Nombre": datos_retencion_sugerida.get("Nombre", ""),
                        "Porcentaje": float(datos_retencion_sugerida.get("Porcentaje", 0)),
                        "Monto": datos_retencion_sugerida.get("Valor_Sugerido", 0),
                        "Clave_x0020_Referencia": datos_retencion_sugerida.get("Clave_Referencia", ""),
                        "Base": datos_retencion_sugerida.get("Base", 0),
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
                else:
                    continue
                    
            except Exception as e:
                continue
        
        logger.error(f"No se pudo subir retención sugerida con ningún campo lookup")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo retención sugerida: {str(e)}")
        return None

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)