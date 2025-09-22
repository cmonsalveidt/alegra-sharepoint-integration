import requests
import base64
import os
import sys
import pandas as pd
import json
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sharepoint_connector import SharePointConnector

# Configurar logging
def setup_logging():
    """Configurar el sistema de logging"""
    
    # Crear carpeta de logs si no existe
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Nombre del archivo de log con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/facturas_historico_{timestamp}.log"
    
    # Configurar el logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # También mostrar en consola
        ]
    )
    
    # Filtro para mostrar solo INFO y errores críticos en consola
    console_handler = logging.getLogger().handlers[1]
    console_handler.setLevel(logging.INFO)
    
    return log_filename

def generar_rango_fechas(fecha_inicio="2024-01-01", fecha_fin=None):
    """
    Genera un rango de fechas desde fecha_inicio hasta fecha_fin
    Si fecha_fin es None, usa la fecha actual
    """
    if fecha_fin is None:
        fecha_fin = date.today()
    elif isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
    
    if isinstance(fecha_inicio, str):
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
    
    fechas = []
    fecha_actual = fecha_inicio
    
    while fecha_actual <= fecha_fin:
        fechas.append(fecha_actual.strftime('%Y-%m-%d'))
        fecha_actual += timedelta(days=1)
    
    return fechas

def obtener_facturas_por_fecha(encoded_credentials, fecha_str, logger):
    """Obtener facturas de una fecha específica desde Alegra"""
    try:
        url = f"https://api.alegra.com/api/v1/invoices?date={fecha_str}"
        
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {encoded_credentials}"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"Fecha {fecha_str}: {len(data)} facturas obtenidas")
            return data
        elif response.status_code == 429:
            logger.warning(f"Rate limit alcanzado para fecha {fecha_str}, esperando...")
            # Esperar un poco antes de continuar
            import time
            time.sleep(2)
            return obtener_facturas_por_fecha(encoded_credentials, fecha_str, logger)
        else:
            logger.error(f"Error consultando fecha {fecha_str}: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        logger.error(f"Error consultando fecha {fecha_str}: {str(e)}")
        return []

def main():
    # Configurar logging
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("INICIO DEL PROCESO HISTÓRICO DE FACTURAS ALEGRA")
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
        
        # Configuración de fechas
        fecha_inicio = os.getenv("FECHA_INICIO", "2024-01-01")  # Se puede configurar en .env
        fecha_fin = os.getenv("FECHA_FIN")  # Si no está definida, usa fecha actual
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista facturas: {list_name_facturas}")
        logger.info(f"Lista items: {list_name_items}")
        logger.info(f"Lista retenciones: {list_name_retenciones}")
        logger.info(f"Lista retenciones sugeridas: {list_name_retenciones_sugeridas}")
        logger.info(f"Fecha inicio: {fecha_inicio}")
        logger.info(f"Fecha fin: {fecha_fin or 'Fecha actual'}")
        
        # Verificar credenciales
        if not username or not password:
            logger.error("Credenciales de Alegra no encontradas en variables de entorno")
            return False
            
        if not site_url:
            logger.error("URL de SharePoint no encontrada en variables de entorno")
            return False
        
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Generar rango de fechas
        logger.info("Generando rango de fechas...")
        fechas = generar_rango_fechas(fecha_inicio, fecha_fin)
        logger.info(f"Total de fechas a procesar: {len(fechas)}")
        logger.info(f"Desde: {fechas[0]} hasta: {fechas[-1]}")
        
        # Listas para acumular todos los datos
        todas_las_facturas = []
        todos_los_items = []
        todas_las_retenciones = []
        todas_las_retenciones_sug = []
        
        # Contadores globales
        total_facturas_obtenidas = 0
        fechas_exitosas = 0
        fechas_con_error = 0
        
        # Procesar cada fecha
        logger.info("Iniciando procesamiento por fechas...")
        
        for i, fecha in enumerate(fechas, 1):
            try:
                logger.info(f"Procesando fecha {i}/{len(fechas)}: {fecha}")
                
                # Obtener facturas de esta fecha
                facturas_fecha = obtener_facturas_por_fecha(encoded_credentials, fecha, logger)
                
                if facturas_fecha:
                    logger.info(f"  -> {len(facturas_fecha)} facturas encontradas")
                    
                    # Procesar facturas de esta fecha
                    facturas_procesadas, items_procesados, retenciones_procesadas, retenciones_sug_procesadas = procesar_facturas_fecha(facturas_fecha, fecha, logger)
                    
                    # Acumular resultados
                    todas_las_facturas.extend(facturas_procesadas)
                    todos_los_items.extend(items_procesados)
                    todas_las_retenciones.extend(retenciones_procesadas)
                    todas_las_retenciones_sug.extend(retenciones_sug_procesadas)
                    
                    total_facturas_obtenidas += len(facturas_fecha)
                    fechas_exitosas += 1
                    
                    logger.info(f"  -> Procesadas {len(facturas_procesadas)} facturas, {len(items_procesados)} items, {len(retenciones_procesadas)} retenciones, {len(retenciones_sug_procesadas)} retenciones sugeridas")
                else:
                    logger.info(f"  -> Sin facturas para {fecha}")
                    fechas_exitosas += 1
                
                # Pausa pequeña para no sobrecargar la API
                if i % 10 == 0:  # Cada 10 fechas, pausa más larga
                    logger.info(f"Pausa después de {i} fechas procesadas...")
                    import time
                    time.sleep(1)
                
            except Exception as e:
                fechas_con_error += 1
                logger.error(f"Error procesando fecha {fecha}: {str(e)}")
                continue
        
        # Resumen de procesamiento
        logger.info("="*60)
        logger.info("RESUMEN DE PROCESAMIENTO POR FECHAS")
        logger.info("="*60)
        logger.info(f"Fechas procesadas exitosamente: {fechas_exitosas}")
        logger.info(f"Fechas con errores: {fechas_con_error}")
        logger.info(f"Total facturas obtenidas: {total_facturas_obtenidas}")
        logger.info(f"Total facturas procesadas: {len(todas_las_facturas)}")
        logger.info(f"Total items procesados: {len(todos_los_items)}")
        logger.info(f"Total retenciones procesadas: {len(todas_las_retenciones)}")
        logger.info(f"Total retenciones sugeridas procesadas: {len(todas_las_retenciones_sug)}")
        
        if not todas_las_facturas:
            logger.warning("No se encontraron facturas para procesar")
            return True
        
        # Crear DataFrames
        logger.info("Creando DataFrames finales...")
        df_invoices = pd.DataFrame(todas_las_facturas)
        df_items = pd.DataFrame(todos_los_items)
        df_retenciones = pd.DataFrame(todas_las_retenciones) if todas_las_retenciones else pd.DataFrame()
        df_retenciones_sug = pd.DataFrame(todas_las_retenciones_sug) if todas_las_retenciones_sug else pd.DataFrame()
        
        logger.info(f"DataFrames creados - Facturas: {len(df_invoices)}, Items: {len(df_items)}, Retenciones: {len(df_retenciones)}, Ret. Sugeridas: {len(df_retenciones_sug)}")
        
        # Subir a SharePoint (solo listas)
        logger.info("INICIANDO SUBIDA A SHAREPOINT")
        
        # Subir a listas de SharePoint (en lotes para evitar timeouts)
        success_listas = subir_facturas_en_lotes(df_invoices, df_items, df_retenciones, df_retenciones_sug, site_url, list_name_facturas, list_name_items, list_name_retenciones, list_name_retenciones_sugeridas, logger)
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO HISTÓRICO")
        logger.info("="*60)
        logger.info(f"Período procesado: {fecha_inicio} a {fechas[-1]}")
        logger.info(f"Fechas exitosas: {fechas_exitosas}/{len(fechas)}")
        logger.info(f"Facturas procesadas: {len(df_invoices)}")
        logger.info(f"Items procesados: {len(df_items)}")
        logger.info(f"Retenciones procesadas: {len(df_retenciones)}")
        logger.info(f"Retenciones sugeridas procesadas: {len(df_retenciones_sug)}")
        logger.info(f"Datos subidos a listas: {'SI' if success_listas else 'NO'}")
        logger.info(f"Archivo de log: {log_file}")
        
        # Mostrar en consola
        print(f"Proceso histórico completado:")
        print(f"  Período: {fecha_inicio} a {fechas[-1]}")
        print(f"  Facturas: {len(df_invoices)}, Items: {len(df_items)}")
        print(f"  Retenciones: {len(df_retenciones)}, Ret. Sugeridas: {len(df_retenciones_sug)}")
        print(f"  Log: {log_file}")
        
        return success_listas
        
    except Exception as e:
        logger.error(f"Error crítico en el proceso principal: {str(e)}")
        logger.error("Detalles del error:", exc_info=True)
        print(f"ERROR: {str(e)}. Ver detalles en: {log_file}")
        return False

def procesar_facturas_fecha(facturas_fecha, fecha, logger):
    """Procesar facturas de una fecha específica"""
    facturas_procesadas = []
    items_procesados = []
    retenciones_procesadas = []
    retenciones_sug_procesadas = []
    
    for i, invoice in enumerate(facturas_fecha):
        try:
            if invoice is None:
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
                'Cantidad_Retenciones': len(invoice.get('retentions', [])) if invoice.get('retentions') else 0,
                'Cantidad_Retenciones_Sugeridas': len(invoice.get('retentionsSuggested', [])) if invoice.get('retentionsSuggested') else 0,
            }
            
            facturas_procesadas.append(invoice_data)
            
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
                        items_procesados.append(item_data)
            
            # Procesar retenciones aplicadas
            retenciones = invoice.get('retentions', [])
            if retenciones:
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
                        retenciones_procesadas.append(retencion_data)
            
            # Procesar retenciones sugeridas
            retenciones_sugeridas = invoice.get('retentionsSuggested', [])
            if retenciones_sugeridas:
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
                        retenciones_sug_procesadas.append(retencion_sugerida_data)
            
        except Exception as e:
            logger.error(f"Error procesando factura {i+1} de fecha {fecha}: {str(e)}")
            continue
    
    return facturas_procesadas, items_procesados, retenciones_procesadas, retenciones_sug_procesadas

def safe_get_nested(obj, *keys, default=''):
    """Función helper para obtener valores anidados de forma segura"""
    for key in keys:
        if isinstance(obj, dict) and key in obj and obj[key] is not None:
            obj = obj[key]
        else:
            return default
    return obj if obj is not None else default

def subir_excel_sharepoint(df_invoices, df_items, df_retenciones, df_retenciones_sug, site_url, carpeta_excel, fecha_inicio, fecha_fin, logger):
    """Subir Excel histórico a SharePoint"""
    try:
        logger.info("Iniciando subida de Excel histórico a SharePoint...")
        
        uploader = SharePointUploader(site_url)
        
        # Preparar DataFrames
        dataframes = {'Facturas': df_invoices}
        
        if not df_items.empty:
            dataframes['Items_Detalle'] = df_items
            logger.info("Agregada hoja de Items al Excel")
        
        if not df_retenciones.empty:
            dataframes['Retenciones_Aplicadas'] = df_retenciones
            logger.info("Agregada hoja de Retenciones al Excel")
        
        if not df_retenciones_sug.empty:
            dataframes['Retenciones_Sugeridas'] = df_retenciones_sug
            logger.info("Agregada hoja de Retenciones Sugeridas al Excel")
        
        if not df_invoices.empty:
            estado_counts = df_invoices['Estado'].value_counts() if 'Estado' in df_invoices.columns else {}
            total_retenciones = len(df_retenciones) if not df_retenciones.empty else 0
            total_ret_sugeridas = len(df_retenciones_sug) if not df_retenciones_sug.empty else 0
            
            stats_data = {
                'Métrica': [
                    'Período Procesado',
                    'Total Facturas',
                    'Facturas Abiertas', 
                    'Facturas Cerradas',
                    'Suma Total Facturas',
                    'Suma Saldos Pendientes',
                    'Promedio por Factura',
                    'Total Items',
                    'Total Retenciones Aplicadas',
                    'Total Retenciones Sugeridas'
                ],
                'Valor': [
                    f"{fecha_inicio} a {fecha_fin}",
                    len(df_invoices),
                    estado_counts.get('open', 0),
                    estado_counts.get('closed', 0),
                    f"${df_invoices['Total'].sum():,.2f}",
                    f"${df_invoices['Saldo'].sum():,.2f}",
                    f"${df_invoices['Total'].mean():,.2f}",
                    len(df_items),
                    total_retenciones,
                    total_ret_sugeridas
                ]
            }
            dataframes['Estadisticas'] = pd.DataFrame(stats_data)
            logger.info("Agregada hoja de Estadísticas al Excel")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = f"facturas_historico_{fecha_inicio}_a_{fecha_fin}_{timestamp}.xlsx"
        
        logger.info(f"Subiendo archivo: {nombre_archivo}")
        
        resultado = uploader.upload_excel_from_dataframes(
            dataframes=dataframes,
            filename=nombre_archivo,
            folder_path=carpeta_excel
        )
        
        if resultado.get('success'):
            logger.info(f"Excel histórico subido exitosamente!")
            logger.info(f"Archivo: {resultado.get('filename')}")
            logger.info(f"URL: {resultado.get('web_url')}")
            return True
        else:
            logger.error(f"Error subiendo Excel: {resultado.get('error')}")
            return False
    
    except Exception as e:
        logger.error(f"Error en subida de Excel: {str(e)}")
        return False

def subir_facturas_en_lotes(df_invoices, df_items, df_retenciones, df_retenciones_sug, site_url, list_name_facturas, list_name_items, list_name_retenciones, list_name_retenciones_sug, logger, lote_size=50):
    """Subir facturas a SharePoint en lotes para evitar timeouts"""
    try:
        logger.info(f"Iniciando subida en lotes a SharePoint (tamaño lote: {lote_size})...")
        
        sp_connector = SharePointConnector()
        
        total_facturas = len(df_invoices)
        facturas_exitosas = 0
        facturas_error = 0
        items_exitosos = 0
        items_error = 0
        retenciones_exitosas = 0
        retenciones_error = 0
        retenciones_sug_exitosas = 0
        retenciones_sug_error = 0
        
        # Procesar en lotes
        for inicio in range(0, total_facturas, lote_size):
            fin = min(inicio + lote_size, total_facturas)
            lote_actual = inicio // lote_size + 1
            total_lotes = (total_facturas + lote_size - 1) // lote_size
            
            logger.info(f"Procesando lote {lote_actual}/{total_lotes}: facturas {inicio+1} a {fin}")
            
            lote_facturas = df_invoices.iloc[inicio:fin]
            
            for index, factura_row in lote_facturas.iterrows():
                try:
                    numero_factura = factura_row['Numero_Factura']
                    
                    datos_factura = factura_row.to_dict()
                    factura_sharepoint_id = send_factura_sharepoint(sp_connector, datos_factura, site_url, list_name_facturas, logger)
                    
                    if factura_sharepoint_id:
                        facturas_exitosas += 1
                        
                        # Procesar items de esta factura
                        factura_items = df_items[df_items['Factura_ID'] == factura_row['ID']]
                        if not factura_items.empty:
                            for _, item_row in factura_items.iterrows():
                                item_dict = item_row.to_dict()
                                item_id = send_item_factura_sharepoint(
                                    sp_connector, item_dict, factura_sharepoint_id, site_url, list_name_items, logger
                                )
                                if item_id:
                                    items_exitosos += 1
                                else:
                                    items_error += 1
                        
                        # Procesar retenciones de esta factura
                        if not df_retenciones.empty:
                            factura_retenciones = df_retenciones[df_retenciones['Factura_ID'] == factura_row['ID']]
                            for _, ret_row in factura_retenciones.iterrows():
                                ret_dict = ret_row.to_dict()
                                ret_id = send_retencion_factura_sharepoint(
                                    sp_connector, ret_dict, factura_sharepoint_id, site_url, list_name_retenciones, logger
                                )
                                if ret_id:
                                    retenciones_exitosas += 1
                                else:
                                    retenciones_error += 1
                        
                        # Procesar retenciones sugeridas de esta factura
                        if not df_retenciones_sug.empty:
                            factura_ret_sug = df_retenciones_sug[df_retenciones_sug['Factura_ID'] == factura_row['ID']]
                            for _, ret_sug_row in factura_ret_sug.iterrows():
                                ret_sug_dict = ret_sug_row.to_dict()
                                ret_sug_id = send_retencion_sugerida_factura_sharepoint(
                                    sp_connector, ret_sug_dict, factura_sharepoint_id, site_url, list_name_retenciones_sug, logger
                                )
                                if ret_sug_id:
                                    retenciones_sug_exitosas += 1
                                else:
                                    retenciones_sug_error += 1
                    else:
                        facturas_error += 1
                        
                except Exception as e:
                    facturas_error += 1
                    logger.error(f"Error procesando factura en lote: {str(e)}")
                    continue
            
            # Pausa entre lotes
            if lote_actual < total_lotes:
                logger.info(f"Pausa entre lotes... ({facturas_exitosas} exitosas hasta ahora)")
                import time
                time.sleep(2)
        
        logger.info("RESUMEN DE SUBIDA EN LOTES:")
        logger.info(f"Facturas exitosas: {facturas_exitosas}")
        logger.info(f"Facturas con errores: {facturas_error}")
        logger.info(f"Items exitosos: {items_exitosos}")
        logger.info(f"Items con errores: {items_error}")
        logger.info(f"Retenciones exitosas: {retenciones_exitosas}")
        logger.info(f"Retenciones con errores: {retenciones_error}")
        logger.info(f"Retenciones sugeridas exitosas: {retenciones_sug_exitosas}")
        logger.info(f"Retenciones sugeridas con errores: {retenciones_sug_error}")
        
        return facturas_exitosas > 0
        
    except Exception as e:
        logger.error(f"Error en subida en lotes: {str(e)}")
        return False

# Funciones de subida con lookup corregido
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
    """Subir item de factura a lista de SharePoint con lookup corregido"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            return None
        
        # Lista de posibles nombres de campos lookup para probar
        lookup_field_variations = [
            "Factura_x0020_de_x0020_VentaLookupId",
            "Factura_x0020_de_x0020_Venta",
            "FacturadeVentaLookupId", 
            "FacturadeVenta",
            "FacturaLookupId",
            "Factura"
        ]
        
        for lookup_field in lookup_field_variations:
            try:
                item_data = {
                    'fields': {
                        lookup_field: int(factura_lookup_id),  # Convertir a int explícitamente
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
                else:
                    logger.debug(f"Error con campo {lookup_field}: {response.status_code} - {response.text}")
                    continue
                    
            except Exception as e:
                logger.debug(f"Error probando campo {lookup_field}: {str(e)}")
                continue
        
        logger.warning(f"No se pudo subir item con ningún campo lookup probado")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo item: {str(e)}")
        return None

def send_retencion_factura_sharepoint(sp_connector, datos_retencion, factura_lookup_id, site_url, list_name, logger):
    """Subir retención aplicada de factura a lista de SharePoint con lookup corregido"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            return None
        
        # Lista de posibles nombres de campos lookup para probar
        lookup_field_variations = [
            "Factura_x0020_de_x0020_VentaLookupId",
            "Factura_x0020_de_x0020_Venta",
            "FacturadeVentaLookupId", 
            "FacturadeVenta",
            "FacturaLookupId",
            "Factura"
        ]
        
        for lookup_field in lookup_field_variations:
            try:
                item_data = {
                    'fields': {
                        lookup_field: int(factura_lookup_id),  # Convertir a int explícitamente
                        "Title": datos_retencion.get("Retencion_ID", ""),
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
                    logger.debug(f"Error con campo {lookup_field}: {response.status_code} - {response.text}")
                    continue
                    
            except Exception as e:
                logger.debug(f"Error probando campo {lookup_field}: {str(e)}")
                continue
        
        logger.warning(f"No se pudo subir retención con ningún campo lookup probado")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo retención: {str(e)}")
        return None

def send_retencion_sugerida_factura_sharepoint(sp_connector, datos_retencion_sugerida, factura_lookup_id, site_url, list_name, logger):
    """Subir retención sugerida de factura a lista de SharePoint con lookup corregido"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            return None
        
        # Lista de posibles nombres de campos lookup para probar
        lookup_field_variations = [
            "Factura_x0020_de_x0020_VentaLookupId",
            "Factura_x0020_de_x0020_Venta",
            "FacturadeVentaLookupId", 
            "FacturadeVenta",
            "FacturaLookupId",
            "Factura"
        ]
        
        for lookup_field in lookup_field_variations:
            try:
                item_data = {
                    'fields': {
                        lookup_field: int(factura_lookup_id),  # Convertir a int explícitamente
                        "Title": datos_retencion_sugerida.get("Retencion_ID", ""),
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
                    logger.debug(f"Error con campo {lookup_field}: {response.status_code} - {response.text}")
                    continue
                    
            except Exception as e:
                logger.debug(f"Error probando campo {lookup_field}: {str(e)}")
                continue
        
        logger.warning(f"No se pudo subir retención sugerida con ningún campo lookup probado")
        return None
        
    except Exception as e:
        logger.error(f"Error subiendo retención sugerida: {str(e)}")
        return None

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)