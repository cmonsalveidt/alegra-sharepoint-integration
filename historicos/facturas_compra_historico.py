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

# Configurar logging
def setup_logging():
    """Configurar el sistema de logging"""
    
    # Crear carpeta de logs si no existe
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Nombre del archivo de log con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/facturas_compra_historico_{timestamp}.log"
    
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

def obtener_facturas_compra_por_fecha(encoded_credentials, fecha_str, logger):
    """Obtener facturas de compra de una fecha específica desde Alegra"""
    try:
        url = f"https://api.alegra.com/api/v1/bills?date={fecha_str}"
        
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {encoded_credentials}"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"Fecha {fecha_str}: {len(data)} facturas de compra obtenidas")
            return data
        elif response.status_code == 429:
            logger.warning(f"Rate limit alcanzado para fecha {fecha_str}, esperando...")
            # Esperar un poco antes de continuar
            import time
            time.sleep(2)
            return obtener_facturas_compra_por_fecha(encoded_credentials, fecha_str, logger)
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
    logger.info("INICIO DEL PROCESO HISTÓRICO DE FACTURAS DE COMPRA ALEGRA")
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
        
        # Configuración de fechas
        fecha_inicio = os.getenv("FECHA_INICIO", "2024-01-01")  # Se puede configurar en .env
        fecha_fin = os.getenv("FECHA_FIN")  # Si no está definida, usa fecha actual
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista Facturas: {list_name_facturas_compra}")
        logger.info(f"Lista Categorías: {list_name_categorias_compra}")
        logger.info(f"Lista Retenciones: {list_name_retenciones_compra}")
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
        todas_las_categorias = []
        todas_las_retenciones = []
        
        # Contadores globales
        total_facturas_obtenidas = 0
        fechas_exitosas = 0
        fechas_con_error = 0
        
        # Procesar cada fecha
        logger.info("Iniciando procesamiento por fechas...")
        
        for i, fecha in enumerate(fechas, 1):
            try:
                logger.info(f"Procesando fecha {i}/{len(fechas)}: {fecha}")
                
                # Obtener facturas de compra de esta fecha
                facturas_fecha = obtener_facturas_compra_por_fecha(encoded_credentials, fecha, logger)
                
                if facturas_fecha:
                    logger.info(f"  → {len(facturas_fecha)} facturas de compra encontradas")
                    
                    # Procesar facturas de esta fecha
                    facturas_procesadas, categorias_procesadas, retenciones_procesadas = procesar_facturas_compra_fecha(facturas_fecha, fecha, logger)
                    
                    # Acumular resultados
                    todas_las_facturas.extend(facturas_procesadas)
                    todas_las_categorias.extend(categorias_procesadas)
                    todas_las_retenciones.extend(retenciones_procesadas)
                    
                    total_facturas_obtenidas += len(facturas_fecha)
                    fechas_exitosas += 1
                    
                    logger.info(f"  → Procesadas {len(facturas_procesadas)} facturas, {len(categorias_procesadas)} categorías, {len(retenciones_procesadas)} retenciones")
                else:
                    logger.info(f"  → Sin facturas de compra para {fecha}")
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
        logger.info(f"Total facturas de compra obtenidas: {total_facturas_obtenidas}")
        logger.info(f"Total facturas procesadas: {len(todas_las_facturas)}")
        logger.info(f"Total categorías procesadas: {len(todas_las_categorias)}")
        logger.info(f"Total retenciones procesadas: {len(todas_las_retenciones)}")
        
        if not todas_las_facturas:
            logger.warning("No se encontraron facturas de compra para procesar")
            return True
        
        # Crear DataFrames
        logger.info("Creando DataFrames finales...")
        df_facturas = pd.DataFrame(todas_las_facturas)
        df_categorias = pd.DataFrame(todas_las_categorias)
        df_retenciones = pd.DataFrame(todas_las_retenciones)
        
        logger.info(f"DataFrames creados - Facturas: {len(df_facturas)}, Categorías: {len(df_categorias)}, Retenciones: {len(df_retenciones)}")
        
        # Subir a SharePoint
        logger.info("INICIANDO SUBIDA A LISTAS DE SHAREPOINT")
        
        # Subir a listas de SharePoint (en lotes para evitar timeouts)
        success_listas = subir_facturas_compra_en_lotes(df_facturas, df_categorias, df_retenciones, site_url, 
                                                       list_name_facturas_compra, list_name_categorias_compra, 
                                                       list_name_retenciones_compra, logger)
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO HISTÓRICO")
        logger.info("="*60)
        logger.info(f"Período procesado: {fecha_inicio} a {fechas[-1]}")
        logger.info(f"Fechas exitosas: {fechas_exitosas}/{len(fechas)}")
        logger.info(f"Facturas procesadas: {len(df_facturas)}")
        logger.info(f"Categorías procesadas: {len(df_categorias)}")
        logger.info(f"Retenciones procesadas: {len(df_retenciones)}")
        logger.info(f"Datos subidos a listas: {'SI' if success_listas else 'NO'}")
        logger.info(f"Archivo de log: {log_file}")
        
        # Mostrar en consola
        print(f"Proceso histórico completado:")
        print(f"  Período: {fecha_inicio} a {fechas[-1]}")
        print(f"  Facturas: {len(df_facturas)}, Categorías: {len(df_categorias)}, Retenciones: {len(df_retenciones)}")
        print(f"  Log: {log_file}")
        
        return success_listas
        
    except Exception as e:
        logger.error(f"Error crítico en el proceso principal: {str(e)}")
        logger.error("Detalles del error:", exc_info=True)
        print(f"ERROR: {str(e)}. Ver detalles en: {log_file}")
        return False

def procesar_facturas_compra_fecha(facturas_fecha, fecha, logger):
    """Procesar facturas de compra de una fecha específica"""
    facturas_procesadas = []
    categorias_procesadas = []
    retenciones_procesadas = []
    
    for i, bill in enumerate(facturas_fecha):
        try:
            if bill is None:
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
            
            facturas_procesadas.append(factura_data)
            
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
                        categorias_procesadas.append(categoria_data)
            
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
                        retenciones_procesadas.append(retencion_data)
            
        except Exception as e:
            logger.error(f"Error procesando factura de compra {i+1} de fecha {fecha}: {str(e)}")
            continue
    
    return facturas_procesadas, categorias_procesadas, retenciones_procesadas

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

def subir_facturas_compra_en_lotes(df_facturas, df_categorias, df_retenciones, site_url, 
                                  list_name_facturas, list_name_categorias, list_name_retenciones, 
                                  logger, lote_size=50):
    """Subir facturas de compra a SharePoint en lotes para evitar timeouts"""
    try:
        logger.info(f"Iniciando subida en lotes a SharePoint (tamaño lote: {lote_size})...")
        
        sp_connector = SharePointConnector()
        
        total_facturas = len(df_facturas)
        facturas_exitosas = 0
        facturas_error = 0
        categorias_exitosas = 0
        categorias_error = 0
        retenciones_exitosas = 0
        retenciones_error = 0
        
        # Procesar en lotes
        for inicio in range(0, total_facturas, lote_size):
            fin = min(inicio + lote_size, total_facturas)
            lote_actual = inicio // lote_size + 1
            total_lotes = (total_facturas + lote_size - 1) // lote_size
            
            logger.info(f"Procesando lote {lote_actual}/{total_lotes}: facturas {inicio+1} a {fin}")
            
            lote_facturas = df_facturas.iloc[inicio:fin]
            
            for index, factura_row in lote_facturas.iterrows():
                try:
                    numero_factura = factura_row['Numero_Factura']
                    factura_id_alegra = factura_row['ID_Factura']
                    
                    datos_factura = factura_row.to_dict()
                    factura_sharepoint_id = send_factura_compra_sharepoint(sp_connector, datos_factura, site_url, list_name_facturas, logger)
                    
                    if factura_sharepoint_id:
                        facturas_exitosas += 1
                        
                        # Procesar categorías de esta factura
                        factura_categorias = df_categorias[df_categorias['Numero_Factura'] == numero_factura]
                        if not factura_categorias.empty:
                            for _, categoria_row in factura_categorias.iterrows():
                                categoria_dict = categoria_row.to_dict()
                                categoria_id = send_categoria_compra_sharepoint(
                                    sp_connector, categoria_dict, factura_sharepoint_id, site_url, list_name_categorias, logger
                                )
                                if categoria_id:
                                    categorias_exitosas += 1
                                else:
                                    categorias_error += 1
                        
                        # Procesar retenciones de esta factura
                        factura_retenciones = df_retenciones[df_retenciones['Factura_de_Compra'] == numero_factura]
                        if not factura_retenciones.empty:
                            for _, retencion_row in factura_retenciones.iterrows():
                                retencion_dict = retencion_row.to_dict()
                                retencion_id = send_retencion_compra_sharepoint(
                                    sp_connector, retencion_dict, factura_sharepoint_id, site_url, list_name_retenciones, logger
                                )
                                if retencion_id:
                                    retenciones_exitosas += 1
                                else:
                                    retenciones_error += 1
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
        logger.info(f"Categorías exitosas: {categorias_exitosas}")
        logger.info(f"Categorías con errores: {categorias_error}")
        logger.info(f"Retenciones exitosas: {retenciones_exitosas}")
        logger.info(f"Retenciones con errores: {retenciones_error}")
        
        return facturas_exitosas > 0
        
    except Exception as e:
        logger.error(f"Error en subida en lotes: {str(e)}")
        return False

# Usar las mismas funciones del script original que ya funcionan
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
        
        response = requests.post(url, headers=headers, json=item_data)
        
        if response.status_code == 201:
            created_item = response.json()
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