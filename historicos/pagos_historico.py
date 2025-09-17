import requests
import base64
import os
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from ..core.sharepoint_connector import SharePointConnector

# Configurar logging
def setup_logging():
    """Configurar el sistema de logging"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/pagos_historico_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # Mostrar INFO y errores en consola
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

def obtener_pagos_por_fecha(encoded_credentials, fecha_str, logger):
    """Obtener pagos de una fecha específica desde Alegra"""
    try:
        url = f"https://api.alegra.com/api/v1/payments?order_direction=DESC&metadata=false&includeUnconciliated=false&date={fecha_str}"
        
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {encoded_credentials}"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"Fecha {fecha_str}: {len(data)} pagos obtenidos")
            return data
        elif response.status_code == 429:
            logger.warning(f"Rate limit alcanzado para fecha {fecha_str}, esperando...")
            # Esperar un poco antes de continuar
            import time
            time.sleep(2)
            return obtener_pagos_por_fecha(encoded_credentials, fecha_str, logger)
        else:
            logger.error(f"Error consultando fecha {fecha_str}: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        logger.error(f"Error consultando fecha {fecha_str}: {str(e)}")
        return []

def main():
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("INICIO DEL PROCESO HISTÓRICO DE PAGOS ALEGRA")
    logger.info("="*60)
    
    try:
        # Cargar configuración
        load_dotenv()
        logger.info("Variables de entorno cargadas")
        
        # Credenciales Alegra
        username = os.getenv("email")
        password = os.getenv("password")
        
        # Configuración SharePoint
        site_url = os.getenv("site_url")
        list_name_pagos = os.getenv("list_pagos")
        
        # Configuración de fechas
        fecha_inicio = os.getenv("FECHA_INICIO", "2024-01-01")  # Se puede configurar en .env
        fecha_fin = os.getenv("FECHA_FIN")  # Si no está definida, usa fecha actual
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista destino: {list_name_pagos}")
        logger.info(f"Fecha inicio: {fecha_inicio}")
        logger.info(f"Fecha fin: {fecha_fin or 'Fecha actual'}")
        
        # Verificar credenciales
        if not username or not password:
            logger.error("Credenciales de Alegra no encontradas")
            return False
            
        if not site_url:
            logger.error("URL de SharePoint no encontrada")
            return False
        
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Generar rango de fechas
        logger.info("Generando rango de fechas...")
        fechas = generar_rango_fechas(fecha_inicio, fecha_fin)
        logger.info(f"Total de fechas a procesar: {len(fechas)}")
        logger.info(f"Desde: {fechas[0]} hasta: {fechas[-1]}")
        
        # Listas para acumular todos los datos
        todos_los_pagos = []
        
        # Contadores globales
        total_pagos_obtenidos = 0
        fechas_exitosas = 0
        fechas_con_error = 0
        
        # Procesar cada fecha
        logger.info("Iniciando procesamiento por fechas...")
        
        for i, fecha in enumerate(fechas, 1):
            try:
                logger.info(f"Procesando fecha {i}/{len(fechas)}: {fecha}")
                
                # Obtener pagos de esta fecha
                pagos_fecha = obtener_pagos_por_fecha(encoded_credentials, fecha, logger)
                
                if pagos_fecha:
                    logger.info(f"  → {len(pagos_fecha)} pagos encontrados")
                    
                    # Procesar pagos de esta fecha
                    pagos_procesados = procesar_pagos_fecha(pagos_fecha, fecha, logger)
                    
                    # Acumular resultados
                    todos_los_pagos.extend(pagos_procesados)
                    
                    total_pagos_obtenidos += len(pagos_fecha)
                    fechas_exitosas += 1
                    
                    logger.info(f"  → Procesados {len(pagos_procesados)} registros unificados")
                else:
                    logger.info(f"  → Sin pagos para {fecha}")
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
        logger.info(f"Total pagos obtenidos: {total_pagos_obtenidos}")
        logger.info(f"Total registros unificados generados: {len(todos_los_pagos)}")
        
        if not todos_los_pagos:
            logger.warning("No se encontraron pagos para procesar")
            return True
        
        # Subir a SharePoint en lotes
        logger.info("INICIANDO SUBIDA A SHAREPOINT EN LOTES")
        success = subir_pagos_en_lotes(todos_los_pagos, site_url, list_name_pagos, logger)
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO HISTÓRICO")
        logger.info("="*60)
        logger.info(f"Período procesado: {fecha_inicio} a {fechas[-1]}")
        logger.info(f"Fechas exitosas: {fechas_exitosas}/{len(fechas)}")
        logger.info(f"Registros procesados: {len(todos_los_pagos)}")
        logger.info(f"Subida a SharePoint: {'EXITOSA' if success else 'FALLÓ'}")
        logger.info(f"Archivo de log: {log_file}")
        
        # Mostrar en consola
        print(f"Proceso histórico completado:")
        print(f"  Período: {fecha_inicio} a {fechas[-1]}")
        print(f"  Registros: {len(todos_los_pagos)}")
        print(f"  Log: {log_file}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}")
        logger.error("Detalles del error:", exc_info=True)
        print(f"ERROR: {str(e)}. Ver detalles en: {log_file}")
        return False

def procesar_pagos_fecha(pagos_fecha, fecha, logger):
    """Procesar pagos de una fecha específica"""
    pagos_unificados = []
    
    for i, payment in enumerate(pagos_fecha):
        if payment is None:
            continue
        
        try:
            # Registro base del pago
            pago_base = {
                # Datos principales del pago
                'Pago_ID': payment.get('id'),
                'Fecha': payment.get('date'),
                'Numero_Pago': safe_get_nested(payment, 'numberTemplate', 'fullNumber', default=''),
                'Numero_Interno': payment.get('number'),
                'Monto_Total': payment.get('amount', 0),
                'Tipo_Pago': payment.get('type'),
                'Metodo_Pago': payment.get('paymentMethod'),
                'Estado_Pago': payment.get('status'),
                'Observaciones_Pago': payment.get('observations', ''),
                'Anotaciones_Pago': payment.get('anotation', ''),
                
                # Cuenta bancaria
                'Cuenta_ID': safe_get_nested(payment, 'bankAccount', 'id', default=''),
                'Cuenta_Nombre': safe_get_nested(payment, 'bankAccount', 'name', default=''),
                'Cuenta_Tipo': safe_get_nested(payment, 'bankAccount', 'type', default=''),
                
                # Cliente
                'Cliente_ID': safe_get_nested(payment, 'client', 'id', default=''),
                'Cliente_Nombre': safe_get_nested(payment, 'client', 'name', default=''),
                'Cliente_Telefono': safe_get_nested(payment, 'client', 'phone', default=''),
                'Cliente_Identificacion': safe_get_nested(payment, 'client', 'identification', default=''),
                
                # Centro de costo
                'Centro_Costo_ID': safe_get_nested(payment, 'costCenter', 'id', default=''),
                'Centro_Costo_Codigo': safe_get_nested(payment, 'costCenter', 'code', default=''),
                'Centro_Costo_Nombre': safe_get_nested(payment, 'costCenter', 'name', default=''),
                
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
            
        except Exception as e:
            logger.error(f"Error procesando pago {i+1} de fecha {fecha}: {str(e)}")
            continue
    
    return pagos_unificados

def safe_get_nested(obj, *keys, default=''):
    """Función helper para obtener valores anidados de forma segura"""
    for key in keys:
        if isinstance(obj, dict) and key in obj and obj[key] is not None:
            obj = obj[key]
        else:
            return default
    return obj if obj is not None else default

def subir_pagos_en_lotes(todos_los_pagos, site_url, list_name, logger, lote_size=50):
    """Subir pagos a SharePoint en lotes para evitar timeouts"""
    try:
        logger.info(f"Iniciando subida en lotes a SharePoint (tamaño lote: {lote_size})...")
        
        sp_connector = SharePointConnector()
        
        total_registros = len(todos_los_pagos)
        registros_exitosos = 0
        registros_error = 0
        
        # Procesar en lotes
        for inicio in range(0, total_registros, lote_size):
            fin = min(inicio + lote_size, total_registros)
            lote_actual = inicio // lote_size + 1
            total_lotes = (total_registros + lote_size - 1) // lote_size
            
            logger.info(f"Procesando lote {lote_actual}/{total_lotes}: registros {inicio+1} a {fin}")
            
            lote_pagos = todos_los_pagos[inicio:fin]
            
            for pago_data in lote_pagos:
                try:
                    numero_pago = pago_data.get('Numero_Pago', f"ID-{pago_data.get('Pago_ID')}")
                    
                    result = send_pago_unificado_sharepoint(sp_connector, pago_data, site_url, list_name, logger)
                    
                    if result:
                        registros_exitosos += 1
                    else:
                        registros_error += 1
                        
                except Exception as e:
                    registros_error += 1
                    logger.error(f"Error procesando registro en lote: {str(e)}")
                    continue
            
            # Pausa entre lotes
            if lote_actual < total_lotes:
                logger.info(f"Pausa entre lotes... ({registros_exitosos} exitosos hasta ahora)")
                import time
                time.sleep(2)
        
        logger.info("RESUMEN DE SUBIDA EN LOTES:")
        logger.info(f"Registros exitosos: {registros_exitosos}")
        logger.info(f"Registros con errores: {registros_error}")
        
        return registros_exitosos > 0
        
    except Exception as e:
        logger.error(f"Error en subida en lotes: {str(e)}")
        return False

def send_pago_unificado_sharepoint(sp_connector, pago_data, site_url, list_name, logger):
    """Subir un registro unificado de pago a SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener ID de la lista {list_name}")
            return None
        
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
        if fecha_factura and fecha_factura.strip():
            item_data['fields']["Fecha_x0020_Factura"] = fecha_factura
        
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
            logger.error(f"Error HTTP subiendo pago: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        logger.error(f"Error subiendo pago unificado: {str(e)}")
        return None

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)