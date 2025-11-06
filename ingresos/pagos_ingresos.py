import requests
import base64
import os
import sys
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

# Agregar el directorio padre al path para importaciones
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sharepoint_connector import SharePointConnector

# Configurar logging
def setup_logging():
    """Configurar el sistema de logging"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    log_filename = f"logs/pagos_alegra_{datetime.now().strftime('%Y-%m-%d')}.log"
    
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
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("INICIO DEL PROCESO DE PAGOS ALEGRA CON ANTICIPOS")
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
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista destino: {list_name_pagos}")
        
        # Verificar credenciales
        if not username or not password:
            logger.error("Credenciales de Alegra no encontradas")
            return False
            
        if not site_url:
            logger.error("URL de SharePoint no encontrada")
            return False
        
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Fecha de consulta (ayer)
        ayer = date.today() - timedelta(days=1)
        ayer_str = ayer.strftime('%Y-%m-%d')
        logger.info(f"Procesando pagos del día: {ayer_str}")
        
        # Obtener datos de Alegra
        logger.info("Consultando API de Alegra...")
        data = obtener_pagos_alegra(encoded_credentials, ayer_str, logger)
        
        if data is None:
            logger.error("No se pudieron obtener datos de Alegra")
            return False
        
        logger.info(f"Obtenidos {len(data)} pagos de Alegra")
        
        # Si no hay pagos, es un éxito, no un error
        if len(data) == 0:
            logger.info("No se encontraron pagos para la fecha especificada - PROCESO EXITOSO")
            print(f"Sin pagos para {ayer_str}. Log en: {log_file}")
            return True
        
        # Procesar datos en estructura unificada (AHORA INCLUYE ANTICIPOS)
        logger.info("Procesando datos en estructura unificada con anticipos...")
        pagos_unificados = procesar_pagos_unificado_con_anticipos(data, logger)
        
        logger.info(f"Generados {len(pagos_unificados)} registros unificados (incluyendo anticipos)")
        
        # Si hay pagos para procesar, subirlos a SharePoint
        if len(pagos_unificados) > 0:
            logger.info("Iniciando subida a SharePoint...")
            success = subir_pagos_sharepoint(pagos_unificados, site_url, list_name_pagos, logger)
        else:
            logger.warning("Se encontraron pagos pero todos tuvieron errores de procesamiento")
            success = False
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN FINAL DEL PROCESO CON ANTICIPOS")
        logger.info("="*60)
        logger.info(f"Pagos originales procesados: {len(data)}")
        logger.info(f"Registros unificados generados: {len(pagos_unificados)}")
        
        if len(pagos_unificados) == 0:
            logger.info("Sin pagos para procesar - PROCESO EXITOSO")
            print(f"Sin pagos para {ayer_str}. Log en: {log_file}")
            final_success = True
        else:
            logger.info(f"Subida a SharePoint: {'EXITOSA' if success else 'FALLÓ'}")
            print(f"Proceso completado. Registros: {len(pagos_unificados)}")
            print(f"Log guardado en: {log_file}")
            final_success = success
        
        logger.info(f"Archivo de log: {log_file}")
        
        return final_success
        
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}")
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

def obtener_pagos_alegra(encoded_credentials, fecha_str, logger):
    """Obtener pagos desde la API de Alegra"""
    try:
        url = f"https://api.alegra.com/api/v1/payments?order_direction=DESC&metadata=false&includeUnconciliated=false&date={fecha_str}"
        
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {encoded_credentials}"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"API respondió exitosamente con {len(data)} pagos")
            return data
        else:
            logger.error(f"Error en API Alegra: {response.status_code} - {response.text}")
            return None
    
    except Exception as e:
        logger.error(f"Error consultando API Alegra: {str(e)}")
        return None

def procesar_pagos_unificado_con_anticipos(data, logger):
    """Procesar los datos de pagos en estructura unificada INCLUYENDO ANTICIPOS APLICADOS"""
    
    pagos_unificados = []
    pagos_procesados = 0
    pagos_con_error = 0
    anticipos_encontrados = 0
    
    for i, payment in enumerate(data):
        if payment is None:
            logger.warning(f"Pago {i+1} es None, saltando...")
            pagos_con_error += 1
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
                
                # NUEVO: Campos para anticipos aplicados (vacíos por defecto)
                'Anticipo_ID': '',
                'Anticipo_Numero': '',
                'Anticipo_Fecha': None,
                'Anticipo_Fecha_Vencimiento': None,
                'Anticipo_Monto_Aplicado': 0,
                'Anticipo_Total_Factura': 0,
                'Anticipo_Total_Pagado_Factura': 0,
                'Anticipo_Saldo_Factura': 0,
                
                # Tipo de registro
                'Tipo_Registro': 'PAGO_SIMPLE'
            }
            
            # Verificar facturas, categorías y anticipos
            invoices = payment.get('invoices', [])
            categories = payment.get('categories', [])
            applied_advances = payment.get('appliedAdvances', [])  # NUEVO: Anticipos aplicados
            
            # Contar anticipos encontrados para estadísticas
            if applied_advances:
                anticipos_encontrados += len(applied_advances)
                logger.debug(f"Pago {pago_base['Numero_Pago']}: Encontrados {len(applied_advances)} anticipos aplicados")
            
            if not invoices and not categories and not applied_advances:
                # Pago simple sin relaciones
                pagos_unificados.append(pago_base)
                logger.debug(f"Pago simple procesado: {pago_base['Numero_Pago']}")
                
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
                            logger.debug(f"Pago con factura procesado: {pago_base['Numero_Pago']} -> {invoice.get('number')}")
                
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
                            logger.debug(f"Pago con categoría procesado: {pago_base['Numero_Pago']} -> {category.get('name')}")
                
                # NUEVO: Si tiene anticipos aplicados
                if applied_advances:
                    for advance in applied_advances:
                        if advance is not None:
                            pago_con_anticipo = pago_base.copy()
                            pago_con_anticipo.update({
                                'Anticipo_ID': advance.get('id'),
                                'Anticipo_Numero': advance.get('number'),
                                'Anticipo_Fecha': advance.get('date'),
                                'Anticipo_Fecha_Vencimiento': advance.get('dueDate'),
                                'Anticipo_Monto_Aplicado': advance.get('amount', 0),
                                'Anticipo_Total_Factura': advance.get('total', 0),
                                'Anticipo_Total_Pagado_Factura': advance.get('totalPaid', 0),
                                'Anticipo_Saldo_Factura': advance.get('balance', 0),
                                'Tipo_Registro': 'PAGO_CON_ANTICIPO'  # NUEVO tipo de registro
                            })
                            pagos_unificados.append(pago_con_anticipo)
                            logger.debug(f"Pago con anticipo procesado: {pago_base['Numero_Pago']} -> {advance.get('number')} (${advance.get('amount', 0)})")
            
            pagos_procesados += 1
            
        except Exception as e:
            logger.error(f"Error procesando pago {i+1}: {str(e)}")
            pagos_con_error += 1
            continue
    
    logger.info(f"Procesamiento completado: {pagos_procesados} pagos exitosos, {pagos_con_error} con errores")
    logger.info(f"Total registros unificados generados: {len(pagos_unificados)}")
    logger.info(f"Total anticipos aplicados encontrados: {anticipos_encontrados}")
    
    # Mostrar estadísticas de tipos de registro
    tipos_registro = {}
    for pago in pagos_unificados:
        tipo = pago.get('Tipo_Registro', 'DESCONOCIDO')
        tipos_registro[tipo] = tipos_registro.get(tipo, 0) + 1
    
    logger.info("Estadísticas de tipos de registro:")
    for tipo, cantidad in tipos_registro.items():
        logger.info(f"  - {tipo}: {cantidad}")
    
    return pagos_unificados

def subir_pagos_sharepoint(pagos_unificados, site_url, list_name, logger):
    """Subir pagos unificados a SharePoint"""
    try:
        logger.info("Inicializando conexión a SharePoint...")
        sp_connector = SharePointConnector()
        
        success_count = 0
        error_count = 0
        
        for i, pago_data in enumerate(pagos_unificados):
            try:
                numero_pago = pago_data.get('Numero_Pago', f"ID-{pago_data.get('Pago_ID')}")
                tipo_registro = pago_data.get('Tipo_Registro', 'DESCONOCIDO')
                logger.info(f"Subiendo registro {i + 1}/{len(pagos_unificados)}: {numero_pago} ({tipo_registro})")
                
                result = send_pago_unificado_sharepoint(sp_connector, pago_data, site_url, list_name, logger)
                
                if result:
                    success_count += 1
                    logger.debug(f"Registro {numero_pago} subido con ID: {result}")
                else:
                    error_count += 1
                    logger.error(f"Error subiendo registro {numero_pago}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error procesando registro {i + 1}: {str(e)}")
                continue
        
        logger.info(f"Subida completada: {success_count} exitosos, {error_count} errores")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error durante subida a SharePoint: {str(e)}")
        return False

def send_pago_unificado_sharepoint(sp_connector, pago_data, site_url, list_name, logger):
    """Subir un registro unificado de pago a SharePoint - VERSIÓN CORREGIDA CON LÓGICA CONDICIONAL"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener ID de la lista {list_name}")
            return None
        
        # VERIFICAR EL TIPO DE REGISTRO PRIMERO
        tipo_registro = pago_data.get("Tipo_Registro", "")
        
        if tipo_registro == "PAGO_CON_ANTICIPO":
            # SOLO campos para anticipos
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
                    "Nombre_x0020_Categoria": "Avances y anticipos recibidos",
                    # Campos específicos de anticipo
                    "ID_x0020_Anticipo": pago_data.get("Anticipo_ID", ""),
                    "Anticipo_x0020_Numero": pago_data.get("Anticipo_Numero", ""),
                    "Anticipo_x0020_Total_x0020_Factu": pago_data.get("Anticipo_Total_Factura", 0),
                    "Anticipo_x0020_Total_x0020_Pagad": pago_data.get("Anticipo_Total_Pagado_Factura", 0),
                    "Anticipo_x0020_Saldo_x0020_Factu": pago_data.get("Anticipo_Saldo_Factura", 0),
                    "Anticipo_x0020_Monto_x0020_Aplic": pago_data.get("Anticipo_Monto_Aplicado", 0),
                    "Anticipo": True  # Campo Sí/No - siempre True para anticipos
                }
            }
            
            # Solo agregar fechas de anticipo si tienen valor válido
            fecha_anticipo = pago_data.get("Anticipo_Fecha")
            if fecha_anticipo and str(fecha_anticipo).strip():
                item_data['fields']["Anticipo_x0020_Fecha"] = fecha_anticipo
                
            fecha_venc_anticipo = pago_data.get("Anticipo_Fecha_Vencimiento")
            if fecha_venc_anticipo and str(fecha_venc_anticipo).strip():
                item_data['fields']["Anticipo_x0020_Fecha_x0020_Venci"] = fecha_venc_anticipo
        
        else:
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
                    "Anticipo": False  # Campo Sí/No - siempre False para no-anticipos
                }
            }
            
            # Solo agregar fecha de factura si tiene valor válido (para pagos con facturas normales)
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