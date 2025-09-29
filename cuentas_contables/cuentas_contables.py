import requests
import base64
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sharepoint_connector import SharePointConnector

def setup_logging():
    """Configurar el sistema de logging"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/cuentas_contables_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    console_handler = logging.getLogger().handlers[1]
    console_handler.setLevel(logging.INFO)  # Cambiado a INFO para ver más detalles
    
    return log_filename

def obtener_cuentas_contables_alegra(encoded_credentials, logger):
    """Obtener todas las cuentas contables en formato plano desde Alegra"""
    try:
        url = "https://api.alegra.com/api/v1/categories?format=plain"
        
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {encoded_credentials}"
        }
        
        logger.info("Consultando API de Alegra para cuentas contables (formato plano)...")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Obtenidas {len(data)} cuentas contables de Alegra")
            return data
        else:
            logger.error(f"Error en API Alegra: {response.status_code} - {response.text}")
            return None
    
    except Exception as e:
        logger.error(f"Error consultando API Alegra: {str(e)}")
        return None

def analizar_estructura_cuentas(cuentas, logger):
    """Analizar la estructura de las cuentas contables"""
    try:
        tipos = {}
        cuentas_raiz = 0
        max_nivel = 0
        
        for cuenta in cuentas:
            tipo = cuenta.get('type', 'unknown')
            tipos[tipo] = tipos.get(tipo, 0) + 1
            
            if not cuenta.get('idParent'):
                cuentas_raiz += 1
            
            # Calcular nivel
            nivel = 0
            parent_id = cuenta.get('idParent')
            while parent_id:
                nivel += 1
                parent_cuenta = next((c for c in cuentas if c['id'] == parent_id), None)
                parent_id = parent_cuenta.get('idParent') if parent_cuenta else None
            
            max_nivel = max(max_nivel, nivel)
        
        logger.info("="*60)
        logger.info("ANÁLISIS DE ESTRUCTURA DE CUENTAS CONTABLES")
        logger.info("="*60)
        logger.info(f"Total de cuentas: {len(cuentas)}")
        logger.info(f"Cuentas raíz (sin padre): {cuentas_raiz}")
        logger.info(f"Máximo nivel de profundidad: {max_nivel}")
        logger.info("\nDistribución por tipo:")
        for tipo, cantidad in sorted(tipos.items()):
            logger.info(f"  {tipo}: {cantidad}")
        
    except Exception as e:
        logger.error(f"Error analizando estructura: {str(e)}")

def subir_cuentas_contables_sharepoint(cuentas, site_url, list_name, logger):
    """Subir cuentas contables a SharePoint en dos pasadas"""
    try:
        logger.info("\nIniciando subida a SharePoint en dos pasadas...")
        
        sp_connector = SharePointConnector()
        
        # Mapa para guardar IDs de SharePoint
        sharepoint_id_map = {}
        
        # PRIMERA PASADA: Subir cuentas raíz (sin padre)
        logger.info("\n" + "="*60)
        logger.info("PRIMERA PASADA: Subiendo cuentas raíz (sin padre)")
        logger.info("="*60)
        cuentas_raiz = [c for c in cuentas if not c.get('idParent')]
        
        for i, cuenta_data in enumerate(cuentas_raiz):
            try:
                nombre_cuenta = cuenta_data.get('name', f"Cuenta-{cuenta_data.get('id')}")
                alegra_id = cuenta_data.get('id')
                logger.info(f"[{i + 1}/{len(cuentas_raiz)}] Subiendo: {nombre_cuenta} (ID: {alegra_id})")
                
                sp_id = send_cuenta_contable_sharepoint(
                    sp_connector, cuenta_data, None, site_url, list_name, logger
                )
                
                if sp_id:
                    sharepoint_id_map[alegra_id] = sp_id
                    logger.info(f"  ✓ Creado con ID SharePoint: {sp_id}")
                else:
                    logger.error(f"  ✗ Falló la creación")
                    
            except Exception as e:
                logger.error(f"  ✗ Error: {str(e)}")
                continue
        
        # SEGUNDA PASADA: Subir cuentas con padre
        logger.info("\n" + "="*60)
        logger.info("SEGUNDA PASADA: Subiendo cuentas con padre")
        logger.info("="*60)
        cuentas_con_padre = [c for c in cuentas if c.get('idParent')]
        
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        for i, cuenta_data in enumerate(cuentas_con_padre):
            try:
                nombre_cuenta = cuenta_data.get('name', f"Cuenta-{cuenta_data.get('id')}")
                alegra_id = cuenta_data.get('id')
                parent_alegra_id = cuenta_data.get('idParent')
                
                # Buscar el ID de SharePoint del padre
                parent_sp_id = sharepoint_id_map.get(parent_alegra_id)
                
                if not parent_sp_id:
                    logger.warning(f"[{i + 1}/{len(cuentas_con_padre)}] Saltando {nombre_cuenta}: padre {parent_alegra_id} no encontrado")
                    skipped_count += 1
                    continue
                
                logger.info(f"[{i + 1}/{len(cuentas_con_padre)}] Subiendo: {nombre_cuenta} (Padre SP: {parent_sp_id})")
                
                sp_id = send_cuenta_contable_sharepoint(
                    sp_connector, cuenta_data, parent_sp_id, site_url, list_name, logger
                )
                
                if sp_id:
                    sharepoint_id_map[alegra_id] = sp_id
                    success_count += 1
                    logger.info(f"  ✓ Creado con ID SharePoint: {sp_id}")
                else:
                    error_count += 1
                    logger.error(f"  ✗ Falló la creación")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"  ✗ Error: {str(e)}")
                continue
        
        logger.info("\n" + "="*60)
        logger.info("RESUMEN DE SUBIDA:")
        logger.info("="*60)
        logger.info(f"Cuentas raíz creadas: {len(sharepoint_id_map) - success_count}")
        logger.info(f"Cuentas con padre exitosas: {success_count}")
        logger.info(f"Errores: {error_count}")
        logger.info(f"Saltados (padre no encontrado): {skipped_count}")
        logger.info(f"Total en SharePoint: {len(sharepoint_id_map)}")
        logger.info("="*60)
        
        return len(sharepoint_id_map) > 0
        
    except Exception as e:
        logger.error(f"Error durante subida a SharePoint: {str(e)}")
        return False

def send_cuenta_contable_sharepoint(sp_connector, cuenta_data, parent_sp_id, site_url, list_name, logger):
    """Subir una cuenta contable individual a SharePoint"""
    try:
        token = sp_connector.get_azure_token()
        site_id = sp_connector.get_site_id(token, site_url)
        list_id = sp_connector.get_list_id(token, site_id, list_name)
        
        if not list_id:
            logger.error(f"No se pudo obtener el ID de la lista {list_name}")
            return None
        
        # Preparar datos básicos
        sharepoint_data = {
            'fields': {
                "Title": str(cuenta_data.get("id", "")),
                "ID_x0020_Global": str(cuenta_data.get("idGlobal", "")),
                "Codigo": str(cuenta_data.get("code") or ""),
                "Nombre": str(cuenta_data.get("name", "")),
                "Texto": str(cuenta_data.get("text", "")),
                "Tipo_x0020_Cuenta_x0020_Contable": str(cuenta_data.get("type", "")),
                "Estado": str(cuenta_data.get("status", "")),
                "Bloqueado": str(cuenta_data.get("blocked", "")),
                "Naturaleza": str(cuenta_data.get("nature", "")),
                "Uso": str(cuenta_data.get("use", "")),
                "Mostrar_x0020_Saldo_x0020_por_x0": str(cuenta_data.get("showThirdPartyBalance", False)),
            }
        }
        
        # Agregar descripción si existe
        descripcion = cuenta_data.get("description")
        if descripcion:
            sharepoint_data['fields']["Descripcion"] = str(descripcion)
        
        # Agregar Regla de Categoría si existe
        category_rule = cuenta_data.get("categoryRule")
        if category_rule and isinstance(category_rule, dict):
            regla_nombre = category_rule.get("name", "")
            if regla_nombre:
                sharepoint_data['fields']["Regla_x0020_de_x0020_Categoria"] = str(regla_nombre)
        
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # SI NO HAY PADRE, crear el registro directamente
        if not parent_sp_id:
            response = requests.post(url, headers=headers, json=sharepoint_data)
            
            if response.status_code == 201:
                created_item = response.json()
                item_id = created_item.get('id')
                return item_id
            else:
                logger.error(f"    Error HTTP {response.status_code}: {response.text}")
                return None
        
        # SI HAY PADRE, probar diferentes variaciones del nombre del campo Lookup
        lookup_variations = [
            "ID_x0020_PadreLookupId",
            "ID_x0020_Padre",
            "IDPadreLookupId",
            "ID_PadreLookupId",
        ]
        
        logger.debug(f"    Intentando asignar padre SharePoint ID: {parent_sp_id}")
        
        for lookup_field in lookup_variations:
            # Crear una copia de los datos
            data_with_lookup = sharepoint_data.copy()
            data_with_lookup['fields'] = sharepoint_data['fields'].copy()
            
            # Agregar el campo lookup con esta variación
            data_with_lookup['fields'][lookup_field] = str(parent_sp_id)
            
            logger.debug(f"    Probando campo: {lookup_field}")
            
            response = requests.post(url, headers=headers, json=data_with_lookup)
            
            if response.status_code == 201:
                created_item = response.json()
                item_id = created_item.get('id')
                logger.info(f"    ✓ Padre asignado exitosamente usando campo: {lookup_field}")
                return item_id
            else:
                logger.debug(f"    Falló con {lookup_field}: {response.status_code}")
                continue
        
        # Si ninguna variación funcionó, loggear el último error
        logger.error(f"    Error: No se pudo asignar padre con ninguna variación del campo")
        logger.error(f"    Último error HTTP {response.status_code}: {response.text}")
        return None
        
    except Exception as e:
        logger.error(f"    Error en subida: {str(e)}")
        return None

def main():
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("PROCESO DE CUENTAS CONTABLES ALEGRA → SHAREPOINT")
    logger.info("="*60)
    
    try:
        load_dotenv()
        logger.info("Variables de entorno cargadas")
        
        username = os.getenv("email")
        password = os.getenv("password")
        site_url = os.getenv("site_url")
        list_name_cuentas = os.getenv("list_cuentas_contables", "Cuentas Contables")
        
        logger.info(f"Site URL: {site_url}")
        logger.info(f"Lista destino: {list_name_cuentas}")
        
        if not username or not password or not site_url:
            logger.error("Faltan credenciales o configuración")
            return False
        
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Obtener datos
        cuentas = obtener_cuentas_contables_alegra(encoded_credentials, logger)
        
        if not cuentas:
            logger.error("No se pudieron obtener cuentas contables")
            return False
        
        # Analizar
        analizar_estructura_cuentas(cuentas, logger)
        
        # Subir
        success = subir_cuentas_contables_sharepoint(
            cuentas, site_url, list_name_cuentas, logger
        )
        
        logger.info("\n" + "="*60)
        logger.info("PROCESO FINALIZADO")
        logger.info("="*60)
        logger.info(f"Resultado: {'EXITOSO' if success else 'FALLÓ'}")
        logger.info(f"Log: {log_file}")
        
        print(f"\nProceso completado. Ver log en: {log_file}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}")
        logger.error("Detalles:", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)