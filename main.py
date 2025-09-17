import os
import sys
import logging
import subprocess
from datetime import datetime
from dotenv import load_dotenv

def setup_logging():
    """Configurar logging para el main"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/main_ingresos_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return log_filename

def ejecutar_script(script_path, nombre_script, logger):
    """Ejecutar un script individual y capturar su resultado"""
    logger.info(f"Iniciando ejecución de {nombre_script}...")
    
    try:
        # Ejecutar el script como subprocess
        result = subprocess.run([
            sys.executable, script_path
        ], 
        capture_output=True, 
        text=True, 
        timeout=1800  # 30 minutos de timeout
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {nombre_script} completado exitosamente")
            if result.stdout:
                logger.info(f"Output: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"❌ {nombre_script} falló con código: {result.returncode}")
            if result.stderr:
                logger.error(f"Error: {result.stderr.strip()}")
            if result.stdout:
                logger.info(f"Output: {result.stdout.strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {nombre_script} excedió el tiempo límite (30 min)")
        return False
    except Exception as e:
        logger.error(f"💥 Error ejecutando {nombre_script}: {str(e)}")
        return False

def main():
    """Función principal que ejecuta todos los scripts de ingresos"""
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    print("="*60)
    print("EJECUTOR PRINCIPAL - PROCESAMIENTO DE INGRESOS")
    print("="*60)
    print(f"Log: {log_file}")
    print()
    
    # Cargar variables de entorno
    load_dotenv()
    logger.info("Variables de entorno cargadas")
    
    # Definir scripts a ejecutar en orden
    scripts_config = [
        {
            'path': 'ingresos/facturas_venta.py',
            'name': 'Facturas de Venta',
            'description': 'Extrae facturas diarias desde Alegra'
        },
        {
            'path': 'ingresos/pagos_ingresos.py', 
            'name': 'Pagos de Ingresos',
            'description': 'Extrae pagos diarios desde Alegra'
        },
        {
            'path': 'ingresos/sincronizador_alegra_sharepoint.py',
            'name': 'Sincronizador',
            'description': 'Sincroniza pagos sin cliente asignado'
        }
    ]
    
    logger.info("INICIO DEL PROCESAMIENTO PRINCIPAL")
    logger.info("="*60)
    
    # Estadísticas
    total_scripts = len(scripts_config)
    scripts_exitosos = 0
    scripts_fallidos = 0
    
    # Ejecutar cada script
    for i, script_config in enumerate(scripts_config, 1):
        script_path = script_config['path']
        script_name = script_config['name']
        script_desc = script_config['description']
        
        print(f"[{i}/{total_scripts}] Ejecutando: {script_name}")
        print(f"    Descripción: {script_desc}")
        
        logger.info(f"[{i}/{total_scripts}] Iniciando {script_name}")
        logger.info(f"Archivo: {script_path}")
        logger.info(f"Descripción: {script_desc}")
        
        # Verificar que el archivo existe
        if not os.path.exists(script_path):
            logger.error(f"❌ Archivo no encontrado: {script_path}")
            print(f"    ❌ ERROR: Archivo no encontrado")
            scripts_fallidos += 1
            continue
        
        # Ejecutar script
        success = ejecutar_script(script_path, script_name, logger)
        
        if success:
            scripts_exitosos += 1
            print(f"    ✅ Completado exitosamente")
        else:
            scripts_fallidos += 1
            print(f"    ❌ Falló - Ver log para detalles")
        
        print()  # Línea en blanco para separar
        logger.info("-" * 40)
    
    # Resumen final
    logger.info("="*60)
    logger.info("RESUMEN FINAL DEL PROCESAMIENTO")
    logger.info("="*60)
    logger.info(f"Total de scripts: {total_scripts}")
    logger.info(f"Scripts exitosos: {scripts_exitosos}")
    logger.info(f"Scripts fallidos: {scripts_fallidos}")
    
    success_rate = (scripts_exitosos / total_scripts) * 100 if total_scripts > 0 else 0
    logger.info(f"Tasa de éxito: {success_rate:.1f}%")
    
    # Mostrar resumen en consola
    print("="*60)
    print("RESUMEN FINAL")
    print("="*60)
    print(f"Scripts ejecutados: {total_scripts}")
    print(f"Exitosos: {scripts_exitosos}")
    print(f"Fallidos: {scripts_fallidos}")
    print(f"Tasa de éxito: {success_rate:.1f}%")
    print(f"Log detallado: {log_file}")
    
    # Determinar código de salida
    if scripts_fallidos == 0:
        logger.info("✅ Todos los scripts se ejecutaron exitosamente")
        print("\n🎉 Procesamiento completado exitosamente")
        return True
    elif scripts_exitosos > 0:
        logger.warning("⚠️ Procesamiento completado con algunos errores")
        print(f"\n⚠️ Procesamiento completado con {scripts_fallidos} errores")
        return True  # Éxito parcial
    else:
        logger.error("❌ Todos los scripts fallaron")
        print("\n💥 Procesamiento falló completamente")
        return False

def ejecutar_modo_desarrollo():
    """Modo especial para desarrollo - ejecuta con más detalle"""
    print("🔧 MODO DESARROLLO ACTIVADO")
    print("Se mostrarán más detalles de cada ejecución")
    print()
    
    # Configurar logging más verbose
    logging.getLogger().setLevel(logging.DEBUG)
    
    return main()

def mostrar_ayuda():
    """Mostrar ayuda de uso"""
    print("="*60)
    print("EJECUTOR PRINCIPAL - PROCESAMIENTO DE INGRESOS")
    print("="*60)
    print()
    print("Uso:")
    print("  python main.py              - Ejecutar procesamiento normal")
    print("  python main.py --dev        - Ejecutar en modo desarrollo")
    print("  python main.py --help       - Mostrar esta ayuda")
    print()
    print("Scripts que se ejecutarán:")
    print("  1. facturas_venta.py        - Extrae facturas diarias")
    print("  2. pagos_ingresos.py        - Extrae pagos diarios") 
    print("  3. sincronizador_alegra_sharepoint.py - Sincroniza datos")
    print()
    print("Requisitos:")
    print("  - Archivo .env configurado")
    print("  - Conexión a internet")
    print("  - Credenciales válidas de Alegra y SharePoint")

if __name__ == "__main__":
    # Verificar argumentos de línea de comandos
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg in ['--help', '-h', 'help']:
            mostrar_ayuda()
            sys.exit(0)
        elif arg in ['--dev', '-d', 'dev']:
            success = ejecutar_modo_desarrollo()
        else:
            print(f"Argumento no reconocido: {sys.argv[1]}")
            print("Usa 'python main.py --help' para ver opciones")
            sys.exit(1)
    else:
        success = main()
    
    # Código de salida
    sys.exit(0 if success else 1)