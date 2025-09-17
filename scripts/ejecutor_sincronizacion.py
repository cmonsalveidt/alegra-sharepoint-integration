import os
import sys
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv

# Importar nuestros módulos
from sincronizador_alegra_sharepoint import SincronizadorAlegra

def setup_logging():
    """Configurar logging para el ejecutor"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/ejecutor_sincronizacion_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return log_filename

class EjecutorSincronizacion:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()
        
        # Configuración desde variables de entorno
        self.modo_sincronizacion = os.getenv("MODO_SINCRONIZACION", "automatico")
        self.intervalo_horas = int(os.getenv("INTERVALO_SINCRONIZACION", "6"))
        self.solo_pagos_sin_cliente = os.getenv("SOLO_PAGOS_SIN_CLIENTE", "true").lower() == "true"

    def ejecutar_sincronizacion_manual(self, tipo="completo"):
        """Ejecutar sincronización manual"""
        self.logger.info(f"Ejecutando sincronización manual: {tipo}")
        
        try:
            if tipo in ["completo", "pagos"]:
                # Ejecutar sincronización de pagos sin cliente
                sincronizador = SincronizadorAlegra()
                success = sincronizador.main()
                
                if success:
                    self.logger.info("Sincronización completada exitosamente")
                    return True
                else:
                    self.logger.error("Sincronización completada con errores")
                    return False
            
            elif tipo == "test":
                # Modo de prueba: solo revisar sin hacer cambios
                self.ejecutar_modo_prueba()
                return True
            
            else:
                self.logger.error(f"Tipo de sincronización no válido: {tipo}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error en sincronización manual: {str(e)}")
            return False

    def ejecutar_modo_prueba(self):
        """Ejecutar en modo de prueba sin hacer cambios"""
        self.logger.info("MODO PRUEBA - Sin realizar cambios")
        
        try:
            # Crear sincronizador en modo solo lectura
            sincronizador = SincronizadorAlegra()
            
            # Obtener pagos sin cliente
            pagos_sin_cliente = sincronizador.obtener_pagos_sin_cliente()
            
            self.logger.info(f"Encontrados {len(pagos_sin_cliente)} pagos sin cliente")
            
            # Para cada pago, solo verificar si hay cambios sin aplicarlos
            cambios_detectados = 0
            
            for pago_sp in pagos_sin_cliente[:5]:  # Solo revisar los primeros 5 para la prueba
                pago_id = pago_sp['Pago_ID']
                numero_pago = pago_sp['Numero_Pago']
                
                self.logger.info(f"[PRUEBA] Revisando pago {numero_pago}")
                
                # Obtener datos de Alegra
                pago_alegra = sincronizador.obtener_pago_desde_alegra(pago_id)
                
                if pago_alegra:
                    cliente_alegra = sincronizador.safe_get_nested(pago_alegra, 'client', 'id', default='')
                    if cliente_alegra:
                        self.logger.info(f"[PRUEBA] Pago {numero_pago} ahora tiene cliente: {sincronizador.safe_get_nested(pago_alegra, 'client', 'name', default='N/A')}")
                        cambios_detectados += 1
                    else:
                        self.logger.info(f"[PRUEBA] Pago {numero_pago} sigue sin cliente")
            
            self.logger.info(f"[PRUEBA] Cambios detectados en {cambios_detectados} pagos")
            print(f"Modo prueba completado. Cambios detectados: {cambios_detectados}")
            
        except Exception as e:
            self.logger.error(f"Error en modo prueba: {str(e)}")

    def ejecutar_sincronizacion_programada(self):
        """Ejecutar sincronización en modo programado"""
        import time
        
        self.logger.info(f"Iniciando sincronización programada (cada {self.intervalo_horas} horas)")
        
        while True:
            try:
                self.logger.info("Ejecutando ciclo de sincronización programada...")
                
                success = self.ejecutar_sincronizacion_manual("completo")
                
                if success:
                    self.logger.info(f"Ciclo completado exitosamente. Próxima ejecución en {self.intervalo_horas} horas")
                else:
                    self.logger.error(f"Ciclo completado con errores. Reintentando en {self.intervalo_horas} horas")
                
                # Esperar hasta el próximo ciclo
                time.sleep(self.intervalo_horas * 3600)  # Convertir horas a segundos
                
            except KeyboardInterrupt:
                self.logger.info("Sincronización programada interrumpida por el usuario")
                break
            except Exception as e:
                self.logger.error(f"Error en sincronización programada: {str(e)}")
                self.logger.info(f"Reintentando en {self.intervalo_horas} horas...")
                time.sleep(self.intervalo_horas * 3600)

    def mostrar_estadisticas(self):
        """Mostrar estadísticas de la base de datos"""
        self.logger.info("Obteniendo estadísticas de SharePoint...")
        
        try:
            sincronizador = SincronizadorAlegra()
            
            # Obtener pagos sin cliente
            pagos_sin_cliente = sincronizador.obtener_pagos_sin_cliente()
            
            print("="*50)
            print("ESTADÍSTICAS DE SHAREPOINT")
            print("="*50)
            print(f"Pagos sin cliente asignado: {len(pagos_sin_cliente)}")
            
            if pagos_sin_cliente:
                print("\nPrimeros 10 pagos sin cliente:")
                for i, pago in enumerate(pagos_sin_cliente[:10], 1):
                    print(f"{i:2}. {pago['Numero_Pago']} - Monto: ${pago.get('fields', {}).get('Monto_x0020_Total', 'N/A')}")
            
            print("="*50)
            
        except Exception as e:
            self.logger.error(f"Error obteniendo estadísticas: {str(e)}")
            print(f"Error obteniendo estadísticas: {str(e)}")

def main():
    """Función principal del ejecutor"""
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    parser = argparse.ArgumentParser(description='Ejecutor de Sincronización Alegra-SharePoint')
    parser.add_argument('--modo', choices=['manual', 'programado', 'prueba', 'stats'], 
                       default='manual', help='Modo de ejecución')
    parser.add_argument('--tipo', choices=['completo', 'pagos', 'test'], 
                       default='completo', help='Tipo de sincronización (solo para modo manual)')
    parser.add_argument('--intervalo', type=int, default=6, 
                       help='Intervalo en horas para modo programado')
    
    args = parser.parse_args()
    
    print("="*60)
    print("EJECUTOR DE SINCRONIZACIÓN ALEGRA-SHAREPOINT")
    print("="*60)
    print(f"Modo: {args.modo}")
    print(f"Log: {log_file}")
    print()
    
    try:
        ejecutor = EjecutorSincronizacion()
        
        if args.modo == 'manual':
            print(f"Ejecutando sincronización manual ({args.tipo})...")
            success = ejecutor.ejecutar_sincronizacion_manual(args.tipo)
            
        elif args.modo == 'programado':
            print(f"Iniciando modo programado (cada {args.intervalo} horas)...")
            print("Presiona Ctrl+C para detener")
            ejecutor.intervalo_horas = args.intervalo
            success = ejecutor.ejecutar_sincronizacion_programada()
            
        elif args.modo == 'prueba':
            print("Ejecutando modo de prueba...")
            success = ejecutor.ejecutar_sincronizacion_manual('test')
            
        elif args.modo == 'stats':
            print("Obteniendo estadísticas...")
            ejecutor.mostrar_estadisticas()
            success = True
            
        else:
            print(f"Modo no válido: {args.modo}")
            success = False
        
        if success:
            print("Ejecución completada exitosamente")
        else:
            print("Ejecución completada con errores")
        
        return success
        
    except Exception as e:
        logger.error(f"Error crítico en ejecutor: {str(e)}")
        print(f"ERROR CRÍTICO: {str(e)}")
        print(f"Ver detalles en: {log_file}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)