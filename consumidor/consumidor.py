import pika
import json
import uuid
import random
import time
import numpy as np
from shared.models import MonteCarloModel, Scenario, Result
from shared import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, SCENARIOS_QUEUE, MODEL_QUEUE, RESULTS_QUEUE

class ConsumidorMonteCarlo:
    def __init__(self, worker_id=None):
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"
        self.current_model = None
        self.scenarios_processed = 0
        self.total_processing_time = 0
        self.connection = None
        self.channel = None
        self.last_activity = time.time()
        self.model_loaded = False
        self.model_load_time = None
        self.connect()
    
    def connect(self):
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials
                )
            )
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue=SCENARIOS_QUEUE, durable=True)
            self.channel.queue_declare(queue=MODEL_QUEUE, durable=True)
            self.channel.queue_declare(queue=RESULTS_QUEUE, durable=True)
            
            print(f"Consumidor {self.worker_id} conectado a RabbitMQ")
            
        except Exception as e:
            print(f"Error conectando consumidor {self.worker_id}: {e}")
            raise
    
    def cargar_modelo(self):
        if self.model_loaded and self.current_model:
            print(f"{self.worker_id}: Modelo {self.current_model.model_id} ya cargado")
            return True
            
        try:
            method_frame, header_frame, body = self.channel.basic_get(MODEL_QUEUE, auto_ack=False)
            
            if method_frame and body:
                model_data = body.decode()
                self.current_model = MonteCarloModel.from_json(model_data)
                self.model_loaded = True
                self.model_load_time = time.time()
                
                self.channel.basic_nack(method_frame.delivery_tag, requeue=True)
                
                print(f"{self.worker_id} cargó modelo: {self.current_model.model_id}")
                print(f"Mensaje permanece en cola para otros consumidores")
                return True
            else:
                print(f"{self.worker_id}: No hay modelo disponible en cola")
                return False
                
        except Exception as e:
            print(f"{self.worker_id} error cargando modelo: {e}")
            return False
    
    def ejecutar_modelo(self, scenario):
        if not self.current_model:
            print("No hay modelo")
            return None, 0
        
        try:
            context = scenario.parameters.copy()
            
            exec_globals = {
                'random': random,
                'np': np,
                'resultado': 0
            }
            exec_globals.update(context)
            
            # Ejecutar el código del modelo
            start_time = time.time()
            exec(self.current_model.function_code, exec_globals)
            processing_time = time.time() - start_time
            
            # Obtener el resultado
            result_value = exec_globals.get('resultado', 0)
            
            self.scenarios_processed += 1
            self.total_processing_time += processing_time
            self.last_activity = time.time()
            
            return result_value, processing_time
            
        except Exception as e:
            print(f"Error ejecutando modelo: {e}")
            return None, 0
    
    def procesar_escenario(self, ch, method, properties, body):
        try:
            scenario_data = body.decode()
            scenario = Scenario.from_json(scenario_data)
            
            if not self.current_model or scenario.model_id != self.current_model.model_id:
                print(f"{self.worker_id}: Recargando modelo para {scenario.model_id}")
                if not self.cargar_modelo():
                    print(f"{self.worker_id}: Modelo no disponible, reintentando...")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    time.sleep(1)
                    return
            
            print(f"{self.worker_id} procesando: {scenario.scenario_id}")
            
            result_value, processing_time = self.ejecutar_modelo(scenario)
            
            if result_value is not None:
                result = Result(
                    scenario_id=scenario.scenario_id,
                    model_id=scenario.model_id,
                    result=result_value,
                    worker_id=self.worker_id
                )
                
                self.channel.basic_publish(
                    exchange='',
                    routing_key=RESULTS_QUEUE,
                    body=result.to_json(),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                
                print(f"{self.worker_id} completó {scenario.scenario_id}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error procesando escenario: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def iniciar_consumo(self):
        print(f"Consumidor {self.worker_id} iniciando...")
        
        if not self.cargar_modelo():
            print(f"{self.worker_id}: Esperando modelo...")
            time.sleep(2)
            if not self.cargar_modelo():
                print(f"{self.worker_id}: Sin modelo")
        
        # Configurar consumo de escenarios
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=SCENARIOS_QUEUE,
            on_message_callback=self.procesar_escenario
        )
        
        print(f"{self.worker_id} listo para procesar escenarios")
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(f"\nConsumidor {self.worker_id} detenido por usuario")
        except Exception as e:
            print(f"Error en consumidor {self.worker_id}: {e}")
        finally:
            self.cerrar()
    
    def obtener_estadisticas(self):
        avg_time = (self.total_processing_time / self.scenarios_processed 
                   if self.scenarios_processed > 0 else 0)
        
        tiempo_inactivo = time.time() - self.last_activity
        estado = "Activo" if tiempo_inactivo < 30 else "Inactivo"
        
        modelo_info = "Ninguno"
        if self.current_model:
            tiempo_carga = time.time() - self.model_load_time if self.model_load_time else 0
            modelo_info = f"{self.current_model.model_id} (hace {tiempo_carga:.0f}s)"
        
        return {
            "worker_id": self.worker_id,
            "modelo_actual": modelo_info,
            "modelo_cargado": self.model_loaded,
            "escenarios_procesados": self.scenarios_processed,
            "tiempo_total_procesamiento": f"{self.total_processing_time:.3f}s",
            "tiempo_promedio": f"{avg_time:.3f}s",
            "ultima_actividad": f"{tiempo_inactivo:.1f}s",
            "estado": estado,
            "eficiencia": f"{(avg_time * 1000):.1f}ms/escenario" if avg_time > 0 else "N/A"
        }
    
    def cerrar(self):
        try:
            if self.connection and not self.connection.is_closed:
                # Mostrar estadísticas finales
                print(f"\nEstadisticas {self.worker_id}:")
                stats = self.obtener_estadisticas()
                print(json.dumps(stats, indent=2, ensure_ascii=False))
                
                self.connection.close()
                print(f"🔌 Conexión del consumidor {self.worker_id} cerrada")
                
        except Exception as e:
            print(f"Error cerrando consumidor {self.worker_id}: {e}")

if __name__ == "__main__":
    import sys
    
    worker_id = sys.argv[1] if len(sys.argv) > 1 else None
    consumidor = ConsumidorMonteCarlo(worker_id)
    
    try:
        consumidor.iniciar_consumo()
    except KeyboardInterrupt:
        print(f"\nEstadísticas finales de {consumidor.worker_id}:")
        print(json.dumps(consumidor.obtener_estadisticas(), indent=2))
    finally:
        consumidor.cerrar()