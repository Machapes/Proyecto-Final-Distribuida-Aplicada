# productor.py (versión con time-out)
import pika
import json
import uuid
import numpy as np
import os
from shared.models import MonteCarloModel, VariableDefinition, DistributionType, Scenario
from shared import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, SCENARIOS_QUEUE, MODEL_QUEUE, RESULTS_QUEUE

class ProductorMonteCarlo:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.current_model = None
        self.scenarios_generados = 0
        self.modelos_disponibles = {}
        self.connect()
        self.cargar_modelos_disponibles()
    
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
            
            # Declarar las colas
            self.channel.queue_declare(queue=SCENARIOS_QUEUE, durable=True)
            self.channel.queue_declare(queue=MODEL_QUEUE, durable=True)
            
            print("Productor conectado a RabbitMQ")
            
        except Exception as e:
            print(f"Error conectando a RabbitMQ: {e}")
            raise

    def cargar_modelos_disponibles(self):
        modelos_dir = "modelos"
        
        if not os.path.exists(modelos_dir):
            os.makedirs(modelos_dir)
        
        for archivo in os.listdir(modelos_dir):
            if archivo.endswith('.txt'):
                ruta_completa = os.path.join(modelos_dir, archivo)
                self.modelos_disponibles[archivo] = ruta_completa
        
        if self.modelos_disponibles:
            print(f"Modelos cargados: {len(self.modelos_disponibles)}")
        else:
            print("No hay modelos en el directorio 'modelos/'")

    def cargar_modelo_desde_archivo(self, archivo_path: str):
        try:
            with open(archivo_path, 'r') as file:
                lines = file.readlines()
            
            # Parsear el archivo
            model_id = str(uuid.uuid4())[:8]
            function_code = ""
            variables = []
            iterations = 1000
            
            for line in lines:
                line = line.strip()
                if line.startswith("#") or not line:
                    continue
                
                if line.startswith("FUNCTION:"):
                    function_code = line.replace("FUNCTION:", "").strip()
                elif line.startswith("ITERATIONS:"):
                    iterations = int(line.replace("ITERATIONS:", "").strip())
                elif line.startswith("VAR:"):
                    parts = line.replace("VAR:", "").strip().split(",")
                    var_name = parts[0].strip()
                    dist_type = DistributionType(parts[1].strip())
                    
                    params = {}
                    for param in parts[2:]:
                        key, value = param.strip().split("=")
                        params[key] = float(value)
                    
                    variables.append(VariableDefinition(var_name, dist_type, params))
            
            self.current_model = MonteCarloModel(
                model_id=model_id,
                function_code=function_code,
                variables=variables,
                iterations=iterations
            )
            
            print(f"Modelo cargado: {model_id}")
            print(f"Variables: {[var.name for var in variables]}")
            print(f"Iteraciones: {iterations}")
            
            return self.current_model
            
        except Exception as e:
            print(f"Error cargando modelo: {e}")
            return None
    
    def publicar_modelo(self):
        if not self.current_model:
            print("o hay modelo cargado")
            return False
        
        try:
            try:
                self.channel.queue_purge(MODEL_QUEUE)
                print("Modelo anterior eliminado")
            except Exception as e:
                print(f"No se pudo limpiar cola de modelo: {e}")
            
            properties = pika.BasicProperties(
                delivery_mode=2,
                expiration='300000'
            )
            
            self.channel.basic_publish(
                exchange='',
                routing_key=MODEL_QUEUE,
                body=self.current_model.to_json(),
                properties=properties
            )           
            return True
            
        except Exception as e:
            print(f"Error publicando modelo: {e}")
            return False
    
    def generar_escenario(self):
        if not self.current_model:
            return None
        
        parameters = {}
        for variable in self.current_model.variables:
            if variable.distribution == DistributionType.UNIFORM:
                low = variable.parameters.get('min', 0)
                high = variable.parameters.get('max', 1)
                parameters[variable.name] = np.random.uniform(low, high)
            
            elif variable.distribution == DistributionType.NORMAL:
                mean = variable.parameters.get('mean', 0)
                std = variable.parameters.get('std', 1)
                parameters[variable.name] = np.random.normal(mean, std)
            
            elif variable.distribution == DistributionType.EXPONENTIAL:
                scale = variable.parameters.get('scale', 1)
                parameters[variable.name] = np.random.exponential(scale)
        
        scenario_id = f"{self.current_model.model_id}_{self.scenarios_generados:06d}"
        return Scenario(scenario_id, self.current_model.model_id, parameters)
    
    def publicar_escenarios(self, cantidad: int):
        if not self.current_model:
            print("No hay modelo cargado. Primero carga un modelo.")
            return
        
        print(f"Generando {cantidad} escenarios...")
        escenarios_publicados = 0
        
        for i in range(cantidad):
            try:
                scenario = self.generar_escenario()
                if scenario:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=SCENARIOS_QUEUE,
                        body=scenario.to_json(),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    self.scenarios_generados += 1
                    escenarios_publicados += 1
                    
                    if escenarios_publicados % 100 == 0:
                        print(f"Escenarios publicados: {escenarios_publicados}/{cantidad}")
                
            except Exception as e:
                print(f"Error publicando escenario: {e}")
        
        print(f"Total de escenarios publicados: {escenarios_publicados}")
    
    def mostrar_menu_principal(self):
        print("Sistema Menu")
        print("1.Cargar modelo")
        print("2.Publicar escenarios")
        print("3.Salir")
    
    def mostrar_menu_modelos(self):
        self.cargar_modelos_disponibles()
        
        if not self.modelos_disponibles:
            print("\nNo hay modelos")
            return []
        
        print("\nMODELOS:")
        
        modelos_lista = list(self.modelos_disponibles.keys())
        for i, modelo in enumerate(modelos_lista, 1):
            print(f"{i}. {modelo}")
        
        print(f"{len(modelos_lista) + 1}.Volver al menú principal")
        return modelos_lista
    
    def ejecutar_interactivo(self):    
        while True:
            self.mostrar_menu_principal()
            opcion = input("\nSelecciona una opción: ").strip()
            
            if opcion == "1":
                # Cargar modelo
                modelos_lista = self.mostrar_menu_modelos()
                if not modelos_lista:
                    continue
                
                try:
                    seleccion = int(input("Selecciona un modelo: "))
                    if 1 <= seleccion <= len(modelos_lista):
                        modelo_seleccionado = modelos_lista[seleccion - 1]
                        ruta_modelo = self.modelos_disponibles[modelo_seleccionado]
                        
                        if self.cargar_modelo_desde_archivo(ruta_modelo):
                            if self.publicar_modelo():
                                print(f"Modelo '{modelo_seleccionado}'Modelo Publicado")
                    elif seleccion == len(modelos_lista) + 1:
                        continue
                    else:
                        print("Opción inválida")
                except ValueError:
                    print("Ingresa un número válido")
            
            elif opcion == "2":
                if not self.current_model:
                    print("carga un modelo")
                    continue
                
                try:
                    cantidad = input("Cantidad de escenarios a publicar: ").strip()
                    if cantidad.isdigit():
                        self.publicar_escenarios(int(cantidad))
                    else:
                        print("Ingresa un número válido")
                except ValueError:
                    print("Ingresa un número válido")
            
            elif opcion == "3":
                print("Saliendo")
                break
            
            else:
                print("Opción inválida")
    
    def cerrar(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Conexión cerrada")

if __name__ == "__main__":
    productor = ProductorMonteCarlo()
    
    try:
        productor.ejecutar_interactivo()
        
    except KeyboardInterrupt:
        print("\nCerrando")
    finally:
        productor.cerrar()