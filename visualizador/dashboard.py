# visualizador/dashboard.py
import pika
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import threading
import time
import numpy as np
from collections import defaultdict
import sys

from shared import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, RESULTS_QUEUE, SCENARIOS_QUEUE, MODEL_QUEUE

# Almacenamiento de datos
resultados = []
workers_activos = defaultdict(float)
scenarios_generated = 0
scenarios_processed = 0
data_lock = threading.Lock()

def setup_rabbitmq_connection():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
        )
        return connection
    except Exception as e:
        print(f"Error conectando a RabbitMQ: {e}")
        return None

def get_queue_stats():
    """Obtiene estadísticas de las colas SIN consumir mensajes"""
    try:
        connection = setup_rabbitmq_connection()
        if not connection:
            return {}
        
        channel = connection.channel()
        
        stats = {}
        for queue_name in [SCENARIOS_QUEUE, MODEL_QUEUE, RESULTS_QUEUE]:
            try:
                method = channel.queue_declare(queue=queue_name, passive=True)
                stats[queue_name] = method.method.message_count
            except:
                stats[queue_name] = 0
        
        connection.close()
        return stats
        
    except Exception as e:
        print(f"Error obteniendo estadísticas: {e}")
        return {}

def check_queues_periodically():
    """Revisa periódicamente el estado de las colas"""
    global scenarios_generated, scenarios_processed
    
    while True:
        try:
            stats = get_queue_stats()
            with data_lock:
                scenarios_generated = stats.get(SCENARIOS_QUEUE, 0)
                scenarios_processed = len(resultados)
                
            time.sleep(2)
            
        except Exception as e:
            print(f"Error en monitoreo: {e}")
            time.sleep(5)

def rabbitmq_consumer():
    """Consume SOLO resultados para el dashboard"""
    print("Dashboard conectando a RabbitMQ...")
    
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
        )
        channel = connection.channel()
        
        channel.queue_declare(queue=RESULTS_QUEUE, durable=True)
        
        def callback(ch, method, properties, body):
            """Procesa resultados SIN interferir con workers"""
            try:
                resultado = json.loads(body.decode())
                
                with data_lock:
                    resultados.append(resultado)
                    
                    # Actualizar información del worker
                    worker_id = resultado.get('worker_id', 'unknown')
                    if worker_id != 'unknown':
                        workers_activos[worker_id] = time.time()
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
                if len(resultados) % 10 == 0:
                    print(f"Dashboard: {len(resultados)} resultados recibidos")
                    
            except Exception as e:
                print(f"Error procesando resultado: {e}")
        
        channel.basic_consume(
            queue=RESULTS_QUEUE,
            on_message_callback=callback,
            auto_ack=False
        )
        
        print("Dashboard escuchando resultados...")
        channel.start_consuming()
        
    except Exception as e:
        print(f"Error en consumidor: {e}")
        time.sleep(5)
        rabbitmq_consumer()

def update_plot(frame):
    """Actualiza los gráficos en tiempo real"""
    global resultados, workers_activos, scenarios_generated, scenarios_processed
    
    with data_lock:
        current_results = resultados.copy()
        current_workers = workers_activos.copy()
        gen = scenarios_generated
        proc = scenarios_processed
    
    # Limpiar gráficos
    ax1.clear()
    ax2.clear()
    ax3.clear()
    ax4.clear()
    
    # Gráfico 1: Progreso de la simulación
    ax1.bar(['Generados', 'Procesados'], [gen, len(current_results)], 
            color=['blue', 'green'], alpha=0.7)
    ax1.set_title('Progreso de Simulacion')
    ax1.set_ylabel('Cantidad de Escenarios')
    
    # Agregar números en las barras
    for i, v in enumerate([gen, len(current_results)]):
        ax1.text(i, v + max(gen, 1)*0.01, str(v), ha='center', va='bottom', fontweight='bold')
    
    # Gráfico 2: Resultados a lo largo del tiempo (simple)
    ax2.plot(range(len(current_results)), range(len(current_results)), 'ro-', markersize=2, alpha=0.7)
    ax2.set_title('Resultados Procesados')
    ax2.set_xlabel('Tiempo')
    ax2.set_ylabel('Total de Resultados')
    ax2.grid(True, alpha=0.3)
    
    # Gráfico 3: Workers activos
    current_time = time.time()
    active_workers = [wid for wid, last_seen in current_workers.items() 
                     if current_time - last_seen < 30]
    
    if active_workers:
        worker_counts = {}
        for worker in active_workers:
            worker_counts[worker] = worker_counts.get(worker, 0) + 1
        
        workers = list(worker_counts.keys())
        counts = list(worker_counts.values())
        
        bars = ax3.bar(workers, counts, color='orange', alpha=0.7)
        ax3.set_title(f'Workers Activos: {len(active_workers)}')
        ax3.set_ylabel('Actividad Reciente')
        
        for bar, count in zip(bars, counts):
            ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.1,
                    str(count), ha='center', va='bottom')
    else:
        ax3.text(0.5, 0.5, 'No hay workers activos', 
                ha='center', va='center', transform=ax3.transAxes)
        ax3.set_title('Workers Activos')
    
    # Gráfico 4: Información del sistema (solo texto simple)
    ax4.axis('off')
    
    info_text = "SISTEMA MONTE CARLO\n\n"
    info_text += f"Workers activos: {len(active_workers)}\n"
    info_text += f"Resultados: {len(current_results)}\n"
    info_text += f"Escenarios pendientes: {gen}\n"
    info_text += f"Ultima actualizacion: {time.strftime('%H:%M:%S')}"
    
    ax4.text(0.05, 0.95, info_text, transform=ax4.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.7))

def main():
    """Función principal del dashboard"""
    global ax1, ax2, ax3, ax4
    
    plt.style.use('ggplot')
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle('Dashboard Monte Carlo - Monitoreo en Tiempo Real', fontsize=14, fontweight='bold')
    
    print("Iniciando dashboard...")
    
    # Hilo para consumir resultados
    consumer_thread = threading.Thread(target=rabbitmq_consumer, daemon=True)
    consumer_thread.start()
    
    # Hilo para monitoreo de colas
    monitor_thread = threading.Thread(target=check_queues_periodically, daemon=True)
    monitor_thread.start()
    
    print("Dashboard iniciado correctamente")
    print("Monitoreando sin interferir con workers...")
    print("Presiona Ctrl+C para cerrar")
    
    try:
        # Animación que actualiza cada 500ms
        ani = animation.FuncAnimation(fig, update_plot, interval=500, cache_frame_data=False)
        plt.show()
        
    except KeyboardInterrupt:
        print("\nCerrando dashboard...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print(f"Resumen final: {len(resultados)} resultados procesados")

if __name__ == "__main__":
    main()