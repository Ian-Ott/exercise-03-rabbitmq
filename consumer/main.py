"""
Exercise 03 — Event Consumer

Implement a RabbitMQ consumer that:
- Connects to RabbitMQ at RABBITMQ_URL env var
- Consumes messages from the "node_events" queue
- Logs each event to stdout: "EVENT: {event} | node: {node_name} | time: {timestamp}"
- Acknowledges each message after processing
"""

# TODO: Implement the consumer
import pika
import time
import os
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

log = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

def conectar_con_reintento():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(RABBITMQ_URL)
            )
            return connection
        except Exception as e:
            time.sleep(3)

def procesar_mensaje(ch, method, properties, body):
    datos = json.loads(body.decode())
    event = datos.get("event")
    node_name = datos.get("node_name")
    timestamp = datos.get("timestamp")
    log.info(
        f"EVENT: {event} | node: {node_name} | time: {timestamp}"
    )

    # Confirmar a RabbitMQ que el mensaje fue procesado correctamente
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    

    connection = conectar_con_reintento()
    channel = connection.channel()

    # Declarar cola durable
    channel.queue_declare(queue='node_events', durable=True)

    # Fair dispatch: un mensaje a la vez por consumidor hasta ACK
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue='node_events',
        on_message_callback=procesar_mensaje,
        auto_ack=False
    )
    log.info("Esperando mensajes...")
    channel.start_consuming()

if __name__ == '__main__':
    main()
