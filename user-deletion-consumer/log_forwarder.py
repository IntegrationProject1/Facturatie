import pika
import os
import xml.etree.ElementTree as ET

def send_log_to_controlroom(service_name, status, code, message):
    try:
        # XML opbouwen volgens LOG_XSD
        log = ET.Element("Log")
        ET.SubElement(log, "ServiceName").text = service_name
        ET.SubElement(log, "Status").text = status
        ET.SubElement(log, "Code").text = code
        ET.SubElement(log, "Message").text = message

        xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(log, encoding='unicode')

        # RabbitMQ connectie en publicatie
        params = pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST"),
            port=int(os.getenv("RABBITMQ_PORT")),
            credentials=pika.PlainCredentials(
                os.getenv("RABBITMQ_USER"),
                os.getenv("RABBITMQ_PASSWORD")
            ),
            virtual_host="/"
        )

        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Versturen naar de juiste exchange/queue
        channel.exchange_declare(exchange=os.getenv("RABBITMQ_LOG_EXCHANGE", "log_monitoring"), exchange_type="direct", durable=True)
        channel.queue_declare(queue=os.getenv("RABBITMQ_LOG_QUEUE", "controlroom.log.events"), durable=True)

        channel.basic_publish(
            exchange=os.getenv("RABBITMQ_LOG_EXCHANGE", "log_monitoring"),
            routing_key=os.getenv("RABBITMQ_LOG_QUEUE", "controlroom.log.events"),
            body=xml_str,
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

        connection.close()

    except Exception as e:
        # Fallback: print log als RabbitMQ faalt
        print(f"Log forwarding failed: {e}")
