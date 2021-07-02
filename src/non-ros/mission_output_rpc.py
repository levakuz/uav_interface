import time
import pika
import psycopg2
import json
from psycopg2 import Error

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                               5672,
                                                               '/',
                                                               credentials, blocked_connection_timeout=0, heartbeat=0))

channel = connection.channel()

connection_db = psycopg2.connect(user="postgres",
                                 password="password",
                                 host="192.168.0.17",
                                 port="5432",
                                 database="postgres")


def add_mission_output_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message = {}
    final_json = {}
    try:
        cursor = connection_db.cursor()
        insert_query = """ UPDATE mission set (id, time_zero, uavs) WHERE id = '{}' VALUES (%s, %s, %s);
        """.format(recived_message["id"])
        val = (recived_message["id"], recived_message["time_zero"], json.dumps(recived_message["uavs"]))
        cursor.execute(insert_query, val)
        connection_db.commit()
        count = cursor.rowcount
        cursor.close()
        # channel.basic_publish(
        #     exchange='',
        #     routing_key="get_mission_params",
        #     body=body,
        #     properties=pika.BasicProperties(
        #         delivery_mode=2,
        #     ))
        final_json = {"status": "success"}
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(final_json))

    except Error as e:
        print("error", e)
        status_message["status"] = "error"
        status_message["error"] = e.pgcode
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))
    except KeyError as e:
        status_message["status"] = "error"
        status_message["key"] = str(e)
        status_message["details"] = "No key"
        print(json.dumps(status_message))
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))

    except TypeError:
        status_message["status"] = "error"
        status_message["details"] = "wrong format"
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


channel.queue_declare(queue='add_mission_output_rpc', durable=False)
channel.basic_consume(queue='add_mission_output_rpc', on_message_callback=add_mission_output_rpc, auto_ack=True)


channel.start_consuming()

