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


def mission_input_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message ={}
    final_json = {}
    try:
        cursor = connection_db.cursor()
        insert_query = """ INSERT INTO mission_input (directive_time_secs, time_out_of_launches, 
        simultaneous_launch_number, reset_point, landing_point, uavs, payload, target_type, dest_poligon,
        targets_number, targets_coords, time_intervals) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """.format(recived_message["directive_time_secs"], recived_message["time_out_of_launches"],
                   recived_message["simultaneous_launch_number"], recived_message["reset_point"],
                   recived_message["landing_point"], recived_message["uavs"], recived_message["payload"],
                   recived_message["target_type"], recived_message["dest_poligon"], recived_message["targets_number"],
                   recived_message["targets_coords"], recived_message["time_intervals"])
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        cursor.close()
        channel.basic_publish(
            exchange='',
            routing_key="get_mission_params",
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))


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
