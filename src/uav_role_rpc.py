import time
import pika
import psycopg2
import json
import random
from psycopg2 import Error
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()


connection_db = psycopg2.connect(user="postgres",
                              password="password",
                              host="192.168.0.17",
                              port="5432",
                              database="postgres")


def add_uav_role_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    print(body)
    print(type(recived_message))
    status_message = {}
    cursor = connection_db.cursor()
    insert_query = """ SELECT id FROM uav_type WHERE name = '{}';
                            """.format(recived_message["uav_type"])
    cursor.execute(insert_query)
    record = cursor.fetchone()
    if record:
        uav_type_id = record[0]
        try:
            print("here1")
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_role(name, uav_type) VALUES (%s, %s)"""
            item_tuple = (recived_message["name"],uav_type_id)
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
            status_message["status"] = "success"
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(status_message))
            print("here 2")
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
    else:
        status_message["status"] = "error"
        status_message["error"] = ""
        status_message["details"] = "Not found name of uav type"
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def get_uav_role_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message ={}
    final_json = {}
    try:
        if recived_message["id"]:
            try:
                cursor = connection_db.cursor()
                insert_query = """ SELECT * FROM uav_role WHERE id = '{}';
                                        """.format(recived_message["id"])

                cursor.execute(insert_query)
                records = cursor.fetchall()
                print(records)
                if records:
                    for record in records:
                        final_json[record[0]] = {}
                        final_json[record[0]]["name"] = record[1]
                        final_json[record[0]]["uav_type"] = record[2]
                    print(final_json)
                else:
                    final_json["status"] = "not found"
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

    except KeyError:
        cursor = connection_db.cursor()
        insert_query = """ SELECT * FROM uav_role;
                                           """
        cursor.execute(insert_query)
        records = cursor.fetchall()
        print(records)
        for record in records:
            final_json[record[0]] = {}
            final_json[record[0]]["name"] = record[1]
            final_json[record[0]]["uav_type"] = record[2]
        print(final_json)

        print(json.dumps(final_json))
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(final_json))


channel.queue_declare(queue='add_uav_role_rpc', durable=False)
channel.basic_consume(queue='add_uav_role_rpc', on_message_callback=add_uav_role_rpc, auto_ack=True)
channel.queue_declare(queue='get_uav_role_rpc', durable=False)
channel.basic_consume(queue='get_uav_role_rpc', on_message_callback=get_uav_role_rpc, auto_ack=True)

channel.start_consuming()
