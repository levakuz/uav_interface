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


def add_uav_type_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    print(body)
    print(type(recived_message))
    status_message = {}
    try:
        print("here1")
        cursor = connection_db.cursor()
        insert_query = """ INSERT INTO uav_type (name, min_vel, max_vel, min_vertical_vel_up, max_vertical_vel_up, min_vertical_vel_down, max_vertical_vel_down, cargo_type, cargo_quantity, fuel_consume, radius_of_turn)
                                                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        item_tuple = (recived_message["name"], recived_message["vel"][0], recived_message["vel"][1],
                      recived_message["vertical_vel_up"][0], recived_message["vertical_vel_up"][1],
                      recived_message["vertical_vel_down"][0], recived_message["vertical_vel_down"][1],
                      recived_message["cargo_type"], recived_message["cargo_quantity"],
                      recived_message["fuel_consume"], recived_message["radius_of_turn"])
        cursor.execute(insert_query, item_tuple)
        connection_db.commit()
        status_message["status"] = "success"
        connection_db.rollback()
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


def get_uav_type_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message ={}
    try:
        if recived_message["name"]:
            try:
                cursor = connection_db.cursor()
                insert_query = """ SELECT * FROM uav_type WHERE name = '{}';
                                        """.format("Haha")
                cursor.execute(insert_query)
                record = cursor.fetchall()
                print(record)
                recived_message = {}
                recived_message["name"] = record[0][-2]
                recived_message["vel"] = []
                recived_message["vel"].append(record[0][0])
                recived_message["vel"].append(record[0][1])
                recived_message["vertical_vel_up"] = []
                recived_message["vertical_vel_up"].append(record[0][3])
                recived_message["vertical_vel_up"].append(record[0][2])
                recived_message["vertical_vel_down"] = []
                recived_message["vertical_vel_down"].append(record[0][5])
                recived_message["vertical_vel_down"].append(record[0][4])
                recived_message["cargo_type"] = record[0][6]
                recived_message["fuel_consume"] = record[0][7]
                recived_message["radius_of_turn"] = record[0][8]
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(recived_message))
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
        insert_query = """ SELECT * FROM uav_type;
                                               """
        cursor.execute(insert_query)
        records = cursor.fetchall()
        print(records)
        final_json = {}
        for record in records:
            print(record)
            final_json[record[-1]] = {}
            final_json[record[-1]]["name"] = record[-2]
            final_json[record[-1]]["vel"] = []
            final_json[record[-1]]["vel"].append(record[0])
            final_json[record[-1]]["vel"].append(record[1])
            final_json[record[-1]]["vertical_vel_up"] = []
            final_json[record[-1]]["vertical_vel_up"].append(record[3])
            final_json[record[-1]]["vertical_vel_up"].append(record[2])
            final_json[record[-1]]["vertical_vel_down"] = []
            final_json[record[-1]]["vertical_vel_down"].append(record[5])
            final_json[record[-1]]["vertical_vel_down"].append(record[4])
            final_json[record[-1]]["cargo_type"] = record[6]
            final_json[record[-1]]["fuel_consume"] = record[7]
            final_json[record[-1]]["radius_of_turn"] = record[8]

        print(json.dumps(final_json))
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(final_json))


channel.queue_declare(queue='add_uav_type_rpc', durable=False)
channel.basic_consume(queue='add_uav_type_rpc', on_message_callback=add_uav_type_rpc, auto_ack=True)
channel.queue_declare(queue='get_uav_type_rpc', durable=False)
channel.basic_consume(queue='get_uav_type_rpc', on_message_callback=get_uav_type_rpc, auto_ack=True)

channel.start_consuming()



