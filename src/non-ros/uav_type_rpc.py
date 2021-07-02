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
        insert_query = """ INSERT INTO uav_type (name, min_vel, max_vel, min_vertical_vel_up, max_vertical_vel_up, 
        min_vertical_vel_down, max_vertical_vel_down, cargo_type, cargo_quantity, fuel_consume, radius_of_turn)
                                                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                               RETURNING id;"""
        item_tuple = (recived_message["name"], recived_message["vel"][0], recived_message["vel"][1],
                      recived_message["vertical_vel_up"][0], recived_message["vertical_vel_up"][1],
                      recived_message["vertical_vel_down"][0], recived_message["vertical_vel_down"][1],
                      recived_message["cargo_type"], recived_message["cargo_quantity"],
                      recived_message["fuel_consume"], recived_message["radius_of_turn"])
        cursor.execute(insert_query, item_tuple)
        connection_db.commit()
        id_of_new_row = cursor.fetchone()[0]
        status_message = {"status": "success", "id": id_of_new_row}
        cursor.close()
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


def get_uav_type_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message ={}
    try:
        if recived_message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM uav_type WHERE id = '{}';
                                    """.format(recived_message["id"])
            cursor.execute(insert_query)
            record = cursor.fetchall()
            cursor.close()
            if record:
                print(record)
                final_json = {"name": record[0][1], "vel": []}
                final_json["vel"].append(record[0][2])
                final_json["vel"].append(record[0][3])
                final_json["vertical_vel_up"] = []
                final_json["vertical_vel_up"].append(record[0][4])
                final_json["vertical_vel_up"].append(record[0][5])
                final_json["vertical_vel_down"] = []
                final_json["vertical_vel_down"].append(record[0][6])
                final_json["vertical_vel_down"].append(record[0][7])
                final_json["cargo_type"] = record[0][8]
                final_json["fuel_consume"] = record[0][9]
                final_json["radius_of_turn"] = record[0][10]
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(final_json))
            else:
                status_message = {"status": "Not found", "details": "No information was found"}
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(status_message))

    except KeyError:
        try:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM uav_type;
                                                   """
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            if records:
                print(records)
                final_json = {}
                for record in records:
                    print(record)
                    final_json[record[0]] = {}
                    final_json[record[0]]["name"] = record[1]
                    final_json[record[0]]["vel"] = []
                    final_json[record[0]]["vel"].append(record[2])
                    final_json[record[0]]["vel"].append(record[3])
                    final_json[record[0]]["vertical_vel_up"] = []
                    final_json[record[0]]["vertical_vel_up"].append(record[4])
                    final_json[record[0]]["vertical_vel_up"].append(record[5])
                    final_json[record[0]]["vertical_vel_down"] = []
                    final_json[record[0]]["vertical_vel_down"].append(record[6])
                    final_json[record[0]]["vertical_vel_down"].append(record[7])
                    final_json[record[0]]["cargo_type"] = record[8]
                    final_json[record[0]]["fuel_consume"] = record[9]
                    final_json[record[0]]["radius_of_turn"] = record[10]

                print(json.dumps(final_json))
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(final_json))
            else:
                status_message = {"status": "Not found", "details": "No information was found"}
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(status_message))
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

    except TypeError:
        status_message["status"] = "error"
        status_message["details"] = "wrong format"
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def delete_uav_type_rpc (ch, method, properties, body):
    recived_message = json.loads(body)
    status_message ={}
    final_json = {}
    try:
        if recived_message["key"] == "id":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM uav_type WHERE id = '{}';
                                    """.format(recived_message["id"])
            cursor.execute(insert_query)
            connection_db.commit()
            rows_deleted = cursor.rowcount
            cursor.close()
            if rows_deleted != 0:
                final_json["status"] = "success"
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(final_json))
            else:
                final_json["status"] = "failed"
                final_json["details"] = "0 rows deleted"
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(final_json))

        elif recived_message["key"] == "table":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM uav_type;
                                                    """
            cursor.execute(insert_query)
            connection_db.commit()
            cursor.close()
            final_json["status"] = "success"
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
        else:
            final_json["status"] = "failed"
            final_json["details"] = "wrong parameter in key"
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
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


channel.queue_declare(queue='add_uav_type_rpc', durable=False)
channel.basic_consume(queue='add_uav_type_rpc', on_message_callback=add_uav_type_rpc, auto_ack=True)
channel.queue_declare(queue='get_uav_type_rpc', durable=False)
channel.basic_consume(queue='get_uav_type_rpc', on_message_callback=get_uav_type_rpc, auto_ack=True)
channel.queue_declare(queue='delete_uav_type_rpc', durable=False)
channel.basic_consume(queue='delete_uav_type_rpc', on_message_callback=delete_uav_type_rpc, auto_ack=True)

channel.start_consuming()



