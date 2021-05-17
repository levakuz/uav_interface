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


def add_co_weapon_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    print(body)
    status_message = {}
    try:
        cursor = connection_db.cursor()
        insert_query = """ INSERT INTO co_weapon (name, range_horizontal, range_vertical, rapidity)
         VALUES (%s, %s, %s, %s)"""
        item_tuple = (recived_message["name"], recived_message["range_horizontal"],
                      recived_message["range_vertical"], recived_message["rapidity"])
        cursor.execute(insert_query, item_tuple)
        connection_db.commit()
        cursor.close()
        status_message["status"] = "success"
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
    except KeyError as e:
        print("error", e)
        status_message["status"] = "error"
        status_message["key"] = str(e)
        status_message["details"] = "No existing key"
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def get_co_weapon_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message ={}
    final_json = {}
    try:
        if recived_message["id"]:
            try:
                cursor = connection_db.cursor()
                insert_query = """ SELECT * FROM co_weapon WHERE id = '{}';
                            """.format(1)
                cursor.execute(insert_query)
                record = cursor.fetchone()
                cursor.close()
                print(record)
                if record:
                    print(record)
                    final_json["id"] = record[0]
                    final_json["name"] = record[1]
                    final_json["range_horizontal"] = record[2]
                    final_json["range_vertical"] = record[3]
                    final_json["rapidity"] = record[4]
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
        insert_query = """ SELECT * FROM co_weapon;
                                                          """
        cursor.execute(insert_query)
        records = cursor.fetchall()
        cursor.close()
        print(records)
        final_json = {}
        for record in records:
            final_json[record[0]] = {}
            final_json[record[0]]["name"] = record[1]
            final_json[record[0]]["range_horizontal"] = record[2]
            final_json[record[0]]["range_vertical"] = record[3]
            final_json[record[0]]["rapidity"] = record[4]
        print(final_json)

        print(json.dumps(final_json))
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(final_json))

    except TypeError:
        status_message["status"] = "error"
        status_message["details"] = "wrong format"
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def delete_co_weapon_rpc (ch, method, properties, body):
    recived_message = json.loads(body)
    status_message ={}
    final_json = {}
    try:
        if recived_message["key"] == "id":
            try:
                cursor = connection_db.cursor()
                insert_query = """ DELETE FROM co_weapon WHERE id = '{}';
                                        """.format(recived_message["id"])
                cursor.execute(insert_query)
                connection_db.commit()
                rows_deleted = cursor.rowcount
                connection_db.commit()
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
        elif recived_message["key"] == "table":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM co_weapon;
                                                    """
            cursor.execute(insert_query)
            connection_db.commit()
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


channel.queue_declare(queue='add_co_weapon_rpc', durable=False)
channel.basic_consume(queue='add_co_weapon_rpc', on_message_callback=add_co_weapon_rpc, auto_ack=True)
channel.queue_declare(queue='get_co_weapon_rpc', durable=False)
channel.basic_consume(queue='get_co_weapon_rpc', on_message_callback=get_co_weapon_rpc, auto_ack=True)
channel.queue_declare(queue='delete_co_weapon_rpc', durable=False)
channel.basic_consume(queue='delete_co_weapon_rpc', on_message_callback=delete_co_weapon_rpc, auto_ack=True)

channel.start_consuming()



