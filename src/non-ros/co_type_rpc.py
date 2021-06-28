import time
import pika
import psycopg2
import json
import random
from psycopg2 import Error
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()


connection_db = psycopg2.connect(user="postgres",
                              password="password",
                              host="localhost",
                              port="5432",
                              database="postgres")


def add_co_type_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    print(body)
    try:
        cursor = connection_db.cursor()
        insert_query = """ SELECT * FROM co_weapon WHERE id = '{}';
                                           """.format(recived_message["weapon"])
        cursor.execute(insert_query)
        records = cursor.fetchall()
        print(records)
        if records:
                cursor = connection_db.cursor()
                insert_query = """ INSERT INTO co_type 
                (name, max_vel, max_acc, min_acc, length, width, height, radius_of_turn, weapon)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                item_tuple = (recived_message["name"], recived_message["max_vel"], recived_message["max_acc"],
                              recived_message["min_acc"],
                              recived_message["length"], recived_message["width"], recived_message["height"],
                              recived_message["radius_of_turn"], recived_message["weapon"])
                cursor.execute(insert_query, item_tuple)
                connection_db.commit()
                status_message= {"status": "success"}
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(status_message))

        else:
            status_message= {"status": "error", "details": "No such weapon"}
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(status_message))
    except Error as e:
        print("error", e)
        status_message= {"status": "error", "error": e.pgcode}
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))
    except KeyError as e:
        print("error", e)
        status_message= {"status": "error", "key": str(e), "details": "No existing key"}
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))
    except TypeError:
        status_message = {"status": "error", "details": "wrong format"}
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def get_co_type_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM co_type WHERE id = '{}';
                                    """.format(recived_message["id"])
            cursor.execute(insert_query)
            record = cursor.fetchone()
            final_json = {}
            if record:
                print(record)
                final_json["id"] = record[0]
                final_json["name"] = record[1]
                final_json["max_acc"] = record[2]
                final_json["min_acc"] = record[3]
                final_json["length"] = record[4]
                final_json["width"] = record[5]
                final_json["height"] = record[6]
                final_json["radius_of_turn"] = record[7]
                final_json["weapon"] = record[8]
                print(final_json)
            else:
                final_json["status"] = "not found"
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except KeyError:
        try:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM co_type;
                                                       """
            cursor.execute(insert_query)
            records = cursor.fetchall()
            print(records)
            final_json = {}
            for record in records:
                final_json[record[0]] = {}
                final_json[record[0]]["id"] = record[0]
                final_json[record[0]]["name"] = record[1]
                final_json[record[0]]["max_acc"] = record[2]
                final_json[record[0]]["min_acc"] = record[3]
                final_json[record[0]]["length"] = record[4]
                final_json[record[0]]["width"] = record[5]
                final_json[record[0]]["height"] = record[6]
                final_json[record[0]]["radius_of_turn"] = record[7]
                final_json[record[0]]["weapon"] = record[8]
            print(final_json)

            print(json.dumps(final_json))
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
        except Error as e:
            print("error", e)
            status_message= {"status": "error", "error": e.pgcode}
            connection_db.rollback()
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(status_message))

    except TypeError:
        status_message = {"status": "error", "details": "wrong format"}
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))
    except Error as e:
        print("error", e)
        status_message = {"status": "error", "error": e.pgcode}
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def delete_co_type_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["key"] == "id":
            try:
                cursor = connection_db.cursor()
                insert_query = """ DELETE FROM co_type WHERE id = '{}';
                                        """.format(recived_message["id"])
                cursor.execute(insert_query)
                connection_db.commit()
                rows_deleted = cursor.rowcount
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
                status_message = {"status": "error", "error": e.pgcode}
                connection_db.rollback()
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(status_message))
        elif recived_message["key"] == "table":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM co_type;
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
        status_message= {"status": "error", "key": str(e), "details": "No key"}
        print(json.dumps(status_message))
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))

    except TypeError:
        status_message= {"status": "error", "details": "wrong format"}
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))
    except Error as e:
        print("error", e)
        status_message = {"status": "error", "error": e.pgcode}
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


channel.queue_declare(queue='add_co_type_rpc', durable=False)
channel.basic_consume(queue='add_co_type_rpc', on_message_callback=add_co_type_rpc, auto_ack=True)
channel.queue_declare(queue='get_co_type_rpc', durable=False)
channel.basic_consume(queue='get_co_type_rpc', on_message_callback=get_co_type_rpc, auto_ack=True)
channel.queue_declare(queue='delete_co_type_rpc', durable=False)
channel.basic_consume(queue='delete_co_type_rpc', on_message_callback=delete_co_type_rpc, auto_ack=True)

channel.start_consuming()


