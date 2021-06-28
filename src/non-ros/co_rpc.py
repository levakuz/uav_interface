import time
import pika
import psycopg2
import json
import random
from psycopg2 import Error

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()


connection_db = psycopg2.connect(user="postgres",
                              password="password",
                              host="192.168.1.65",
                              port="5432",
                              database="postgres")


def add_co_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    print(body)
    try:
        cursor = connection_db.cursor()
        insert_query = """ SELECT * FROM co_type WHERE id = '{}';
                                                """.format(recived_message["co_type"])
        cursor.execute(insert_query)
        record = cursor.fetchone()
        if record:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO co (co_type) VALUES ('{}');""".format(recived_message["co_type"])
            # item_tuple = (recived_message["co_type"])
            cursor.execute(insert_query)
            connection_db.commit()
            status_message = {"status": "success"}
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(status_message))
        else:
            status_message = {"status": "error", "details": "No such co_type"}
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
    except KeyError as e:
        print("error", e)
        status_message = {"status": "error", "key": str(e), "details": "No key was given"}
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


def get_co_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM co WHERE id = '{}';
                                    """.format(recived_message["id"])
            cursor.execute(insert_query)
            record = cursor.fetchone()
            if record:
                print(record)
                final_json["id"] = record[0]
                final_json["co_type"] = record[1]
                print(final_json)
            else:
                final_json = {"status": "Not found", "details": "No co with such id was found"}
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except KeyError:
        try:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM co;
                                                       """
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            print(records)
            final_json = {}
            if records:
                for record in records:
                    final_json[record[0]] = {}
                    final_json[record[0]]["co_type"] = record[1]
                print(final_json)
            else:
                final_json = {"status": "Not found", "details": "CO is empty"}
            print(json.dumps(final_json))
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

    except TypeError:
        status_message = {"status": "error", "details": "wrong format"}
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


def delete_co_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["key"] == "id":

            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM co WHERE id = '{}';
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
        elif recived_message["key"] == "table":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM co;
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
        status_message = {"status": "error", "key": str(e), "details": "No key"}
        print(json.dumps(status_message))
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
        status_message= {"status": "error", "error": e.pgcode}
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


channel.queue_declare(queue='add_co_rpc', durable=False)
channel.basic_consume(queue='add_co_rpc', on_message_callback=add_co_rpc, auto_ack=True)
channel.queue_declare(queue='get_co_rpc', durable=False)
channel.basic_consume(queue='get_co_rpc', on_message_callback=get_co_rpc, auto_ack=True)
channel.queue_declare(queue='delete_co_rpc', durable=False)
channel.basic_consume(queue='delete_co_rpc', on_message_callback=delete_co_rpc, auto_ack=True)

channel.start_consuming()

