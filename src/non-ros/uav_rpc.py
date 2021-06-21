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


def add_uav_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    print(body)
    status_message = {}
    try:
        cursor = connection_db.cursor()
        insert_query = """ SELECT * FROM uav_role WHERE id = '{}';
                                    """.format(recived_message["uav_role"])
        cursor.execute(insert_query)
        record = cursor.fetchone()
        cursor.close()
        print(record)

        if record:
            uav_type = record[2]
            uav_role = record[0]
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav (tail_number, uav_type, fuel_resource, time_for_prepare, uav_role)
                                                                      VALUES (%s, %s, %s, %s, %s)"""
            item_tuple = (recived_message["tail_number"], uav_type, recived_message["fuel_resource"],
                          recived_message["time_for_prepare"], uav_role)
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
            cursor.close()
            status_message["status"] = "success"
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(status_message))
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM uav WHERE tail_number = '{}';
                                                """.format(recived_message["tail_number"])
            cursor.execute(insert_query)
            record = cursor.fetchone()
            print(record[0])
            cursor.close()
            ch.basic_publish(
                            exchange='uav_create',
                            routing_key="",
                            body=json.dumps({"status": "created", "id": record[0]}),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                            ))
        else:
            status_message["status"] = "error"
            status_message["details"] = "Not found uav role"
            channel.basic_publish(exchange='',
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


def get_uav_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["tail_number"]:

            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM uav WHERE tail_number = '{}';
                                    """.format(recived_message["tail_number"])
            cursor.execute(insert_query)
            record = cursor.fetchone()
            cursor.close()
            if record:
                print(record)
                final_json["id"] = record[-2]
                final_json["tail_number"] = record[0]
                final_json["uav_type"] = record[1]
                final_json["uav_role"] = record[-1]
                final_json["fuel_resource"] = record[2]
                final_json["time_for_prepare"] = record[3]
                print(final_json)
            else:
                final_json["status"] = "Not found"
                final_json["details"] = "No information was found"
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))

    except KeyError:
        try:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM uav;
                                                       """
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            print(records)
            final_json = {}
            if records:
                for record in records:
                    final_json[record[-2]] = {}
                    final_json[record[-2]]["tail_number"] = record[0]
                    final_json[record[-2]]["uav_type"] = record[1]
                    final_json[record[-2]]["uav_role"] = record[-1]
                    final_json[record[-2]]["fuel_resource"] = record[2]
                    final_json[record[-2]]["time_for_prepare"] = record[3]
                print(final_json)

                print(json.dumps(final_json))
            else:
                final_json["status"] = "Not found"
                final_json["details"] = "No information was found"
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
        status_message = {"status": "error", "error": e.pgcode}
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def delete_uav_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["key"] == "id":

            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM uav WHERE id = '{}';
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
            insert_query = """ DELETE FROM uav;
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
        status_message = {"status": "error", "error": e.pgcode}
        connection_db.rollback()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


channel.queue_declare(queue='add_uav_rpc', durable=False)
channel.basic_consume(queue='add_uav_rpc', on_message_callback=add_uav_rpc, auto_ack=True)
channel.queue_declare(queue='get_uav_rpc', durable=False)
channel.basic_consume(queue='get_uav_rpc', on_message_callback=get_uav_rpc, auto_ack=True)
channel.queue_declare(queue='delete_uav_rpc', durable=False)
channel.basic_consume(queue='delete_uav_rpc', on_message_callback=delete_uav_rpc, auto_ack=True)

channel.start_consuming()

