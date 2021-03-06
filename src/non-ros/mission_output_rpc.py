import time
import pika
import psycopg2
import json
from psycopg2 import Error

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
                                                               5672,
                                                               '/',
                                                               credentials, blocked_connection_timeout=0, heartbeat=0))

channel = connection.channel()

connection_db = psycopg2.connect(user="postgres",
                                 password="password",
                                 host="192.168.1.65",
                                 port="5432",
                                 database="postgres")


def add_mission_output_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message = {}
    final_json = {}
    try:
        cursor = connection_db.cursor()
        insert_query = """ INSERT INTO mission_output (id, time_zero, uavs) VALUES ( %s, %s, %s);
        """
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


def delete_mission_output__rpc(ch, method, properties, body):
    recived_message = json.loads(body)

    final_json = {}
    try:
        if recived_message["key"] == "id":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM mission_output WHERE id = '{}';
                                                    """.format(recived_message["id"])
            cursor.execute(insert_query)
            connection_db.commit()
            count = cursor.rowcount
            cursor.close()
            if count != 0:
                final_json["status"] = "success"
            else:
                final_json["status"] = "error"
                final_json["details"] = "0 rows were deleted"
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))

        elif recived_message["key"] == "table":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM mission_output;
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


def get_mission_output_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM mission_output WHERE id = '{}';
                                                    """.format(recived_message["id"])
            cursor.execute(insert_query)
            connection_db.commit()
            records = cursor.fetchall()
            cursor.close()
            if records:
                final_json = {}
                final_json["id"] = records[0][0]
                final_json["time_zero"] = records[0][1]
                final_json["uavs"] = records[0][2]
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(final_json))
            else:
                status_message = {"status": "Not found", "details": "No input of mission with such id was found"}
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(status_message))
    except KeyError:
        try:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM mission_output;
                                                   """
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            if records:
                print(records)
                final_json = {}
                for record in records:
                    final_json = {}
                    final_json[record[0]] = {}
                    final_json[record[0]]["id"] = record[0]
                    final_json[record[0]]["time_zero"] = record[1]
                    final_json[record[0]]["uavs"] = record[2]
                print(json.dumps(final_json))
                ch.basic_publish(exchange='',
                                 routing_key=properties.reply_to,
                                 properties=pika.BasicProperties(correlation_id= \
                                                                     properties.correlation_id),
                                 body=json.dumps(final_json))
            else:
                status_message = {"status": "Not found", "details": "No mission input information was found"}
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


channel.queue_declare(queue='add_mission_output_rpc', durable=False)
channel.basic_consume(queue='add_mission_output_rpc', on_message_callback=add_mission_output_rpc, auto_ack=True)
channel.queue_declare(queue='delete_mission_output__rpc', durable=False)
channel.basic_consume(queue='delete_mission_output__rpc', on_message_callback=delete_mission_output__rpc, auto_ack=True)
channel.queue_declare(queue='get_mission_output_rpc', durable=False)
channel.basic_consume(queue='get_mission_output_rpc', on_message_callback=get_mission_output_rpc, auto_ack=True)

channel.start_consuming()

