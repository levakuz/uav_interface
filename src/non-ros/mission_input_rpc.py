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

def add_mission_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    status_message = {}
    final_json = {}
    try:
        cursor = connection_db.cursor()
        insert_query = """ INSERT INTO mission (status, directive_time_secs, time_out_of_launches, 
        simultaneous_launch_number, reset_point, landing_point, uavs, payload, target_type, dest_poligon,
        targets_number, targets_coords, time_intervals) VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)\
        RETURNING id;"""
        val = (0, recived_message["directive_time_secs"], recived_message["time_out_of_launches"], \
              recived_message["simultaneous_launch_number"], recived_message["reset_point"], \
              recived_message["landing_point"], recived_message["uavs"], recived_message["payload"], \
              recived_message["target_type"], recived_message["dest_poligon"], recived_message["targets_number"], \
              recived_message["targets_coords"], recived_message["time_intervals"])
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
        id_of_new_row = cursor.fetchone()[0]
        status_message = {"status": "success", "id": id_of_new_row}
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

    except TypeError as e:
        print(e)
        status_message["status"] = "error"
        status_message["details"] = "wrong format"
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


def delete_mission_rpc(ch, method, properties, body):
    recived_message = json.loads(body)

    final_json = {}
    try:
        if recived_message["key"] == "id":
            cursor = connection_db.cursor()
            insert_query = """ DELETE FROM mission_input WHERE id = '{}';
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
            insert_query = """ DELETE FROM mission_input;
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


def get_mission_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    final_json = {}
    try:
        if recived_message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT * FROM mission_input WHERE id = '{}';
                                                    """.format(recived_message["id"])
            cursor.execute(insert_query)
            connection_db.commit()
            records = cursor.fetchall()
            cursor.close()
            if records:
                final_json["directive_time_secs"] = records[0][0]
                final_json["time_out_of_launches"] = records[0][1]
                final_json["simultaneous_launch_number"] = records[0][2]
                final_json["reset_point"] = records[0][3]
                final_json["landing_point"] = records[0][4]
                final_json["uavs"] = records[0][5]
                final_json["payload"] = records[0][6]
                final_json["target_type"] = records[0][7]
                final_json["dest_poligon"] = records[0][8]
                final_json["targets_number"] = records[0][9]
                final_json["targets_coords"] = records[0][10]
                final_json["time_intervals"] = records[0][11]
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
            insert_query = """ SELECT * FROM mission_input;
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
                    final_json[record[0]]["directive_time_secs"] = record[0]
                    final_json[record[0]]["time_out_of_launches"] = record[1]
                    final_json[record[0]]["simultaneous_launch_number"] = record[2]
                    final_json[record[0]]["reset_point"] = record[3]
                    final_json[record[0]]["landing_point"] = record[4]
                    final_json[record[0]]["uavs"] = record[5]
                    final_json[record[0]]["payload"] = record[6]
                    final_json[record[0]]["target_type"] = record[7]
                    final_json[record[0]]["dest_poligon"] = record[8]
                    final_json[record[0]]["targets_number"] = record[9]
                    final_json[record[0]]["targets_coords"] = record[10]
                    final_json[record[0]]["time_intervals"] = record[11]
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
    except TypeError as e:
        print(e)
        print("here")
        status_message = {"status": "error", "details": "wrong format"}
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))


channel.queue_declare(queue='add_mission_rpc', durable=False)
channel.basic_consume(queue='add_mission_rpc', on_message_callback=add_mission_rpc, auto_ack=True)
channel.queue_declare(queue='delete_mission_rpc', durable=False)
channel.basic_consume(queue='delete_mission_rpc', on_message_callback=delete_mission_rpc, auto_ack=True)
channel.queue_declare(queue='get_mission_rpc', durable=False)
channel.basic_consume(queue='get_mission_rpc', on_message_callback=get_mission_rpc, auto_ack=True)

channel.start_consuming()
