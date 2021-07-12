import time
import pika
import psycopg2
import json
from psycopg2 import Error
from RpcClient import RpcClient
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
    new_msg = recived_message
    print(recived_message)
    status_message = {}
    final_json = {}
    print("here")
    try:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO mission (status, directive_time_secs, time_out_of_launches, 
            simultaneous_launch_number, reset_point, landing_point, uavs, payload, target_type, dest_poligon,
            targets_number, targets_coords, time_intervals, success_level) VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)\
            RETURNING id;"""
            val = (
                0,
                recived_message["directive_time_secs"],
                recived_message["time_out_of_launches"],
                recived_message["simultaneous_launch_number"],
                json.dumps(recived_message["reset_point"]),
                json.dumps(recived_message["landing_point"]),
                json.dumps(recived_message["uavs"]),
                json.dumps(recived_message["payload"]),
                json.dumps(recived_message["target_type"]),
                json.dumps(recived_message["destination"]["dest_poligon"]),
                recived_message["destination"]["targets_number"],
                json.dumps(recived_message["destination"]["targets_coords"]),
                json.dumps(recived_message["time_intervals"]),
                recived_message["success_level"]
            )
            cursor.execute(insert_query, val)
            connection_db.commit()
            count = cursor.rowcount
            # cursor.close()
            print("hewew")
            # channel.basic_publish(
            #     exchange='',
            #     routing_key="get_mission_params",
            #     body=body,
            #     properties=pika.BasicProperties(
            #         delivery_mode=2,
            #     ))
            id_of_new_row = cursor.fetchone()[0]
            status_message = {"status": "success", "id": id_of_new_row}
            # ch.basic_publish(exchange='',
            #                  routing_key=properties.reply_to,
            #                  properties=pika.BasicProperties(correlation_id= \
            #                                                      properties.correlation_id),
            #                  body=json.dumps(status_message))

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

    cursor = connection_db.cursor()
    insert_query = """ SELECT co_type.name, co_type.max_vel, co_type.min_acc, co_type.max_acc, co_type.length,
     co_type.width, co_type.height, co_type.radius_of_turn, co_weapon.range_horizontal, co_weapon.range_vertical,
      co_weapon.rapidity FROM co_type LEFT JOIN co_weapon ON co_type.weapon = co_weapon.id WHERE co_type.id = '{}';
                                                     """.format(recived_message["target_type"])
    cursor.execute(insert_query)
    connection_db.commit()
    records = cursor.fetchone()
    print(records)
    if records:
        new_msg["target_type"] = {
            "target_type_name": records[0],
            "max_speed": records[1],
            "min_acceleration": records[2],
            "max_acceleration": records[3],
            "length": records[4],
            "width": records[5],
            "height": records[6],
            "uturn_radius": records[7],
            "weapon": {
                "h_distance": records[8],
                "v_distance": records[9],
                "rate_of_fire": records[10]

            }
        }
        for i in recived_message["uavs"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT uav.tail_number, uav.fuel_resource, uav.time_for_prepare, uav_type.name,
             uav_type.min_vel, uav_type.max_vel, uav_type.min_vertical_vel_up, uav_type.max_vertical_vel_up,
              uav_type.min_vertical_vel_down, uav_type.max_vertical_vel_down, uav_type.cargo_type, uav_type.cargo_quantity,
               uav_type.fuel_consume, uav_type.radius_of_turn FROM uav LEFT JOIN uav_type ON uav.uav_type = uav_type.id
                WHERE uav.id = {};""".format(i)
            cursor.execute(insert_query)
            connection_db.commit()
            records = cursor.fetchone()
            print(" ")
            print(records)
            new_msg["uavs"] = []
            new_msg["uav_types"] = []
            new_uav_info = {
                "flight_number": records[0],
                "uav_type_name": records[3],
                "flight_hours_resource": records[1],
                "time_to_start_secs": records[2]
            }
            new_msg["uavs"].append(new_uav_info)
            if records[3] not in new_msg["uav_types"]:
                new_uav_type_info = {
                    "uav_type_name": records[3],
                    "min_h_speed": records[4],
                    "max_h_speed": records[5],
                    "min_v_speed_up": records[6],
                    "max_v_speed_up": records[7],
                    "min_v_speed_down": records[8],
                    "max_v_speed_down": records[9],
                    "payload_type_name": records[10],
                    "payload_number": records[11],
                    "resource_consumption_per_hour": records[12],
                    "uturn_radius": records[13]
                }
                new_msg["uav_types"].append(new_uav_type_info)
        MissionRpcOutput = RpcClient()
        result = MissionRpcOutput.call(json.dumps(recived_message), "get_goal_settings_input")
        result = json.loads(result)
        cursor = connection_db.cursor()
        insert_query = """ UPDATE mission set time_zero = %s,  uavs_to_mission = %s, accomplished = %s, status = 1 WHERE id = {};
        """.format(int(id_of_new_row))
        val = (result["time_zero"], json.dumps(result["uavs"]), int(result["accomplished"]))
        cursor.execute(insert_query, val)
        connection_db.commit()
        count = cursor.rowcount
        cursor.close()
        status_message = {
            "status": "success",
            "id": id_of_new_row,
            "accomplished": result["accomplished"],
            'time_zero': result["time_zero"],
            'uavs': result["uavs"]
        }
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(status_message))
    else:
        status_message = {"status":"error", "info": "no such co_type found"}
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
