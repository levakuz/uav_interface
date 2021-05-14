import time
import pika
import psycopg2
import json
import random
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()


channel.exchange_declare("UAV", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)


connection_db = psycopg2.connect(user="postgres",
                              password="password",
                              host="192.168.0.17",
                              port="5432",
                              database="postgres")


def find_timestamp(id, time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE time <= '{}' and id  = {} ORDER BY time DESC;
                                           """.format(time ,id)
    cursor.execute(insert_query)
    record = cursor.fetchone()
    print(record)
    final_json = {}
    # for record in records:
    # print(record)
    json_list = False
    print(record)
    if record:
        json_list = {'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        json_list["time"] = record[0].strftime("%H:%M:%S")
        if json_list["coords"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT coords FROM uav_dynamic_params WHERE time <= '{}' AND id = '{}' AND coords is not null ORDER BY time DESC;
                                                   """.format(time, id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            json_list["coords"] = record[0]
            cursor.close()
        # else:
            # json_list["coords"] = json_list["coords"][0]

        if json_list["altitude"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT altitude FROM uav_dynamic_params WHERE time <= '{}' AND id = '{}' AND altitude is not null ORDER BY time DESC;
                                                   """.format(time, id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            json_list["altitude"] = record[0]
            cursor.close()
        # else:
        #     json_list["altitude"] = json_list["altitude"][0]

        if json_list["global_coords"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT global_coords FROM uav_dynamic_params WHERE time <= '{}' AND id = '{}' AND global_coords is not null ORDER BY time DESC;
                                                   """.format(time, id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            cursor.close()
            json_list["global_coords"] = record


        if json_list["battery"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT battery FROM uav_dynamic_params WHERE time <= '{}' AND id = '{}' AND battery is not null ORDER BY time DESC;
                                                   """.format(time, id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            cursor.close()
            json_list["battery"] = record[0]
        # else:
        #     json_list["battery"] = json_list["battery"][0]

        print(json_list)
    return json_list


def find_timestamp_last(id):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE time >= '{}' and id  = {} ORDER BY time DESC;
                                           """.format("00:00" ,id)
    cursor.execute(insert_query)
    record = cursor.fetchone()
    cursor.close()
    final_json = {}
    # for record in records:
    # print(record)
    json_list = False
    if record:
        json_list = {'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        json_list["time"] = record[0].strftime("%H:%M:%S")
        if json_list["coords"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT coords FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND coords is not null ORDER BY time DESC;
                                                   """.format("00:00", id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            cursor.close()
            json_list["coords"] = record[0]
        # else:
            # json_list["coords"] = json_list["coords"][0]

        if json_list["altitude"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT altitude FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND altitude is not null ORDER BY time DESC;
                                                   """.format("00:00", id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            cursor.close()
            json_list["altitude"] = record[0]
        # else:
        #     json_list["altitude"] = json_list["altitude"][0]

        if json_list["global_coords"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT global_coords FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND global_coords is not null ORDER BY time DESC;
                                                   """.format("00:00", id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            cursor.close()
            json_list["global_coords"] = record


        if json_list["battery"] is None:
            cursor = connection_db.cursor()
            insert_query = """ SELECT battery FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND battery is not null ORDER BY time DESC;
                                                   """.format("00:00", id)
            cursor.execute(insert_query)
            record = cursor.fetchone()
            cursor.close()
            json_list["battery"] = record[0]
        # else:
        #     json_list["battery"] = json_list["battery"][0]

        print(json_list)
    return json_list


def show_uav_ids_rpc(ch, method, properties, body):
    print(body)

    print("here")
    cursor = connection_db.cursor()
    insert_query = """ SELECT DISTINCT id FROM uav_dynamic_params;
                                           """
    cursor.execute(insert_query)
    record = cursor.fetchall()
    cursor.close()
    print(json.dumps(record))
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                     body=json.dumps(record))


def uav_all_parametrs_rpc(ch, method, properties, body):
    final_json = {}
    message = json.loads(body)
    print(message)
    try:
        if message["id"]:
            if message["time"] == "last":
                final_json = find_timestamp_last(message["id"])

            else:
                final_json = find_timestamp(message["id"], message["time"])
    except KeyError:
        cursor = connection_db.cursor()
        insert_query = """ SELECT DISTINCT id FROM uav_dynamic_params;
                                           """
        cursor.execute(insert_query)
        record = cursor.fetchall()
        cursor.close()
        if message["time"] == "last":
            for i in record:
                jsonlist = find_timestamp_last(i[0])
                final_json[i[0]] = jsonlist
            print(final_json)
        else:
            for i in record:
                jsonlist = find_timestamp(i[0], message["time"])
                final_json[i[0]] = jsonlist
            print(final_json)
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                     body=json.dumps(final_json))


def uav_local_pose_rpc(ch, method, properties, body):
    message = json.loads(body)
    try:
        if message["time"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT DISTINCT id FROM uav_dynamic_params;
                                                       """
            cursor.execute(insert_query)
            record = cursor.fetchall()
            cursor.close()
            final_json = {}
            for i in record:
                print(i[0])
                cursor = connection_db.cursor()
                insert_query = """ SELECT coords, time FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND coords is not null  ORDER BY time DESC;
                                        """.format("17:30", i[0])
                cursor.execute(insert_query)
                records = cursor.fetchall()
                cursor.close()
                print(records[0])
                record = json.loads(records[0][0])
                record["time"] = records[0][1].strftime("%H:%M:%S")
                final_json[i[0]] = record
            print(final_json)
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except KeyError:
        if message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT coords, time FROM uav_dynamic_params WHERE id = '{}' AND coords is not null  ORDER BY time DESC;
                                    """.format(message["id"])
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            print(records[0][0])
            record = json.loads(records[0][0])
            record ["time"] = records[0][1].strftime("%H:%M:%S")
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(record))


def uav_global_pose_rpc(ch, method, properties, body):
    message = json.loads(body)
    try:
        if message["time"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT DISTINCT id FROM uav_dynamic_params;
                                                       """
            cursor.execute(insert_query)
            record = cursor.fetchall()
            cursor.close()
            final_json = {}
            for i in record:
                print(i[0])
                cursor = connection_db.cursor()
                insert_query = """ SELECT global_coords, time FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND global_coords is not null  ORDER BY time DESC;
                                        """.format(message["time"], i[0])
                cursor.execute(insert_query)
                records = cursor.fetchall()
                cursor.close()
                if records:
                    print(records)
                    record = json.loads(records[0][0])
                    record["time"] = records[0][1].strftime("%H:%M:%S")
                    final_json[i[0]] = record
            print(final_json)
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except KeyError:
        if message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT global_coords, time FROM uav_dynamic_params WHERE id = '{}' AND global_coords is not null  ORDER BY time DESC;
                                    """.format(message["id"])
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            print(records[0][0])
            record = json.loads(records[0][0])
            record["time"] = records[0][1].strftime("%H:%M:%S")
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(record))


def uav_altitude_rpc(ch, method, properties, body):
    message = json.loads(body)
    try:
        if message["time"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT DISTINCT id FROM uav_dynamic_params;
                                                       """
            cursor.execute(insert_query)
            record = cursor.fetchall()
            cursor.close()
            final_json = {}
            newrecord = {}
            for i in record:
                print(i[0])
                cursor = connection_db.cursor()
                insert_query = """ SELECT altitude, time FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND altitude is not null  ORDER BY time DESC;
                                        """.format(message["time"], i[0])
                cursor.execute(insert_query)
                records = cursor.fetchall()
                cursor.close()
                if records:
                    print(records)
                    newrecord["altitude"] = records[0][0]
                    newrecord["time"] = records[0][1].strftime("%H:%M:%S")
                    final_json[i[0]] = newrecord
            print(final_json)
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except KeyError:
        if message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT altitude, time FROM uav_dynamic_params WHERE id = '{}' AND altitude is not null  ORDER BY time DESC;
                                    """.format(message["id"])
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            print(records[0])
            record = {}
            record["altitude"] = records[0][0]
            record["time"] = records[0][1].strftime("%H:%M:%S")
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(record))


def uav_battery_rpc(ch, method, properties, body):
    message = json.loads(body)
    try:
        if message["time"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT DISTINCT id FROM uav_dynamic_params;
                                                       """
            cursor.execute(insert_query)
            record = cursor.fetchall()
            cursor.close()
            final_json = {}
            newrecord = {}
            for i in record:
                print(i[0])
                cursor = connection_db.cursor()
                insert_query = """ SELECT battery, time FROM uav_dynamic_params WHERE time >= '{}' AND id = '{}' AND battery is not null  ORDER BY time DESC;
                                        """.format(message["time"], i[0])
                cursor.execute(insert_query)
                records = cursor.fetchall()
                cursor.close()
                if records:
                    print(records)
                    newrecord["battery"] = records[0][0]
                    newrecord["time"] = records[0][1].strftime("%H:%M:%S")
                    final_json[i[0]] = newrecord
            print(final_json)
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except KeyError:
        if message["id"]:
            cursor = connection_db.cursor()
            insert_query = """ SELECT battery, time FROM uav_dynamic_params WHERE id = '{}' AND battery is not null  ORDER BY time DESC;
                                    """.format(message["id"])
            cursor.execute(insert_query)
            records = cursor.fetchall()
            cursor.close()
            print(records[0][0])
            record = {}
            record["battery"] = records[0][0]
            record ["time"] = records[0][1].strftime("%H:%M:%S")
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(record))


channel.queue_declare(queue='uav_local_pose_rpc', durable=False)
channel.queue_declare(queue='uav_global_pose_rpc', durable=False)
channel.queue_declare(queue='uav_altitude_rpc', durable=False)
channel.queue_declare(queue='uav_battery_rpc', durable=False)
channel.queue_declare(queue='uav_all_parametrs_rpc', durable=False)
channel.queue_declare(queue='show_uav_ids_rpc', durable=False)

channel.basic_consume(queue='uav_local_pose_rpc', on_message_callback=uav_local_pose_rpc, auto_ack=True)
channel.basic_consume(queue='uav_global_pose_rpc', on_message_callback=uav_global_pose_rpc, auto_ack=True)
channel.basic_consume(queue='uav_altitude_rpc', on_message_callback=uav_altitude_rpc, auto_ack=True)
channel.basic_consume(queue='uav_battery_rpc', on_message_callback=uav_battery_rpc, auto_ack=True)
channel.basic_consume(queue='uav_all_parametrs_rpc', on_message_callback=uav_all_parametrs_rpc, auto_ack=True)
channel.basic_consume(queue='show_uav_ids_rpc', on_message_callback=show_uav_ids_rpc, auto_ack=True)

channel.start_consuming()






