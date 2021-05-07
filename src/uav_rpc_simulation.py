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


def uav_local_pose_rpc(ch, method, properties, body):
    message = json.loads(body)

    cursor = connection_db.cursor()
    insert_query = """ SELECT coords, time FROM uav_dynamic_params WHERE id = '{}' AND coords is not null  ORDER BY time DESC;
                            """.format(message["id"])
    cursor.execute(insert_query)
    records = cursor.fetchall()
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

    cursor = connection_db.cursor()
    insert_query = """ SELECT global_coords, time FROM uav_dynamic_params WHERE id = '{}' AND global_coords is not null  ORDER BY time DESC;
                            """.format(message["id"])
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records[0][0])
    record = json.loads(records[0][0])
    record ["time"] = records[0][1].strftime("%H:%M:%S")
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                     body=json.dumps(record))


def uav_altitude_rpc(ch, method, properties, body):
    message = json.loads(body)

    cursor = connection_db.cursor()
    insert_query = """ SELECT altitude, time FROM uav_dynamic_params WHERE id = '{}' AND altitude is not null  ORDER BY time DESC;
                            """.format(message["id"])
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records[0][0])
    record = json.loads(records[0][0])
    record ["time"] = records[0][1].strftime("%H:%M:%S")
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                     body=json.dumps(record))


def uav_battery_rpc(ch, method, properties, body):
    message = json.loads(body)

    cursor = connection_db.cursor()
    insert_query = """ SELECT battery, time FROM uav_dynamic_params WHERE id = '{}' AND battery is not null  ORDER BY time DESC;
                            """.format(message["id"])
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records[0][0])
    record = json.loads(records[0][0])
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

channel.basic_consume(queue='uav_local_pose_rpc', on_message_callback=uav_local_pose_rpc, auto_ack=True)
channel.basic_consume(queue='uav_global_pose_rpc', on_message_callback=uav_global_pose_rpc, auto_ack=True)
channel.basic_consume(queue='uav_altitude_rpc', on_message_callback=uav_altitude_rpc, auto_ack=True)
channel.basic_consume(queue='uav_battery_rpc', on_message_callback=uav_battery_rpc, auto_ack=True)
channel.start_consuming()

