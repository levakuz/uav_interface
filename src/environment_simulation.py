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

channel.exchange_declare("environment", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)


wind_vel_data = 5
wind_direction_x = 0
wind_direction_y = 0
wind_direction_z = 20
temperature = 25


def wind_direction_callback(wind_direction_x, wind_direction_y, wind_direction_z):
    """ Option 2"""
    vel_data = {"direction":{}}
    vel_data["velocity"] = wind_vel_data
    vel_data["direction"]["x"] = wind_direction_x
    vel_data["direction"]["y"] = wind_direction_y
    vel_data["direction"]["z"] = wind_direction_z
    try:
        cursor = connection_db.cursor()
        insert_query = """ INSERT INTO co_weapon (name, range_horizontal, range_vertical, rapidity)
            VALUES (%s, %s, %s, %s)"""
        item_tuple = (recived_message["name"], recived_message["range_horizontal"],
                      recived_message["range_vertical"], recived_message["rapidity"])
        cursor.execute(insert_query, item_tuple)
        connection_db.commit()
        cursor.close()
    except Error as e:
        print("error", e)

    channel.basic_publish(
        exchange='environment',
        routing_key="wind_direction",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def temperature_callback(temperature):
    temp_data = {"temperature": temperature}
    print(temp_data)
    channel.basic_publish(
        exchange='environment',
        routing_key="temperature",
        body=json.dumps(temp_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


while True:
    wind_direction_callback(wind_direction_x,wind_direction_y,wind_direction_z)
    wind_velocity(wind_vel_data)
    temperature_callback(temperature)
    time.sleep(20)

