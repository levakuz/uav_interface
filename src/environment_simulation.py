import time
import pika
import psycopg2
import json
import random
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()

channel = connection.channel()

channel.exchange_declare("velocity", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("direction", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("temperature", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)

wind_vel_data = 5
wind_direction_x = random.randrange(0, 200)
wind_direction_y = random.randrange(0, 200)
wind_direction_z = random.randrange(0, 100)
temperature = random.randrange(0, 40)


def wind_velocity(wind_vel_data):
    """Option 1"""
    vel_data = {"velocity": wind_vel_data}
    print(vel_data)
    channel.basic_publish(
        exchange='velocity',
        routing_key="wind",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def wind_direction_callback(wind_direction_x, wind_direction_y, wind_direction_z):
    """ Option 2"""
    vel_data = {"direction":{}}
    vel_data["velocity"] = wind_vel_data
    vel_data["direction"]["x"] = wind_direction_x
    vel_data["direction"]["y"] = wind_direction_y
    vel_data["direction"]["z"] = wind_direction_z
    channel.basic_publish(
        exchange='direction',
        routing_key="wind",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def temperature_callback(temperature):
    temp_data = {"temperature": temperature}
    channel.basic_publish(
        exchange='temperature',
        routing_key="",
        body=json.dumps(temp_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


while True:
    wind_direction_callback(wind_direction_x,wind_direction_y,wind_direction_z)
    wind_velocity(wind_vel_data)
    temperature_callback(temperature)
    time.sleep(60)
