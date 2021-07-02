import time
import pika
import psycopg2
import json
import random
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
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


def wind_velocity(wind_vel_data):
    """Option 1"""
    vel_data = {"velocity": wind_vel_data}
    print(vel_data)
    channel.basic_publish(
        exchange='environment',
        routing_key="wind_vel",
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
    print(vel_data)
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