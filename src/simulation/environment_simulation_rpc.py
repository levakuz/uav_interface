import time
import pika
import psycopg2
import json
import random
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',
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


def wind_velocity_rpc(ch, method, properties, body):
    vel_data = {"velocity": wind_vel_data}
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                     body=json.dumps(vel_data))


def wind_direction_rpc(ch, method, properties, body):
    vel_data = {"direction": {}}
    vel_data["velocity"] = wind_vel_data
    vel_data["direction"]["x"] = wind_direction_x
    vel_data["direction"]["y"] = wind_direction_y
    vel_data["direction"]["z"] = wind_direction_z
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                     body=json.dumps(vel_data))


def temperature_rpc(ch, method, properties, body):
    temp_data = {"temperature": temperature}
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                     body=json.dumps(temp_data))


channel.queue_declare(queue='wind_velocity_rpc', durable=False)
channel.queue_declare(queue='wind_direction_rpc', durable=False)
channel.queue_declare(queue='temperature_rpc', durable=False)
channel.basic_consume(queue='wind_velocity_rpc', on_message_callback=wind_velocity_rpc, auto_ack=True)
channel.basic_consume(queue='wind_direction_rpc', on_message_callback=wind_direction_rpc, auto_ack=True)
channel.basic_consume(queue='temperature_rpc', on_message_callback=temperature_rpc, auto_ack=True)
channel.start_consuming()