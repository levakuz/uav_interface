#!/usr/bin/env python
import json

import rospy
import threading
from UaVSaveStates import UavSaver
import pika
credentials = pika.PlainCredentials("admin", "admin")
connection = pika.BlockingConnection(pika.ConnectionParameters("192.168.1.65",
                                                                    5672,
                                                                    '/',
                                                                    credentials, blocked_connection_timeout=0,
                                                                    heartbeat=0))

channel = connection.channel()
uav_list = []


def check_save_message(ch, method, properties, body):
    global uav_list
    print("check_message")
    if json.loads(body)["request"] == "save":
        for i in uav_list:
            i.save_data()
            print(i)


def check_uav_create(ch, method, properties, body):
    global uav_list
    print("check_uav_create")
    if json.loads(body)["status"] == "created":
        print(json.loads(body)["id"])
        new_uav = UavSaver(json.loads(body)["id"],
                           "postgres",
                           "password",
                           "192.168.1.65",
                           "postgres")
        uav_list.append(new_uav)
        print(uav_list)


channel.exchange_declare("uav_create", exchange_type='fanout', passive=False,
                         durable=False, auto_delete=False, arguments=None)

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='save_data', queue=queue_name)

result1 = channel.queue_declare(queue='', exclusive=True)
uav_create_queue = result1.method.queue
channel.queue_bind(exchange='uav_create', queue=uav_create_queue)

channel.basic_consume(
    queue=queue_name, on_message_callback=check_save_message, auto_ack=True)
channel.basic_consume(
    queue=uav_create_queue, on_message_callback=check_uav_create, auto_ack=True)
channel.start_consuming()
