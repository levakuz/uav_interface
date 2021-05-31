#!/usr/bin/env python
import rospy
from gazebo_msgs.srv import SpawnModel
from geometry_msgs.msg import Quaternion, Pose, Point
import pika
import json

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                               5672,
                                                               '/',
                                                               credentials, blocked_connection_timeout=0, heartbeat=0))

channel = connection.channel()


def spawn_co_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    try:
        if recived_message["key"] == "id":
            rover_name = "p3at" + str(recived_message["id"])
            spawn_model_client = rospy.ServiceProxy('/gazebo/spawn_urdf_model', SpawnModel)
            spawn_model_client(rover_name, open("/home/levakuz/catkin/src/targets_path_planning/urdf/pioneer3at_1.urdf", 'r').read(),
                    "/rover", Pose(position=Point(-10, 6.0, 2), orientation=Quaternion(0, 0, 0, 0)), "world")
            final_json = {"status": "success"}
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
        elif recived_message["key"] == "array":
            for i in recived_message["id"]:
                rover_name = "p3at" + str(i)
                spawn_model_client = rospy.ServiceProxy('/gazebo/spawn_urdf_model', SpawnModel)
                spawn_model_client(rover_name,
                                   open("/home/levakuz/catkin/src/targets_path_planning/urdf/pioneer3at_1.urdf", 'r').read(),
                                   "/rover", Pose(position=Point(-10, 6.0, 2), orientation=Quaternion(0, 0, 0, 0)), "world")
            final_json = {"status": "success"}
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except Exception:
        final_json = {"error": "error"}
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(final_json))


channel.queue_declare(queue='spawn_co_rpc', durable=False)
channel.basic_consume(queue='spawn_co_rpc', on_message_callback=spawn_co_rpc, auto_ack=True)

channel.start_consuming()
