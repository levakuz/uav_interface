#!/usr/bin/env python
import rospy
import json
from geometry_msgs.msg import Quaternion, Pose
from std_msgs.msg import Float64

import pika
import psycopg2
from psycopg2 import Error


credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',
                                                               5672,
                                                               '/',
                                                               credentials))

channel = connection.channel()

channel.exchange_declare("velocity", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("direction", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("temperature", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)


connection = psycopg2.connect(user="postgres",
                              password="vfvfcdtnf",
                              host="127.0.0.1",
                              port="5432",
                              database="postgres_db")


def wind_velocity(data):
    """Option 1"""
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    vel_data = {"velocity": data.data}
    print(vel_data)
    channel.basic_publish(
        exchange='environment',
        routing_key="wind_velocity",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def wind_direction_callback(data):
    """ Option 2"""
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    vel_data = {"direction":{}}
    vel_data["velocity"] = data.velocity
    vel_data["direction"]["x"] = data.direction.x
    vel_data["direction"]["y"] = data.direction.y
    vel_data["direction"]["z"] = data.direction.z
    channel.basic_publish(
        exchange='environment',
        routing_key="wind_direction",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def temperature_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    temp_data = {"temperature": data.data}
    channel.basic_publish(
        exchange='environment',
        routing_key="temperature",
        body=json.dumps(temp_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def listener():
    # In ROS, nodes are uniquely named. If two nodes with the same
    # name are launched, the previous one is kicked off. The
    # anonymous=True flag means that rospy will choose a unique
    # name for our 'listener' node so that multiple listeners can
    # run simultaneously.
    rospy.init_node('environment_interface_listener', anonymous=True)
    rospy.Subscriber("/wind/velocity", Float64, wind_velocity)
    rospy.Subscriber("/wind/diection", Pose, wind_direction_callback)
    rospy.Subscriber("/temperature/temp_var", Float64, temperature_callback)
    rospy.spin()


if __name__ == '__main__':
    listener()
