#!/usr/bin/env python
import rospy
import json
from geometry_msgs.msg import Quaternion, Pose
from sensor_msgs.msg import BatteryState
from mavros_msgs.msg import Altitude

import pika
import psycopg2
from psycopg2 import Error

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.33',
                                                               5672,
                                                               '/',
                                                               credentials))

channel = connection.channel()
print(channel)

channel.exchange_declare("geoposition", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("battery", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("altitude", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)

connection = psycopg2.connect(user="postgres",
                              # пароль, который указали при установке PostgreSQL
                              password="1111",
                              host="127.0.0.1",
                              port="5432",
                              database="postgres_db")

def local_position_uav_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    channel.basic_publish(
        exchange='geoposition',
        routing_key="UAV_local",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def global_position_uav_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    channel.basic_publish(
        exchange='geoposition',
        routing_key="UAV_local",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def global_position_uav_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    channel.basic_publish(
        exchange='geoposition',
        routing_key="UAV_global",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def voltage_uav_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.voltage)
    json_data = {"charge": data.voltage}
    channel.basic_publish(
        exchange='battery',
        routing_key="UAV_global",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def altitude_uav_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"altitude": data.terrain}
    print(json_data)
    channel.basic_publish(
        exchange='altitude',
        routing_key="UAV_global",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def listener():
    rospy.init_node('uav_interface_listener', anonymous=True)
    rospy.Subscriber("/mavros/local_position", Pose, local_position_uav_callback)
    rospy.Subscriber("/mavros/global_position", Pose, global_position_uav_callback)
    rospy.Subscriber("/mavros/battery", BatteryState, voltage_uav_callback)
    rospy.Subscriber("/mavros/altitude", Altitude, altitude_uav_callback)

    # spin() simply keeps python from exiting until this node is stopped
    rospy.spin()


if __name__ == '__main__':
    listener()