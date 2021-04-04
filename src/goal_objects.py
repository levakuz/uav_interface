#!/usr/bin/env python
import rospy
import json
from geometry_msgs.msg import Quaternion, Pose
from geometry_msgs.msg import Twist
import psycopg2
from psycopg2 import Error

import pika

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.33',
                                                               5672,
                                                               '/',
                                                               credentials))

channel = connection.channel()
print(channel)

channel.exchange_declare("geoposition", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("goals", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)

connection = psycopg2.connect(user="postgres",
                              # пароль, который указали при установке PostgreSQL
                              password="1111",
                              host="127.0.0.1",
                              port="5432",
                              database="postgres_db")

def pose_co_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"coordinates":{}, "angles":{}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    print(json_data)
    channel.basic_publish(
        exchange='geoposition',
        routing_key="CO",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def goal_co_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"coordinates":{}, "angles":{}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    channel.basic_publish(
        exchange='goals',
        routing_key="CO",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def trajectory_co_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = []
    point_data = {"coordinates": {}, "angles": {}}
    for point in data.waypoints:
        point_data["coordinates"]["x"] = point.position.x
        point_data["coordinates"]["y"] = point.position.y
        point_data["coordinates"]["z"] = point.position.z
        point_data["angles"]["x"] = point.orientation.x
        point_data["angles"]["y"] = point.orientation.y
        point_data["angles"]["z"] = point.orientation.z
        point_data["angles"]["w"] = point.orientation.w
        json_data.append(point_data)
    channel.basic_publish(
        exchange='trajectory',
        routing_key="CO",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def cmd_vel_co_callback(data):
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    vel_data = {"linear": {}, "angular": {}}
    vel_data["linear"]["x"] = data.linear.x
    vel_data["linear"]["y"] = data.linear.y
    vel_data["linear"]["z"] = data.linear.z
    vel_data["angular"]["x"] = data.angular.x
    vel_data["angular"]["y"] = data.angular.y
    vel_data["angular"]["z"] = data.angular.z
    channel.basic_publish(
        exchange='velocity',
        routing_key="CO",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def listener():
    rospy.init_node('goal_objects_interface_listener', anonymous=True)
    rospy.Subscriber("/sim_target/global_position", Pose, pose_co_callback)
    rospy.Subscriber("/sim_target/waypoint", Pose, goal_co_callback)
    rospy.Subscriber("/sim_target/waypoints_array", Pose, trajectory_co_callback)
    rospy.Subscriber("/sim_target/cmd_vel", Twist, cmd_vel_co_callback)
    rospy.spin()


if __name__ == '__main__':
    listener()