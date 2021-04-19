#!/usr/bin/env python
import time

import rospy
import json
from geometry_msgs.msg import Quaternion, PoseStamped
from sensor_msgs.msg import BatteryState
from mavros_msgs.msg import Altitude

import pika
import psycopg2
from psycopg2 import Error
import datetime

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.71',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()
print(channel)

channel.exchange_declare("geoposition", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("battery", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("altitude", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)

connection_db = psycopg2.connect(user="postgres",
                              password="vfvfcdtnf",
                              host="127.0.0.1",
                              port="5432",
                              database="postgres_db")

old_local_data = {}
old_global_data = {}
old_voltage_data = 0
altitude_old_data = 0


def local_position_uav_callback(data):
    time = datetime.datetime.now().time().strftime("%H:%M:%S")
    # print(time)
    global old_local_data
    # rospy.loginfo(data)
    # rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = '{:.2f}'.format(data.pose.position.x)
    json_data["coordinates"]["y"] = '{:.2f}'.format(data.pose.position.y)
    json_data["coordinates"]["z"] = '{:.2f}'.format(data.pose.position.z)
    json_data["angles"]["x"] = '{:.2f}'.format(data.pose.orientation.x)
    json_data["angles"]["y"] = '{:.2f}'.format(data.pose.orientation.y)
    json_data["angles"]["z"] = '{:.2f}'.format(data.pose.orientation.z)
    json_data["angles"]["w"] = '{:.2f}'.format(data.pose.orientation.w)
    # print(json_data)

    if old_local_data != json_data:
        # print("here")
        channel.basic_publish(
                exchange='geoposition',
                routing_key="UAV_local",
                body=json.dumps(json_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                ))
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET coords = '{}' WHERE time = '{}' AND id = {};
        """.format(json.dumps(json_data), time, 1)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        print(count, "Succesfull update")
        if count == 0:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, coords)
                                                       VALUES (%s, %s, %s)"""
            item_tuple = (time, 1, json.dumps(json_data))
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
            count = cursor.rowcount
            print(count, "Succesfull INSERT")
        rospy.sleep(5)
    old_local_data = json_data




def global_position_uav_callback(data):
    time = datetime.datetime.now().time().strftime("%H:%M:%S")
    global old_global_data
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = '{:.2f}'.format(data.pose.position.x)
    json_data["coordinates"]["y"] = '{:.2f}'.format(data.pose.position.y)
    json_data["coordinates"]["z"] = '{:.2f}'.format(data.pose.position.z)
    json_data["angles"]["x"] = '{:.2f}'.format(data.pose.orientation.x)
    json_data["angles"]["y"] = '{:.2f}'.format(data.pose.orientation.y)
    json_data["angles"]["z"] = '{:.2f}'.format(data.pose.orientation.z)
    json_data["angles"]["w"] = '{:.2f}'.format(data.pose.orientation.w)
    if old_global_data != json_data:
        channel.basic_publish(
            exchange='geoposition',
            routing_key="UAV_local",
            body=json.dumps(json_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET coords = '{}' WHERE time = '{}' AND id = {};
                """.format(json.dumps(json_data), time, 1)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        print(count, "Succesfull update")
        if count == 0:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, coords)
                                                       VALUES (%s, %s, %s)"""
            item_tuple = (time, 1, json.dumps(json_data))
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
            count = cursor.rowcount
            print(count, "Succesfull update")
    old_global_data = json_data
    rospy.sleep(5)


# def global_position_uav_callback(data):
#     rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
#     json_data = {"coordinates": {}, "angles": {}}
#     json_data["coordinates"]["x"] = data.position.x
#     json_data["coordinates"]["y"] = data.position.y
#     json_data["coordinates"]["z"] = data.position.z
#     json_data["angles"]["x"] = data.orientation.x
#     json_data["angles"]["y"] = data.orientation.y
#     json_data["angles"]["z"] = data.orientation.z
#     json_data["angles"]["w"] = data.orientation.w
#     print(json_data)
#     channel.basic_publish(
#         exchange='geoposition',
#         routing_key="UAV_global",
#         body=json.dumps(json_data),
#         properties=pika.BasicProperties(
#             delivery_mode=2,
#         ))
#     time.sleep(10)


def voltage_uav_callback(data):
    time = datetime.datetime.now().time().strftime("%H:%M:%S")
    global old_voltage_data
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.voltage)
    voltage = '{:.0f}'.format(data.voltage)
    json_data = {"battery": voltage}
    # print(json_data)
    if old_voltage_data != voltage:
        print("*******************")
        print(voltage)
        print(old_voltage_data)
        print("************")
        channel.basic_publish(
            exchange='battery',
            routing_key="UAV_global",
            body=json.dumps(json_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET battery = '{}' WHERE time = '{}' AND id = {};
                """.format(voltage, time, 1)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        print(count, "Succesfull update")
        if count == 0:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, battery)
                                                       VALUES (%s, %s, %s)"""
            item_tuple = (time, 1, voltage)
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
            count = cursor.rowcount
            print(count, "Succesfull update")
    old_voltage_data = voltage
    rospy.sleep(5)


def altitude_uav_callback(data):
    time = datetime.datetime.now().time().strftime("%H:%M:%S")
    global altitude_old_data
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    altitude = '{:.1f}'.format(data.terrain)
    json_data = {"altitude": altitude}
    if altitude_old_data != altitude:
        channel.basic_publish(
            exchange='altitude',
            routing_key="UAV_global",
            body=json.dumps(json_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET altitude = '{}' WHERE time = '{}' AND id = {};
                    """.format(altitude, time, 1)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        print(count, "Succesfull update altitude")
        if count == 0:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, altitude)
                                                           VALUES (%s, %s, %s)"""
            item_tuple = (time, 1, altitude)
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
            count = cursor.rowcount
            print(count, "Succesfull INSERT altitude")
    altitude_old_data = altitude
    rospy.sleep(5)


def listener():
    rospy.init_node('uav_interface_listener', anonymous=True)
    rospy.Subscriber("/mavros/local_position/pose", PoseStamped, local_position_uav_callback)
    rospy.Subscriber("/mavros/global_position", PoseStamped, global_position_uav_callback)
    rospy.Subscriber("/mavros/battery", BatteryState, voltage_uav_callback)
    rospy.Subscriber("/mavros/altitude", Altitude, altitude_uav_callback)

    # spin() simply keeps python from exiting until this node is stopped
    rospy.spin()


if __name__ == '__main__':
    listener()