#!/usr/bin/env python
import rospy
import json
from std_msgs.msg import String
from geometry_msgs.msg import Quaternion, Pose
from std_msgs.msg import Float64
import psycopg2
from psycopg2 import Error
import pika
import datetime

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.105',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()
# print(channel)

channel.exchange_declare("geoposition", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("goals", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("velocity", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("direction", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("temperature", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("pressure", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("battery", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("goals", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)

connection_db = psycopg2.connect(user="postgres",
                              password="vfvfcdtnf",
                              host="127.0.0.1",
                              port="5432",
                              database="postgres_db")

# time = 0
def pose_co_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    print(json_data)
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                  VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(json_data))
    # update_query = """Update dynamic_params set params = {} where time = {}""".format(json_data, time)
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # print(time)
    # cursor.execute("SELECT * from dynamic_params WHERE time = (%s)", time)
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='geoposition',
        routing_key="CO",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def goal_co_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                      VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(json_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from dynamic_params where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='goals',
        routing_key="CO",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def trajectory_co_callback(data):
    time = datetime.datetime.now().time()
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
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                      VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(json_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from dynamic_params where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='trajectory',
        routing_key="CO",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def cmd_vel_co_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    vel_data = {"linear": {}, "angular": {}}
    vel_data["linear"]["x"] = data.linear.x
    vel_data["linear"]["y"] = data.linear.y
    vel_data["linear"]["z"] = data.linear.z
    vel_data["angular"]["x"] = data.angular.x
    vel_data["angular"]["y"] = data.angular.y
    vel_data["angular"]["z"] = data.angular.z
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                      VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(vel_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from dynamic_params where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='velocity',
        routing_key="CO",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def wind_velocity(data):
    """Option 1"""
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    vel_data = {"velocity": data.data}
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO environment (time, wind_vel)
                                      VALUES (%s, %s)"""
    item_tuple = (time,data.data)
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from environment where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='velocity',
        routing_key="wind",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def wind_direction_callback(data):
    """ Option 2"""
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    vel_data = {"direction": {}}
    vel_data["velocity"] = data.velocity
    vel_data["direction"]["x"] = data.direction.x
    vel_data["direction"]["y"] = data.direction.y
    vel_data["direction"]["z"] = data.direction.z
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO environment (time, wind_coords)
                                      VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(vel_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='direction',
        routing_key="wind",
        body=json.dumps(vel_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def temperature_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    temp_data = {"temperature": data}
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO environment (time, temp)
                                      VALUES (%s, %s)"""
    item_tuple = (time, data.data)
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='temperature',
        routing_key="",
        body=json.dumps(temp_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def local_position_uav_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                        VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(json_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from dynamic_params where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='geoposition',
        routing_key="UAV_local",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def global_position_uav_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    json_data = {"coordinates": {}, "angles": {}}
    json_data["coordinates"]["x"] = data.position.x
    json_data["coordinates"]["y"] = data.position.y
    json_data["coordinates"]["z"] = data.position.z
    json_data["angles"]["x"] = data.orientation.x
    json_data["angles"]["y"] = data.orientation.y
    json_data["angles"]["z"] = data.orientation.z
    json_data["angles"]["w"] = data.orientation.w
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                        VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(json_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from dynamic_params where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='geoposition',
        routing_key="UAV_global",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def voltage_uav_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    json_data = {"charge": data}
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                        VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(json_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from dynamic_params where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='battery',
        routing_key="UAV_battery",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def altitude_uav_callback(data):
    time = datetime.datetime.now().time()
    rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
    json_data = {"altitude": data}
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                        VALUES (%s, %s)"""
    item_tuple = (time, json.dumps(json_data))
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")
    # Get result
    # cursor.execute("SELECT * from dynamic_params where time = {}".format(time))
    # print("Result", cursor.fetchall())
    channel.basic_publish(
        exchange='altitude',
        routing_key="UAV_altitude",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def listener():
    # In ROS, nodes are uniquely named. If two nodes with the same
    # name are launched, the previous one is kicked off. The
    # anonymous=True flag means that rospy will choose a unique
    # name for our 'listener' node so that multiple listeners can
    # run simultaneously.
    rospy.init_node('listener', anonymous=True)
    rospy.Subscriber("/sim_target/global_position", Pose, pose_co_callback)
    rospy.Subscriber("/sim_target/waypoint", Pose, goal_co_callback)
    rospy.Subscriber("/sim_target/waypoints_array", Pose, trajectory_co_callback)
    rospy.Subscriber("/sim_target/cmd_vel", Pose, cmd_vel_co_callback)
    rospy.Subscriber("/wind/velocity", Float64, wind_velocity)
    rospy.Subscriber("/wind/direction", Pose, wind_direction_callback)
    rospy.Subscriber("/temperature/temp_var", Float64, temperature_callback)
    rospy.Subscriber("/mavros/local_position", Pose, local_position_uav_callback)
    rospy.Subscriber("/mavros/global_position", Pose, global_position_uav_callback)
    rospy.Subscriber("/mavros/battery", Float64, voltage_uav_callback)
    rospy.Subscriber("/mavros/altitude", Float64, altitude_uav_callback)

    # spin() simply keeps python from exiting until this node is stopped
    rospy.spin()


if __name__ == '__main__':
    listener()
    channel.start_consuming()