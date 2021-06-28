#!/usr/bin/env python
import rospy
import json
from std_msgs.msg import String
from geometry_msgs.msg import Quaternion, PoseStamped
from std_msgs.msg import Float64
from std_msgs.msg import Header
import psycopg2
from psycopg2 import Error
import pika
import datetime



credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()
print(channel)

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
channel.exchange_declare("environment", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("altitude", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)

channel.queue_declare(queue='CO_goals', durable=False)
channel.queue_declare(queue='wind_direction', durable=False)
channel.queue_declare(queue='wind_velocity', durable=False)
channel.queue_declare(queue='temperature', durable=False)
channel.queue_declare(queue='pressure', durable=False)
channel.queue_declare(queue='UAV_altitude', durable=False)
channel.queue_declare(queue='UAV_local_position', durable=False)

channel.queue_bind(exchange='goals', queue='CO_goals', routing_key='CO')
channel.queue_bind(exchange='goals', queue='UAV_local_position', routing_key='UAV')
channel.queue_bind(exchange='environment', queue='wind_direction', routing_key='wind_direction')
channel.queue_bind(exchange='environment', queue='wind_velocity', routing_key='wind_velocity')
channel.queue_bind(exchange='environment', queue='temperature', routing_key='temperature')
channel.queue_bind(exchange='environment', queue='pressure', routing_key='pressure')
channel.queue_bind(exchange='altitude', queue='UAV_altitude', routing_key='UAV_altitude')


connection_db = psycopg2.connect(user="postgres",
                              password="password",
                              host="192.168.1.65",
                              port="5432",
                              database="postgres_db")


def goal_co_callback(ch, method, properties, body):
    time = datetime.datetime.now().time()
    pub = rospy.Publisher('/sim_target/waypoint', PoseStamped, queue_size=10)
    # rospy.init_node('goal_co_interface_pub', anonymous=True)
    data = PoseStamped()
    data.position.x = body["coordinates"]["x"]
    data.position.y = body["coordinates"]["y"]
    data.position.z = body["coordinates"]["z"]
    data.orientation.x = body["angles"]["x"]
    data.orientation.y = body["angles"]["y"]
    data.orientation.z = body["angles"]["z"]
    data.orientation.w = body["angles"]["w"]
    pub.publish(data)
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO dynamic_params (time, params)
                                           VALUES (%s, %s)"""
    item_tuple = (time, body)
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")


def wind_velocity(ch, method, properties, body):
    """Option 1"""
    time = datetime.datetime.now().time()
    pub = rospy.Publisher('/wind/velocity', Float64, queue_size=10)
    # rospy.init_node('wind_velocity_interface_pub', anonymous=True)
    data = Float64()
    data.data = body["velocity"]
    pub.publish(data)
    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO environment (time, wind_vel)
                                           VALUES (%s, %s)"""
    item_tuple = (time, body["velocity"])
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")


def wind_direction_callback(ch, method, properties, body):
    """ Option 2"""
    time = datetime.datetime.now().time()
    pub = rospy.Publisher('/wind/direction', PoseStamped, queue_size=10)
    # rospy.init_node('wind_direction_interface_pub', anonymous=True)
    data = PoseStamped()
    data.velocity = body["velocity"]
    data.direction.x = body["direction"]["x"]
    data.direction.y = body["direction"]["y"]
    data.direction.z = body["direction"]["z"]
    pub.publish(data)

    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO environment (time, wind_coords)
                                           VALUES (%s, %s)"""
    item_tuple = (time, body)
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")


def temperature_callback(ch, method, properties, body):
    time = datetime.datetime.now().time()
    pub = rospy.Publisher('/temperature/temp_var', Float64, queue_size=10)
    # rospy.init_node('temperature_interface_pub', anonymous=True)
    data = Float64()
    data.data = body["temperature"]
    pub.publish(data)

    cursor = connection_db.cursor()
    insert_query = """ INSERT INTO environment (time, temp)
                                           VALUES (%s, %s)"""
    item_tuple = (time, body["temperature"])
    cursor.execute(insert_query, item_tuple)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Succesfull update")


def altitude_uav_callback(ch, method, properties, body):
    pub = rospy.Publisher('/mavros/altitude', Float64, queue_size=10)
    # rospy.init_node('altitude_interface_pub', anonymous=True)
    data = Float64()
    data.data = body["altitude"]
    pub.publish(data)


def uav_local_position_callback(ch, method, properties, body):
    rospy.init_node('uav_local_position', anonymous=True)
    print(body)
    body = json.loads(body)
    rate = rospy.Rate(20)
    print(body["coordinates"]["x"])
    pub = rospy.Publisher('/mavros/setpoint_position/local', PoseStamped, queue_size=20)

    data = PoseStamped()
    # data.header = Header()
    data.header.stamp = rospy.Time.now()
    data.header.frame_id = "map"
    data.pose.position.x = int(body["coordinates"]["x"])
    data.pose.position.y = int(body["coordinates"]["y"])
    data.pose.position.z = int(body["coordinates"]["z"])
    # data.pose.orientation.x = float(body["angles"]["x"])
    # data.pose.orientation.y = float(body["angles"]["y"])
    # data.pose.orientation.z = float(body["angles"]["z"])
    # data.pose.orientation.w = float(body["angles"]["w"])
    pub.publish(data)
    print(data)
    while not rospy.is_shutdown():
        pub.publish(data)
        rate.sleep()
        print(data)


channel.basic_consume(queue='CO_goals', on_message_callback=goal_co_callback, auto_ack=True)
channel.basic_consume(queue='wind_direction', on_message_callback=wind_direction_callback, auto_ack=True)
channel.basic_consume(queue='wind_velocity', on_message_callback=wind_velocity, auto_ack=True)
channel.basic_consume(queue='temperature', on_message_callback=temperature_callback, auto_ack=True)
channel.basic_consume(queue='UAV_altitude', on_message_callback=altitude_uav_callback, auto_ack=True)
channel.basic_consume(queue='UAV_local_position', on_message_callback=uav_local_position_callback, auto_ack=True)


if __name__ == '__main__':
    print(' [*] Waiting for messages. To exit press CTRL+C')
    # rospy.init_node('interface_publisher', anonymous=True)
    channel.start_consuming()
