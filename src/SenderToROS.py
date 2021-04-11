#!/usr/bin/env python
import rospy
import  json
from std_msgs.msg import String
from geometry_msgs.msg import Quaternion, Pose
from std_msgs.msg import Float64
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


channel.queue_bind(exchange='goals', queue='CO_goals', routing_key='CO')

channel.queue_declare(queue='CO_goals', durable=False)


channel.queue_bind(exchange='environment', queue='wind_direction', routing_key='wind_direction')
channel.queue_bind(exchange='environment', queue='wind_velocity', routing_key='wind_velocity')
channel.queue_bind(exchange='environment', queue='temperature', routing_key='temperature')
channel.queue_bind(exchange='environment', queue='pressure', routing_key='pressure')


channel.queue_declare(queue='wind_direction', durable=False)
channel.queue_declare(queue='wind_velocity', durable=False)
channel.queue_declare(queue='temperature', durable=False)
channel.queue_declare(queue='pressure', durable=False)


channel.queue_bind(exchange='altitude', queue='UAV_altitude', routing_key='UAV_altitude')


channel.queue_declare(queue='UAV_altitude', durable=False)


connection_db = psycopg2.connect(user="postgres",
                              # пароль, который указали при установке PostgreSQL
                              password="1111",
                              host="127.0.0.1",
                              port="5432",
                              database="postgres_db")

time = ""


def goal_co_callback(ch, method, properties, body):
    pub = rospy.Publisher('/sim_target/waypoint', Pose, queue_size=10)
    rospy.init_node('goal_co_interface_pub', anonymous=True)
    data = Pose()
    data.position.x = body["coordinates"]["x"]
    data.position.y = body["coordinates"]["y"]
    data.position.z = body["coordinates"]["z"]
    data.orientation.x = body["angles"]["x"]
    data.orientation.y = body["angles"]["y"]
    data.orientation.z = body["angles"]["z"]
    data.orientation.w = body["angles"]["w"]
    pub.publish(data)


def wind_velocity(ch, method, properties, body):
    """Option 1"""
    pub = rospy.Publisher('/wind/velocity', Float64, queue_size=10)
    rospy.init_node('wind_velocity_interface_pub', anonymous=True)
    data = Float64()
    data.data = body["velocity"]
    pub.publish(data)

    cursor = connection_db.cursor()
    update_query = """Update environment set wind_vel = {} where time = {}""".format(data, time)
    cursor.execute(update_query)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Запись успешно обновлена")
    # Получить результат
    cursor.execute("SELECT * from environment")
    print("Результат", cursor.fetchall())


def wind_direction_callback(ch, method, properties, body):
    """ Option 2"""
    pub = rospy.Publisher('/wind/direction', Pose, queue_size=10)
    rospy.init_node('wind_direction_interface_pub', anonymous=True)
    data = Pose()
    data.velocity = body["velocity"]
    data.direction.x = body["direction"]["x"]
    data.direction.y = body["direction"]["y"]
    data.direction.z = body["direction"]["z"]
    pub.publish(data)

    cursor = connection_db.cursor()
    update_query = """Update environment set wind_coords = {} where time = {}""".format(data, time)
    cursor.execute(update_query)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Запись успешно обновлена")
    # Получить результат
    cursor.execute("SELECT * from environment")
    print("Результат", cursor.fetchall())


def temperature_callback(ch, method, properties, body):
    pub = rospy.Publisher('/temperature/temp_var', Float64, queue_size=10)
    rospy.init_node('temperature_interface_pub', anonymous=True)
    data = Float64()
    data.data = body["temperature"]
    pub.publish(data)

    cursor = connection_db.cursor()
    update_query = """Update environment set temp = {} where time = {}""".format(data, time)
    cursor.execute(update_query)
    connection_db.commit()
    count = cursor.rowcount
    print(count, "Запись успешно обновлена")
    # Получить результат
    cursor.execute("SELECT * from environment")
    print("Результат", cursor.fetchall())


def altitude_uav_callback(ch, method, properties, body):
    pub = rospy.Publisher('/mavros/altitude', Float64, queue_size=10)
    rospy.init_node('altitude_interface_pub', anonymous=True)
    data = Float64()
    data.data = body["altitude"]
    pub.publish(data)


channel.basic_consume(queue='CO_goals', on_message_callback=goal_co_callback, auto_ack=True)
channel.basic_consume(queue='wind_direction', on_message_callback=wind_direction_callback, auto_ack=True)
channel.basic_consume(queue='wind_velocity', on_message_callback=wind_velocity, auto_ack=True)
channel.basic_consume(queue='temperature', on_message_callback=temperature_callback, auto_ack=True)
channel.basic_consume(queue='UAV_altitude', on_message_callback=altitude_uav_callback, auto_ack=True)


if __name__ == '__main__':
    print(' [*] Waiting for messages. To exit press CTRL+C')
    rospy.spin()
    channel.start_consuming()
