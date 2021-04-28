import rospy
import json
from geometry_msgs.msg import Quaternion, PoseStamped
from sensor_msgs.msg import BatteryState
from mavros_msgs.msg import Altitude
from sensor_msgs.msg import NavSatFix
import pika
import psycopg2
from psycopg2 import Error
import datetime
import multiprocessing as mp
# import threading

class UAV_listener(object):
    """docstring"""

    def __init__(self, uav_id, username_rmq, password_rmq, ip_rmq, db_username, db_password, db_ip, db_name, time_interval):
        """Constructor"""
        self.username_rmq = username_rmq
        self.password_rmq = password_rmq
        self.ip_rmq = ip_rmq
        self.uav_id = uav_id
        self.db_username = db_username
        self.db_password = db_password
        self.db_ip = db_ip
        self.db_name = db_name
        self.time_interval = time_interval
        self.old_voltage_data = self.old_local_data = self.altitude_old_data = self.old_global_data = 0
        self.prev_time_local = datetime.datetime.now()
        self.prev_time_altitude = self.prev_time_voltage = self.prev_time_global = self.prev_time_local
        self.credentials = pika.PlainCredentials(self.username_rmq, self.password_rmq)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.ip_rmq,
                                                                       5672,
                                                                       '/',
                                                                       self.credentials, blocked_connection_timeout=0,
                                                                       heartbeat=0))

        self.channel = self.connection.channel()

        self.channel.exchange_declare("geoposition", exchange_type='topic', passive=False,
                                 durable=False, auto_delete=False, arguments=None)
        self.channel.exchange_declare("battery", exchange_type='topic', passive=False,
                                 durable=False, auto_delete=False, arguments=None)
        self.channel.exchange_declare("altitude", exchange_type='topic', passive=False,
                                 durable=False, auto_delete=False, arguments=None)
        self.connection_db = psycopg2.connect(user=self.db_username,
                                         password=self.db_password,
                                         host=self.db_ip,
                                         port="5432",
                                         database=self.db_name)

    def local_position_uav_callback(self, data):
        time = datetime.datetime.now()
        # print(time)
        # rospy.loginfo(data)
        # rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
        json_data = {"id": self.uav_id, "coordinates": {}, "angles": {}}
        json_data["coordinates"]["x"] = '{:.2f}'.format(data.pose.position.x)
        json_data["coordinates"]["y"] = '{:.2f}'.format(data.pose.position.y)
        json_data["coordinates"]["z"] = '{:.2f}'.format(data.pose.position.z)
        json_data["angles"]["x"] = '{:.2f}'.format(data.pose.orientation.x)
        json_data["angles"]["y"] = '{:.2f}'.format(data.pose.orientation.y)
        json_data["angles"]["z"] = '{:.2f}'.format(data.pose.orientation.z)
        json_data["angles"]["w"] = '{:.2f}'.format(data.pose.orientation.w)
        if (time - self.prev_time_local).total_seconds() > self.time_interval:
            self.prev_time_local = time
            if self.old_local_data != json_data:
                # print("here")
                try:
                    self.channel.basic_publish(
                        exchange='geoposition',
                        routing_key="UAV_local",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                except Exception as e:
                    print(e)
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.ip_rmq,
                                                                                        5672,
                                                                                        '/',
                                                                                        self.credentials,
                                                                                        blocked_connection_timeout=0,
                                                                                        heartbeat=0))
                    self.channel = self.connection.channel()
                    self.channel.basic_publish(
                        exchange='geoposition',
                        routing_key="UAV_local",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                try:
                    cursor = self.connection_db.cursor()
                    insert_query = """ UPDATE uav_dynamic_params SET coords = '{}' WHERE time = '{}' AND id = {};
                    """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), self.uav_id)
                    cursor.execute(insert_query)
                    self.connection_db.commit()
                    count = cursor.rowcount
                except Error as e:
                    print("error", e)
                    count = 0
                    self.connection_db.rollback()
                # print(count, "Succesfull update")
                if count == 0:
                    try:
                        cursor = self.connection_db.cursor()
                        insert_query = """ INSERT INTO uav_dynamic_params (time, id, coords)
                                                                   VALUES (%s, %s, %s)"""
                        item_tuple = (time.time().strftime("%H:%M:%S"), self.uav_id, json.dumps(json_data))
                        cursor.execute(insert_query, item_tuple)
                        self.connection_db.commit()
                    except Error as e:
                        print("error", e)
                        count = 0
                        self.connection_db.rollback()

                        # count = cursor.rowcount
                    # print(count, "Succesfull INSERT")
                self.old_local_data = json_data
        # rospy.sleep(5)

    def global_position_uav_callback(self, data):
        time = datetime.datetime.now()
        # rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
        json_data = {}
        json_data["id"] = self.uav_id
        json_data["lattitude"] = '{:.2f}'.format(data.latitude)
        json_data["longtitude"] = '{:.2f}'.format(data.longitude)
        json_data["altitude"] = '{:.2f}'.format(data.altitude)
        if (time - self.prev_time_global).total_seconds() > self.time_interval:
            self.prev_time_global = time
            if self.old_global_data != json_data:
                try:
                    self.channel.basic_publish(
                        exchange='geoposition',
                        routing_key="UAV_local",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                except Exception as e:
                    print(e)
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.ip_rmq,
                                                                                        5672,
                                                                                        '/',
                                                                                        self.credentials,
                                                                                        blocked_connection_timeout=0,
                                                                                        heartbeat=0))
                    self.channel = self.connection.channel()
                    self.channel.basic_publish(
                        exchange='geoposition',
                        routing_key="UAV_local",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                try:
                    cursor = self.connection_db.cursor()
                    insert_query = """ UPDATE uav_dynamic_params SET global_coords = '{}' WHERE time = '{}' AND id = {};
                            """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), self.uav_id)
                    cursor.execute(insert_query)
                    self.connection_db.commit()
                    count = cursor.rowcount
                    # print(count, "Succesfull update")
                except Error as e:
                    print("error", e)
                    count = 0
                    self.connection_db.rollback()
                if count == 0:
                    try:
                        cursor = self.connection_db.cursor()
                        insert_query = """ INSERT INTO uav_dynamic_params (time, id, global_coords)
                                                                   VALUES (%s, %s, %s)"""
                        item_tuple = (time.time().strftime("%H:%M:%S"), self.uav_id, json.dumps(json_data))
                        cursor.execute(insert_query, item_tuple)
                        self.connection_db.commit()
                    except Error as e:
                        print("error", e)
                        count = 0
                        self.connection_db.rollback()
                    # count = cursor.rowcount
                    # print(count, "Succesfull update")
            self.old_global_data = json_data
        # rospy.sleep(5)

    def altitude_uav_callback(self,data):
        print(self.uav_id)
        time = datetime.datetime.now()
        # rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
        altitude = '{:.2f}'.format(data.amsl)
        json_data = {"id": self.uav_id, "altitude": altitude}
        if (time - self.prev_time_altitude).total_seconds() > self.time_interval:
            self.prev_time_altitude = time
            if self.altitude_old_data != altitude:
                try:
                    self.channel.basic_publish(
                        exchange='altitude',
                        routing_key="UAV_global",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                except Exception as e:
                    print(e)
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.ip_rmq,
                                                                                        5672,
                                                                                        '/',
                                                                                        self.credentials,
                                                                                        blocked_connection_timeout=0,
                                                                                        heartbeat=0))
                    self.channel = self.connection.channel()
                    self.channel.basic_publish(
                        exchange='altitude',
                        routing_key="UAV_global",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                try:
                    cursor = self.connection_db.cursor()
                    insert_query = """ UPDATE uav_dynamic_params SET altitude = '{}' WHERE time = '{}' AND id = {};
                                """.format(altitude, time.time().strftime("%H:%M:%S"), self.uav_id)
                    cursor.execute(insert_query)
                    self.connection_db.commit()
                    count = cursor.rowcount
                    # print(count, "Succesfull update altitude")
                except Error as e:
                    print("error", e)
                    count = 0
                    self.connection_db.rollback()
                if count == 0:
                    try:
                        cursor = self.connection_db.cursor()
                        insert_query = """ INSERT INTO uav_dynamic_params (time, id, altitude)
                                                                       VALUES (%s, %s, %s)"""
                        item_tuple = (time.time().strftime("%H:%M:%S"), self.uav_id, altitude)
                        cursor.execute(insert_query, item_tuple)
                        self.connection_db.commit()
                    except Error as e:
                        print("error", e)
                        self.connection_db.rollback()
                    # count = cursor.rowcount
                    # print(count, "Succesfull INSERT altitude")
            self.altitude_old_data = altitude
        # rospy.sleep(5)

    def voltage_uav_callback(self, data):
        time = datetime.datetime.now()
        # rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.voltage)
        voltage = '{:.0f}'.format(data.voltage)
        json_data = {"id": self.uav_id, "battery": voltage}
        # print(json_data)
        if (time - self.prev_time_voltage).total_seconds() > self.time_interval:
            self.prev_time_voltage = time
            if self.old_voltage_data != voltage:
                try:
                    self.channel.basic_publish(
                        exchange='battery',
                        routing_key="UAV_global",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                except Exception as e:
                    print(e)
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.ip_rmq,
                                                                                        5672,
                                                                                        '/',
                                                                                        self.credentials,
                                                                                        blocked_connection_timeout=0,
                                                                                        heartbeat=0))
                    self.channel = self.connection.channel()
                    self.channel.basic_publish(
                        exchange='battery',
                        routing_key="UAV_global",
                        body=json.dumps(json_data),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
                try:
                    cursor = self.connection_db.cursor()
                    insert_query = """ UPDATE uav_dynamic_params SET battery = '{}' WHERE time = '{}' AND id = {};
                            """.format(voltage, time.time().strftime("%H:%M:%S"), self.uav_id)
                    cursor.execute(insert_query)
                    self.connection_db.commit()
                    count = cursor.rowcount
                    # print(count, "Succesfull update")
                except Error as e:
                    print("error", e)
                    count = 0
                    self.connection_db.rollback()
                if count == 0:
                    try:
                        cursor = self.connection_db.cursor()
                        insert_query = """ INSERT INTO uav_dynamic_params (time, id, battery)
                                                                   VALUES (%s, %s, %s)"""
                        item_tuple = (time.time().strftime("%H:%M:%S"), self.uav_id, voltage)
                        cursor.execute(insert_query, item_tuple)
                        self.connection_db.commit()
                    except Error as e:
                        print("error", e)
                        self.connection_db.rollback()
                    # count = cursor.rowcount
                    # print(count, "Succesfull update")
                self.old_voltage_data = voltage
        # rospy.sleep(5)

    def listener(self):
        # rospy.Subscriber("/mavros/local_position/pose", PoseStamped, local_position_uav_callback)
        # rospy.Subscriber("/mavros/global_position/global", PoseStamped, global_position_uav_callback)
        # rospy.Subscriber("/mavros/battery", BatteryState, voltage_uav_callback)
        # rospy.Subscriber("/mavros/altitude", Altitude, altitude_uav_callback)
        rospy.Subscriber("/uav" + self.uav_id + "/mavros/local_position/pose", PoseStamped,
                         self.local_position_uav_callback)
        rospy.Subscriber("/uav" + self.uav_id + "/mavros/global_position/global", NavSatFix,
                         self.global_position_uav_callback)
        rospy.Subscriber("/uav" + self.uav_id + "/mavros/battery", BatteryState, self.voltage_uav_callback)
        rospy.Subscriber("/uav" + self.uav_id + "/mavros/altitude", Altitude, self.altitude_uav_callback)

        # p1 = mp.Process(
        #     target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/local_position/pose", PoseStamped, self.local_position_uav_callback))
        # p2 = mp.Process(
        #     target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/global_position/global", NavSatFix, self.global_position_uav_callback))
        # p3 = mp.Process(target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/battery", BatteryState, self.voltage_uav_callback))
        # p4 = mp.Process(target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/altitude", Altitude, self.altitude_uav_callback))
        # # spin() simply keeps python from exiting until this node is stopped
        # p1.start()
        # p2.start()
        # p3.start()
        # p4.start()
        # p1.join()
        # p2.join()
        # p3.join()
        # p4.join()

        # t1 = threading.Thread(
        #     target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/local_position/pose", PoseStamped, self.local_position_uav_callback))
        # t2 = threading.Thread(
        #     target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/global_position/global", NavSatFix, self.global_position_uav_callback))
        # t3 = threading.Thread(target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/battery", BatteryState, self.voltage_uav_callback))
        # t4 = threading.Thread(target=rospy.Subscriber("/uav" + self.uav_id + "/mavros/altitude", Altitude, self.altitude_uav_callback))
        # # spin() simply keeps python from exiting until this node is stopped
        # t1.start()
        # t2.start()
        # t3.start()
        # t4.start()
        # t1.join()
        # t2.join()
        # t3.join()
        # t4.join()

