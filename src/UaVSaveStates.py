#!/usr/bin/env python
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
import threading


class UavSaver(threading.Thread):

    def __init__(self, uav_id, db_username, db_password, db_ip, db_name):
        """Constructor"""
        threading.Thread.__init__(self)
        self.uav_id = uav_id
        self.db_username = db_username
        self.db_password = db_password
        self.db_ip = db_ip
        self.db_name = db_name
        self.old_voltage_data = self.old_local_data = self.altitude_old_data = self.old_global_data = 0

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
        json_data = {"id": self.uav_id, "lattitude": '{:.2f}'.format(data.latitude),
                     "longtitude": '{:.2f}'.format(data.longitude), "altitude": '{:.2f}'.format(data.altitude)}

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
                self.connection_db.rollback()
            # count = cursor.rowcount
            # print(count, "Succesfull update")
            self.old_global_data = json_data
        # rospy.sleep(5)

    def altitude_uav_callback(self,data):
        time = datetime.datetime.now()
        print(self.uav_id)
        # rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
        altitude = '{:.2f}'.format(data.amsl)
        json_data = {"id": self.uav_id, "altitude": altitude}
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

    # def listener(self):
        # rospy.Subscriber("/mavros/local_position/pose", PoseStamped, local_position_uav_callback)
        # rospy.Subscriber("/mavros/global_position/global", PoseStamped, global_position_uav_callback)
        # rospy.Subscriber("/mavros/battery", BatteryState, voltage_uav_callback)
        # rospy.Subscriber("/mavros/altitude", Altitude, altitude_uav_callback)
        # rospy.Subscriber("/uav" + self.uav_id + "/mavros/local_position/pose", PoseStamped,
        #                  self.local_position_uav_callback)
        # data = rospy.wait_for_message("/gazebo/model_states", ModelStates)
        # rospy.Subscriber("/uav" + self.uav_id + "/mavros/global_position/global", NavSatFix,
        #                  self.global_position_uav_callback)
        # rospy.Subscriber("/uav" + self.uav_id + "/mavros/battery", BatteryState, self.voltage_uav_callback)
        # rospy.Subscriber("/uav" + self.uav_id + "/mavros/altitude", Altitude, self.altitude_uav_callback)
        # self.channel.exchange_declare("save_data", exchange_type='fanout', passive=False,
        #                               durable=False, auto_delete=False, arguments=None)
        # result = self.channel.queue_declare(queue='', exclusive=True)
        # queue_name = result.method.queue

        # self.channel.queue_bind(exchange='save_data', queue=queue_name)
        # self.channel.basic_consume(
        #     queue=queue_name, on_message_callback=self.check_message, auto_ack=True)
        # self.channel.start_consuming()

    def save_data(self):
        print("here1")
        rospy.init_node('uav_dynamic_params_save', anonymous=True)
        local_pose = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/local_position/pose", PoseStamped)
        global_pose = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/global_position/global", NavSatFix)
        voltage = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/battery", BatteryState)
        altitude = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/altitude", Altitude)
        self.local_position_uav_callback(local_pose)
        self.global_position_uav_callback(global_pose)
        self.voltage_uav_callback(voltage)
        self.altitude_uav_callback(altitude)
        return

    def enable_rmq_listener(self):
        self.channel.exchange_declare("save_data", exchange_type='fanout', passive=False,
                                      durable=False, auto_delete=False, arguments=None)
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange='save_data', queue=queue_name)
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=self.check_message, auto_ack=True)
        self.channel.start_consuming()

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


