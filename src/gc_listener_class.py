import rospy
import json
from geometry_msgs.msg import Quaternion, PoseStamped, Twist
from rsm_msgs.msg import WaypointArray
import pika
import psycopg2
from psycopg2 import Error
import datetime


class CoListener:
    def __init__(self, name, co_id, username_rmq, password_rmq, ip_rmq):
        """Constructor"""
        self.name = name
        self.username_rmq = username_rmq
        self.password_rmq = password_rmq
        self.ip_rmq = ip_rmq
        self.co_id = co_id
        self.old_pose_data = self.old_goal_data = self.old_trajectory_data = self.old_velocity_data = 0
        self.credentials = pika.PlainCredentials(self.username_rmq, self.password_rmq)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.ip_rmq,
                                                                            5672,
                                                                            '/',
                                                                            self.credentials,
                                                                            blocked_connection_timeout=0,
                                                                            heartbeat=0))

        self.channel = self.connection.channel()

        self.channel.exchange_declare("geoposition", exchange_type='topic', passive=False,
                                 durable=False, auto_delete=False, arguments=None)
        self.channel.exchange_declare("goals", exchange_type='topic', passive=False,
                                 durable=False, auto_delete=False, arguments=None)

        self.connection_db = psycopg2.connect(user=self.db_username,
                                              password=self.db_password,
                                              host=self.db_ip,
                                              port="5432",
                                              database=self.db_name)

    def pose_co_callback(self, data):
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
        if self.old_pose_data != json_data:
            # print("here")
            try:
                self.channel.basic_publish(
                    exchange='geoposition',
                    routing_key="CO",
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
                    routing_key="CO",
                    body=json.dumps(json_data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ))
            # try:
            #     cursor = self.connection_db.cursor()
            #     insert_query = """ UPDATE co_dynamic_params SET position = '{}' WHERE time = '{}' AND id = {};
            #        """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), self.co_id)
            #     cursor.execute(insert_query)
            #     self.connection_db.commit()
            #     count = cursor.rowcount
            # except Error as e:
            #     print("error", e)
            #     count = 0
            #     self.connection_db.rollback()
            # # print(count, "Succesfull update")
            # if count == 0:
            #     try:
            #         cursor = self.connection_db.cursor()
            #         insert_query = """ INSERT INTO co_dynamic_params (time, id, position)
            #                                                       VALUES (%s, %s, %s)"""
            #         item_tuple = (time.time().strftime("%H:%M:%S"), self.co_id, json.dumps(json_data))
            #         cursor.execute(insert_query, item_tuple)
            #         self.connection_db.commit()
            #     except Error as e:
            #         print("error", e)
            #         count = 0
            #         self.connection_db.rollback()
            #
            #         # count = cursor.rowcount
            #     # print(count, "Succesfull INSERT")
            self.old_pose_data = json_data
        # rospy.sleep(5)

    def goal_co_callback(self, data):
        rospy.loginfo(rospy.get_caller_id() + "I heard %s", data)
        json_data = {"coordinates": {}, "angles": {}}
        json_data["coordinates"]["x"] = data.position.x
        json_data["coordinates"]["y"] = data.position.y
        json_data["coordinates"]["z"] = data.position.z
        json_data["angles"]["x"] = data.orientation.x
        json_data["angles"]["y"] = data.orientation.y
        json_data["angles"]["z"] = data.orientation.z
        json_data["angles"]["w"] = data.orientation.w
        self.channel.basic_publish(
            exchange='goals',
            routing_key="CO",
            body=json.dumps(json_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        if self.old_goal_data != json_data:
            # print("here")
            try:
                self.channel.basic_publish(
                    exchange='goals',
                    routing_key="CO",
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
                    exchange='goals',
                    routing_key="CO",
                    body=json.dumps(json_data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ))
            # try:
            #     cursor = self.connection_db.cursor()
            #     insert_query = """ UPDATE co_dynamic_params SET goal = '{}' WHERE time = '{}' AND id = {};
            #        """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), self.co_id)
            #     cursor.execute(insert_query)
            #     self.connection_db.commit()
            #     count = cursor.rowcount
            # except Error as e:
            #     print("error", e)
            #     count = 0
            #     self.connection_db.rollback()
            # # print(count, "Succesfull update")
            # if count == 0:
            #     try:
            #         cursor = self.connection_db.cursor()
            #         insert_query = """ INSERT INTO co_dynamic_params (time, id, goal)
            #                                                       VALUES (%s, %s, %s)"""
            #         item_tuple = (time.time().strftime("%H:%M:%S"), self.co_id, json.dumps(json_data))
            #         cursor.execute(insert_query, item_tuple)
            #         self.connection_db.commit()
            #     except Error as e:
            #         print("error", e)
            #         count = 0
            #         self.connection_db.rollback()

                    # count = cursor.rowcount
                # print(count, "Succesfull INSERT")
            self.old_goal_data = json_data
        # rospy.sleep(5)

    def trajectory_co_callback(self, data):
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
        if self.old_trajectory_data != json_data:
            # print("here")
            try:
                self.channel.basic_publish(
                    exchange='trajectory',
                    routing_key="CO",
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
                    exchange='trajectory',
                    routing_key="CO",
                    body=json.dumps(json_data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ))
            # try:
            #     cursor = self.connection_db.cursor()
            #     insert_query = """ UPDATE co_dynamic_params SET trajectory = '{}' WHERE time = '{}' AND id = {};
            #                """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), self.co_id)
            #     cursor.execute(insert_query)
            #     self.connection_db.commit()
            #     count = cursor.rowcount
            # except Error as e:
            #     print("error", e)
            #     count = 0
            #     self.connection_db.rollback()
            # # print(count, "Succesfull update")
            # if count == 0:
            #     try:
            #         cursor = self.connection_db.cursor()
            #         insert_query = """ INSERT INTO co_dynamic_params (time, id, trajectory)
            #                                                               VALUES (%s, %s, %s)"""
            #         item_tuple = (time.time().strftime("%H:%M:%S"), self.co_id, json.dumps(json_data))
            #         cursor.execute(insert_query, item_tuple)
            #         self.connection_db.commit()
            #     except Error as e:
            #         print("error", e)
            #         count = 0
            #         self.connection_db.rollback()

                    # count = cursor.rowcount
                # print(count, "Succesfull INSERT")
            self.old_trajectory_data = json_data
        # rospy.sleep(5)

    def cmd_vel_co_callback(self, data):
        rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.data)
        json_data = {"linear": {}, "angular": {}}
        json_data["linear"]["x"] = data.linear.x
        json_data["linear"]["y"] = data.linear.y
        json_data["linear"]["z"] = data.linear.z
        json_data["angular"]["x"] = data.angular.x
        json_data["angular"]["y"] = data.angular.y
        json_data["angular"]["z"] = data.angular.z
        if self.old_velocity_data != json_data:
            # print("here")
            try:
                self.channel.basic_publish(
                    exchange='velocity',
                    routing_key="CO",
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
                    exchange='velocity',
                    routing_key="CO",
                    body=json.dumps(json_data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ))
            # try:
            #     cursor = self.connection_db.cursor()
            #     insert_query = """ UPDATE co_dynamic_params SET velocity = '{}' WHERE time = '{}' AND id = {};
            #                """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), self.co_id)
            #     cursor.execute(insert_query)
            #     self.connection_db.commit()
            #     count = cursor.rowcount
            # except Error as e:
            #     print("error", e)
            #     count = 0
            #     self.connection_db.rollback()
            # # print(count, "Succesfull update")
            # if count == 0:
            #     try:
            #         cursor = self.connection_db.cursor()
            #         insert_query = """ INSERT INTO co_dynamic_params (time, id, velocity)
            #                                                               VALUES (%s, %s, %s)"""
            #         item_tuple = (time.time().strftime("%H:%M:%S"), self.co_id, json.dumps(json_data))
            #         cursor.execute(insert_query, item_tuple)
            #         self.connection_db.commit()
            #     except Error as e:
            #         print("error", e)
            #         count = 0
            #         self.connection_db.rollback()

                    # count = cursor.rowcount
                # print(count, "Succesfull INSERT")
            self.old_velocity_data = json_data
        # rospy.sleep(5)

    def listener(self):
        # rospy.Subscriber("/mavros/local_position/pose", PoseStamped, local_position_uav_callback)
        # rospy.Subscriber("/mavros/global_position/global", PoseStamped, global_position_uav_callback)
        # rospy.Subscriber("/mavros/battery", BatteryState, voltage_uav_callback)
        # rospy.Subscriber("/mavros/altitude", Altitude, altitude_uav_callback)
        rospy.Subscriber("/sim_" + self.name + self.co_id + "/waypoint", PoseStamped,
                         self.pose_co_callback)
        rospy.Subscriber("/sim_" + self.name + self.co_id + "/global_position", PoseStamped,
                         self.goal_co_callback)
        rospy.Subscriber("/sim_" + self.name + self.co_id + "/waypoints_array", WaypointArray, self.trajectory_co_callback)
        rospy.Subscriber("/sim_" + self.name + self.co_id + "/cmd_vel", Twist, self.cmd_vel_co_callback)
