#!/usr/bin/env python
import rospy
import json
from gazebo_msgs.msg import ModelStates
from geometry_msgs.msg import Quaternion, Pose
from geometry_msgs.msg import Twist
import psycopg2
from psycopg2 import Error
import datetime
import pika


class GoalObjectsListener:
    def __init__(self, username_rmq, password_rmq, ip_rmq):
        self.username_rmq = username_rmq
        self.password_rmq = password_rmq
        self.ip_rmq = ip_rmq
        self.prev_positions = self.prev_twists = []
        self.prev_time = datetime.datetime.now()
        self.credentials = pika.PlainCredentials(self.username_rmq, self.password_rmq)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.ip_rmq,
                                                                            5672,
                                                                            '/',
                                                                            self.credentials,
                                                                            blocked_connection_timeout=0,
                                                                            heartbeat=0))
        self.channel = self.connection.channel()

        self.channel.exchange_declare("CO", exchange_type='topic', passive=False,
                                 durable=False, auto_delete=False, arguments=None)
        self.connection_db = psycopg2.connect(user=self.db_username,
                                         password=self.db_password,
                                         host=self.db_ip,
                                         port="5432",
                                         database=self.db_name)

    def gazebo_co_callback(self, data):
        # print("here")
        # rospy.loginfo(rospy.get_caller_id() + "I heard %s", data.name)
        names = data.name
        positions = data.pose
        twists = data.twist
        if not self.prev_twists:
            self.prev_twists = twists
        if not self.prev_positions:
            self.prev_positions = positions
            # print(prev_twists)
        # print(positions[0])
        # print(names)
        new_names = {k: v for v, k in enumerate(names)}
        # for i in names:
        #     print(i.index)
        # print(new_names)
        heightmap_index = new_names["heightmap"]
        # print(twists[1].linear.z)
        # newdict= {"pose": {"position": {}, "orientation":{}}, "twist":{"angular": {}, "linear": {}}}
        # newdict = {}
        # print(newdict)
        for i in range(len(names)):
            if i != heightmap_index:
                # print(names[i])
                positions[i].position.x = round(positions[i].position.x, 2)
                positions[i].position.y = round(positions[i].position.y, 2)
                positions[i].position.z = round(positions[i].position.z, 2)

                positions[i].orientation.x = round(positions[i].orientation.x, 2)
                positions[i].orientation.y = round(positions[i].orientation.y, 2)
                positions[i].orientation.z = round(positions[i].orientation.z, 2)
                positions[i].orientation.w = round(positions[i].orientation.w, 2)

                twists[i].angular.x = round(twists[i].angular.x, 2)
                twists[i].angular.y = round(twists[i].angular.y, 2)
                twists[i].angular.z = round(twists[i].angular.z, 2)

                twists[i].linear.x = round(twists[i].linear.x, 2)
                twists[i].linear.y = round(twists[i].linear.y, 2)
                twists[i].linear.z = round(twists[i].linear.z, 2)

                if self.prev_twists[i] != twists[i]:

                    vel_data = {i: {"angular": {"x": twists[i].angular.x,
                                                "y": twists[i].angular.y,
                                                "z": twists[i].angular.z},
                                "linear": {"x": twists[i].linear.x,
                                           "y": twists[i].linear.y,
                                           "z": twists[i].linear.z}}}
                    # try:
                    #     cursor = self.connection_db.cursor()
                    #     insert_query = """ UPDATE co_dynamic_params SET vel = '{}' WHERE time = '{}' AND id = {};
                    #     """.format(json.dumps(vel_data), time.time().strftime("%H:%M:%S"), i)
                    #     cursor.execute(insert_query)
                    #     self.connection_db.commit()
                    #     count = cursor.rowcount
                    # except Error as e:
                    #     print("error", e)
                    #     count = 0
                    #     self.connection_db.rollback()
                    # if count == 0:
                    #     try:
                    #         cursor = self.connection_db.cursor()
                    #         insert_query = """ INSERT INTO co_dynamic_params (time, id, vel)
                    #                                                    VALUES (%s, %s, %s)"""
                    #         item_tuple = (time.time().strftime("%H:%M:%S"), i, json.dumps(vel_data))
                    #         cursor.execute(insert_query, item_tuple)
                    #         self.connection_db.commit()
                    #     except Error as e:
                    #         print("error", e)
                    #         self.connection_db.rollback()
                    try:
                        self.channel.basic_publish(
                            exchange='CO',
                            routing_key="velocity",
                            body=json.dumps(vel_data),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                            ))
                    except Exception as e:
                        print(e)
                        self.connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                                                            5672,
                                                                                            '/',
                                                                                            self.credentials,
                                                                                            blocked_connection_timeout=0,
                                                                                            heartbeat=0))
                        self.channel = self.connection.channel()
                        self.channel.basic_publish(
                            exchange='CO',
                            routing_key="velocity",
                            body=json.dumps(vel_data),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                            ))
                if self.prev_positions[i] != positions[i]:
                    pose_data = {i: {"position": {"x": positions[i].position.x,
                                                  "y": positions[i].position.y,
                                                  "z": positions[i].position.z},
                                     "orientation": {"x": positions[i].orientation.x,
                                                     "y": positions[i].orientation.y,
                                                     "z":positions[i].orientation.z,
                                                     "w":positions[i].orientation.w}}}
                    # try:
                    #     cursor = self.connection_db.cursor()
                    #     insert_query = """ UPDATE co_dynamic_params SET coords = '{}' WHERE time = '{}' AND id = {};
                    #                        """.format(json.dumps(pose_data), time.time().strftime("%H:%M:%S"), i)
                    #     cursor.execute(insert_query)
                    #     self.connection_db.commit()
                    #     count = cursor.rowcount
                    # except Error as e:
                    #     print("error", e)
                    #     count = 0
                    #     self.connection_db.rollback()
                    # if count == 0:
                    #     try:
                    #         cursor = self.connection_db.cursor()
                    #         insert_query = """ INSERT INTO co_dynamic_params (time, id, coords)
                    #                                                                       VALUES (%s, %s, %s)"""
                    #         item_tuple = (time.time().strftime("%H:%M:%S"), i, json.dumps(pose_data))
                    #         cursor.execute(insert_query, item_tuple)
                    #         self.connection_db.commit()
                    #     except Error as e:
                    #         print("error", e)
                    #     self.connection_db.rollback()
                    try:
                        self.channel.basic_publish(
                            exchange='CO',
                            routing_key="position",
                            body=json.dumps(vel_data),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                            ))
                    except Exception as e:
                        print(e)
                        self.connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                                                            5672,
                                                                                            '/',
                                                                                            self.credentials,
                                                                                            blocked_connection_timeout=0,
                                                                                            heartbeat=0))
                        self.channel = self.connection.channel()
                        self.channel.basic_publish(
                            exchange='CO',
                            routing_key="position",
                            body=json.dumps(pose_data),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                            ))
        self.prev_twists = twists
        self.prev_positions = positions

    def listener(self):
        rospy.init_node('goal_objects_interface_listener', anonymous=True)
        rospy.Subscriber("/gazebo/model_states", ModelStates, self.gazebo_co_callback)

#example
lisener = GoalObjectsListener("admin", "admin", "192.168.0.17", "postgres", "password", "192.168.0.17", "postgres", 20)
lisener.listener()
rospy.spin()