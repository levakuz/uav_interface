#!/usr/bin/env python
import rospy
from gazebo_msgs.srv import SpawnModel
from geometry_msgs.msg import Quaternion, Pose, Point
import pika
import json
import random
# credentials = pika.PlainCredentials('admin', 'admin')
# connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
#                                                                5672,
#                                                                '/',
#                                                                credentials, blocked_connection_timeout=0, heartbeat=0))

# channel = connection.channel()
def spawn_co_rpc(ch, method, properties, body):
    recived_message = json.loads(body)
    topleft = recived_message["TL"]
    bottomright = recived_message["BR"]
    rover_name = "ral" + str(recived_message["id"])
    spawn_model_client = rospy.ServiceProxy('/gazebo/spawn_sdf_model', SpawnModel)
    spawn_model_client(rover_name,
                                       open("/home/defender/UAV_Swarm_gazebo/Autopilot/Tools/sitl_gazebo/models/ral_x6/ral_x6.sdf", 'r').read(),
                                       "/rover", Pose(position=Point(random.uniform(topleft["x"], bottomright["x"]),
                                                                     random.uniform(topleft["y"], bottomright["y"]), 2),
                                                      orientation=Quaternion(0, 0, 0, 0)), "world")
rover_name = "ral6"
print(rover_name)
spawn_model_client = rospy.ServiceProxy('/gazebo/spawn_sdf_model', SpawnModel)
print("here")
spawn_model_client(rover_name,
                                   open("/home/defender/UAV_Swarm_gazebo/Autopilot/Tools/sitl_gazebo/models/iris_dual_gps/iris_dual_gps.sdf", 'r').read(),
                                   "/rover", Pose(position=Point(-10, 6.0, 10), orientation=Quaternion(0, 0, 0, 0)), "world")