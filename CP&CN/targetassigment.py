#!/usr/bin/env python
import rospy
import subprocess
import pika
import json
import psycopg2
from psycopg2 import Error

def get_target_assigment_input():
    insert_query = """ SELECT co_type.name, co_type.length, co_type.width, co_type.height, mission.success_level,
     mission.accomplished,  FROM mission LEFT JOIN co_type ON co_type.weapon = mission.id WHERE co_type.id = '{}';
                                                         """.format(recived_message["target_type"])
    local_pose = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/local_position/pose", PoseStamped)
    global_pose = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/global_position/global", NavSatFix)
    voltage = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/battery", BatteryState)
    altitude = rospy.wait_for_message("/uav" + str(self.uav_id) + "/mavros/altitude", Altitude)
    print("here")
    cmd = ['./targetassigment']
    output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
    print(str(output))
    if "TargetsAssignmentStatus:finished" in str(output):
        with open('gsoutput.json', 'r', encoding='utf-8') as g:
            output_targets = json.loads(g)

#TODO переделать в ROS

