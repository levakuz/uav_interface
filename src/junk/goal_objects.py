#!/usr/bin/env python
import rospy
from src import GoalListenerClass

# def __init__(self, username_rmq, password_rmq, ip_rmq, db_username, db_password, db_ip, db_name, time_interval):

lisener = GoalListenerClass.GoalObjectsListener("admin", "admin", "localhost", "postgres", "password", "localhost", "postgres",
                                                20)
lisener.listener()
rospy.spin()