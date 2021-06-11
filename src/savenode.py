#!/usr/bin/env python
import rospy
import threading
from GoalSaveStates import GoalObjectsSaver
from UaVSaveStates import UavSaver
while not rospy.is_shutdown():
    goalsaves = GoalObjectsSaver("admin", "admin", '192.168.0.17', "postgres", "password", "192.168.0.17", "postgres")
    uavsaves = UavSaver(1, "admin", "admin", '192.168.0.17', "postgres", "password", "192.168.0.17", "postgres")
    t1 = threading.Thread(goalsaves.enable_rmq_listener())
    t2 = threading.Thread(uavsaves.enable_rmq_listener())
    t1.start()
    t2.start()
    t1.join()
    t2.join()
rospy.spin()