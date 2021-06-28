#!/usr/bin/env python
import rospy
from gazebo_msgs.srv import SpawnModel
from geometry_msgs.msg import Quaternion, Pose, Point
import pika
import json
import random
from Heightmap import Heightmap
import PathPlanner as pp

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',
                                                               5672,
                                                               '/',
                                                               credentials, blocked_connection_timeout=0, heartbeat=0))

channel = connection.channel()


def spawn_co_rpc(ch, method, properties, body):
    hm = Heightmap()
    hmap, height, width = hm.prepare_heightmap()
    map_handler = pp.PathPlanner(hmap, height, width)
    map_handler.gridmap_preparing()
    recived_message = json.loads(body)
    obstacles = map_handler.detect_obstacles()
    #print(map_handler.obstacles)
    # print(map_handler.map)
    try:
        topleft = recived_message["TL"]
        bottomright = recived_message["BR"]
        if recived_message["key"] == "id":
            rover_name = "p3at" + str(recived_message["id"])
            spawn_model_client = rospy.ServiceProxy('/gazebo/spawn_urdf_model', SpawnModel)
            spawn_model_client(rover_name, open("/home/levakuz/catkin/src/targets_path_planning/urdf/pioneer3at_1.urdf", 'r').read(),
                    "/rover", Pose(position=Point(-10, 6.0, 2), orientation=Quaternion(0, 0, 0, 0)), "world")
            final_json = {"status": "success"}
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
        elif recived_message["key"] == "array":
            for i in recived_message["id"]:
                rover_name = "p3at" + str(i)
                spawn_model_client = rospy.ServiceProxy('/gazebo/spawn_urdf_model', SpawnModel)
                spawnpoint_is_obstacle = True
                while spawnpoint_is_obstacle:  # Проверяем является ли точка препятствием
                    new_point = (str(int(random.uniform(int(topleft["x"]), int(bottomright["x"])))),
                                 str(int(random.uniform(int(topleft["y"]), int(bottomright["y"])))))
                    print("Checking")
                    if new_point not in map_handler.obstacles: # Если нет в list с препятствиями, то спавним
                        spawn_model_client(
                            rover_name,
                            open("/home/levakuz/catkin/src/targets_path_planning/urdf/pioneer3at_1.urdf", 'r').read(),
                            "/rover",
                            Pose(position=Point(int(new_point[0]), int(new_point[1]), 2),
                                 orientation=Quaternion(0, 0, 0, 0)), "world")
                        spawnpoint_is_obstacle = False
                        print("Done")

            final_json = {"status": "success"}
            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=json.dumps(final_json))
    except Exception as e:
        final_json = {"error": "error"}
        print(e)
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(final_json))


channel.queue_declare(queue='spawn_co_rpc', durable=False)
channel.basic_consume(queue='spawn_co_rpc', on_message_callback=spawn_co_rpc, auto_ack=True)

channel.start_consuming()