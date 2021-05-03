import time
import pika
import psycopg2
import json
import random
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()

channel.exchange_declare("geoposition", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("battery", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
channel.exchange_declare("altitude", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)

# connection_db = psycopg2.connect(user="postgres",
#                               password="password",
#                               host="192.168.0.17",
#                               port="5432",
#                               database="postgres")


localx = random.randrange(0, 200)
localy = random.randrange(0, 200)
localz = random.randrange(0, 100)
local_angle_x = random.randrange(0, 180)
local_angle_y = random.randrange(0, 180)
local_angle_z = random.randrange(0, 180)
local_angle_w = 0

lattitude = random.randrange(0,200)
longtitude = random.randrange(0, 200)

voltage = random.randrange(0, 50)
altitude = random.randrange(20, 50)

new_localx = localx
new_localy = localy
new_lattitude = lattitude
new_longtitude = longtitude


def send_locals(localx, localy, localz, local_angle_x, local_angle_y, local_angle_z, local_angle_w):
    uav_id = random.randrange(1,4)
    json_data = {"id": uav_id, "coordinates": {}, "angles": {}}
    if uav_id == 1:
        json_data["coordinates"]["x"] = '{:.2f}'.format(localx)
        json_data["coordinates"]["y"] = '{:.2f}'.format(localy)
        json_data["coordinates"]["z"] = '{:.2f}'.format(localz)
        json_data["angles"]["x"] = '{:.2f}'.format(local_angle_x)
        json_data["angles"]["y"] = '{:.2f}'.format(local_angle_y)
        json_data["angles"]["z"] = '{:.2f}'.format(local_angle_z)
        json_data["angles"]["w"] = '{:.2f}'.format(local_angle_w)
    else:
        json_data["coordinates"]["x"] = '{:.2f}'.format(localx + uav_id)
        json_data["coordinates"]["y"] = '{:.2f}'.format(localy + uav_id)
        json_data["coordinates"]["z"] = '{:.2f}'.format(localz + uav_id)
        json_data["angles"]["x"] = '{:.2f}'.format(local_angle_x + uav_id)
        json_data["angles"]["y"] = '{:.2f}'.format(local_angle_y + uav_id)
        json_data["angles"]["z"] = '{:.2f}'.format(local_angle_z + uav_id)
        json_data["angles"]["w"] = '{:.2f}'.format(local_angle_w)
    print(json_data)
    channel.basic_publish(
        exchange='geoposition',
        routing_key="UAV_local",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def send_global(lattitude, longtitude, altitude):
    json_data = {}
    json_data["id"] = 1
    json_data["lattitude"] = lattitude
    json_data["longtitude"] = longtitude
    json_data["altitude"] = altitude
    print(json_data)
    channel.basic_publish(
        exchange='geoposition',
        routing_key="UAV_global",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))

def send_altitude(altitude):
    uav_id = random.randrange(1, 4)
    json_data = {}
    json_data["id"] = uav_id
    if uav_id == 1:
        json_data["altitude"] = altitude
    else:
        json_data["altitude"] = altitude - 10
    channel.basic_publish(
        exchange='altitude',
        routing_key="",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))

def send_voltage(voltage):
    uav_id = random.randrange(1, 4)
    json_data = {}
    json_data["id"] = uav_id
    json_data["battery"] = voltage
    channel.basic_publish(
        exchange='battery',
        routing_key="",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


while True:
    if new_localx < 1000 and new_localy < 1000:
        new_localx = new_localx +2
        new_localy = new_localy + 2

        new_lattitude = new_lattitude + 2
        new_longtitude = new_longtitude + 2

    else:
        new_localx = localx
        new_localy = localy
        new_lattitude = lattitude
        new_longtitude = longtitude
    send_locals(new_localx, new_localy ,localz, local_angle_x, local_angle_y,local_angle_z,local_angle_w)
    send_global(new_lattitude, new_longtitude, altitude)
    send_voltage(voltage)
    send_altitude(altitude)
    time.sleep(5)

