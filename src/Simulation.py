import datetime
import time
import pika
import psycopg2
import json
import random
from psycopg2 import Error
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                               5672,
                                                               '/',
                                                               credentials,blocked_connection_timeout=0,heartbeat=0))

channel = connection.channel()

channel.exchange_declare("UAV", exchange_type='topic', passive=False,
                         durable=False, auto_delete=False, arguments=None)
connection_db = psycopg2.connect(user="postgres",
                                         password="password",
                                         host="192.168.0.17",
                                         port="5432",
                                         database="postgres")


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
    time = datetime.datetime.now()
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
        exchange='UAV',
        routing_key="geoposition_local",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
    try:
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET coords = '{}' WHERE time = '{}' AND id = {};
        """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), uav_id)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
    except Error as e:
        print("error", e)
        count = 0
        connection_db.rollback()
    # print(count, "Succesfull update")
    if count == 0:
        try:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, coords)
                                                       VALUES (%s, %s, %s)"""
            item_tuple = (time.time().strftime("%H:%M:%S"), uav_id, json.dumps(json_data))
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
        except Error as e:
            print("error", e)
            connection_db.rollback()


def send_global(lattitude, longtitude, altitude):
    time = datetime.datetime.now()
    json_data = {}
    json_data["id"] = 1
    json_data["lattitude"] = lattitude
    json_data["longtitude"] = longtitude
    json_data["altitude"] = altitude
    print(json_data)
    channel.basic_publish(
        exchange='UAV',
        routing_key="geoposition_global",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
    try:
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET global_coords = '{}' WHERE time = '{}' AND id = {};
                """.format(json.dumps(json_data), time.time().strftime("%H:%M:%S"), 1)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        # print(count, "Succesfull update")
    except Error as e:
        print("error", e)
        count = 0
        connection_db.rollback()
    if count == 0:
        try:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, global_coords)
                                                       VALUES (%s, %s, %s)"""
            item_tuple = (time.time().strftime("%H:%M:%S"), 1, json.dumps(json_data))
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
        except Error as e:
            print("error", e)
            connection_db.rollback()


def send_altitude(altitude):
    time = datetime.datetime.now()
    uav_id = random.randrange(1, 4)
    json_data = {}
    json_data["id"] = uav_id
    if uav_id == 1:
        json_data["altitude"] = altitude
    else:
        json_data["altitude"] = altitude - 10
    channel.basic_publish(
        exchange='UAV',
        routing_key="altitude",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
    try:
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET altitude = '{}' WHERE time = '{}' AND id = {};
                    """.format(altitude, time.time().strftime("%H:%M:%S"), uav_id)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        # print(count, "Succesfull update altitude")
    except Error as e:
        print("error", e)
        count = 0
        connection_db.rollback()

    if count == 0:
        try:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, altitude)
                                                           VALUES (%s, %s, %s)"""
            item_tuple = (time.time().strftime("%H:%M:%S"), uav_id, altitude)
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
        except Error as e:
            print("error", e)
            connection_db.rollback()


def send_voltage(voltage):
    time = datetime.datetime.now()
    uav_id = random.randrange(1, 4)
    json_data = {}
    json_data["id"] = uav_id
    json_data["battery"] = voltage
    channel.basic_publish(
        exchange='UAV',
        routing_key="battery",
        body=json.dumps(json_data),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
    try:
        cursor = connection_db.cursor()
        insert_query = """ UPDATE uav_dynamic_params SET battery = '{}' WHERE time = '{}' AND id = {};
                """.format(voltage, time.time().strftime("%H:%M:%S"), uav_id)
        cursor.execute(insert_query)
        connection_db.commit()
        count = cursor.rowcount
        # print(count, "Succesfull update")
    except Error as e:
        print("error", e)
        count = 0
        connection_db.rollback()
    if count == 0:
        try:
            cursor = connection_db.cursor()
            insert_query = """ INSERT INTO uav_dynamic_params (time, id, battery)
                                                       VALUES (%s, %s, %s)"""
            item_tuple = (time.time().strftime("%H:%M:%S"), uav_id, voltage)
            cursor.execute(insert_query, item_tuple)
            connection_db.commit()
        except Error as e:
            print("error", e)
            connection_db.rollback()


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

