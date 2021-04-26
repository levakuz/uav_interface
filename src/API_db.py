from flask import jsonify, make_response, render_template, Flask
import psycopg2
from waitress import serve
import json
connection_db = psycopg2.connect(user="postgres",
                              password="password",
                              host="192.168.0.17",
                              port="5432",
                              database="postgres")

app = Flask(__name__)


@app.route('/uav_dynamic/<id>')
def uav_id(id):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE id = '{}' ORDER BY time DESC;
                        """.format(id)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = {'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    print(json.dumps(final_json))
    return jsonify(final_json)


@app.route('/uav_dynamic/<id>/greater_time/<time>')
def uav_id_greater_time(id, time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE id = '{}' AND time >= '{}'::time ORDER BY time DESC;
                        """.format(id,time)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = { 'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    print(json.dumps(final_json))
    return jsonify(final_json)


@app.route('/uav_dynamic/<id>/less_time/<time>')
def uav_id_less_time(id, time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE id = '{}' AND time <= '{}'::time ORDER BY time DESC;
                        """.format(id,time)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = { 'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    print(json.dumps(final_json))
    return jsonify(final_json)


@app.route('/uav_dynamic/greater_time/<time>')
def uav_greater_time(time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE time >= '{}'::time ORDER BY time DESC;
                        """.format(time)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = {'id': record[1], 'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    print(json.dumps(final_json))
    return jsonify(final_json)


@app.route('/uav_dynamic/less_time/<time>')
def uav_less_time(time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE time <= '{}'::time ORDER BY time DESC;
                        """.format(time)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = {'id': record[1], 'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    print(json.dumps(final_json))
    return jsonify(final_json)


@app.route('/uav_dynamic/greater_and_less_time/<time1>/<time2>')
def uav_greater_and_less_time(time1, time2):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM uav_dynamic_params WHERE time >= '{}'::time AND time <= '{}'::time ORDER BY time DESC;
                        """.format(time1, time2)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = {'id': record[1], 'coords': record[2], 'altitude': record[3], 'battery': record[4], 'global_coords': record[5]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    print(json.dumps(final_json))
    return jsonify(final_json)


@app.route('/co/<id>')
def co_id(id):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM co WHERE id = '{}';
                        """.format(id)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'id': record[0], 'co_type': record[1]}
    print(json_list)
    return jsonify(json_list)


@app.route('/co/type/<type>')
def cotype(type):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM co WHERE co_type = '{}';
                        """.format(type)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'id': record[0], 'co_type': record[1]}
    print(json_list)
    return jsonify(json_list)


@app.route('/co_type/<name>')
def co_type(name):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM co_type WHERE name = '{}';
                        """.format(name)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'id': record[0], 'name': record[1], 'max_vel': record[2], 'min_vel': record[3],
                     'max_acc': record[4], 'min_acc': record[5], 'length': record[6], 'width': record[7],
                     'height': record[8], 'radius_of_turn': record[9], 'weapon': record[10]}
    print(json_list)
    return jsonify(json_list)


@app.route('/co_weapon/<name>')
def co_weapon(name):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM co_weapon WHERE name = '{}';
                        """.format(name)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'id': record[0], 'rage_horizontal': record[1], 'range_vertical': record[2], 'rapidity': record[3]}
    print(json_list)
    return jsonify(json_list)


@app.route('/environment/<time>')
def environment(time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM environment WHERE time = '{}'::time;
                        """.format(time)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'wind_vel': record[1], 'wind_coords': record[2], 'temp': record[3]}
    print(json_list)
    return jsonify(json_list)


@app.route('/environment/greater_time/<time>')
def environment_greater_time(time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM environment WHERE time >= '{}'::time;
                        """.format(time)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = {'wind_vel': record[1], 'wind_coords': record[2], 'temp': record[3]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    return jsonify(final_json)


@app.route('/environment/less_time/<time>')
def environment_less_time(time):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM environment WHERE time <= '{}'::time;
                        """.format(time)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = {'wind_vel': record[1], 'wind_coords': record[2], 'temp': record[3]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    return jsonify(final_json)


@app.route('/environment/greater_and_less_time/<time1>/<time2>')
def environment_greater_and_less_time(time1,time2):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM environment WHERE time <= '{}'::time AND ime >= '{}'::time;
                        """.format(time1,time2)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    final_json = {}
    for record in records:
        print(record)
        json_list = {'wind_vel': record[1], 'wind_coords': record[2], 'temp': record[3]}
        final_json[record[0].strftime("%H:%M:%S")] = json_list
    print(final_json)
    return jsonify(final_json)


@app.route('/mission/<id>')
def mission(id):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM mission WHERE id = '{}';
                        """.format(id)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'id': record[0], 'mission_achievment': record[1], 'time_start': record[2],
                     'array_of_uavs': record[3], 'gr_polygon_coords': record[4], 'goals_cuants': record[5],
                     'goal_coords': record[6], 'time_from_get_goal_to_start': record[7],
                     'time_from_start_to_formation': record[8], 'time_from_formation_to_arrival': record[9],
                     'time_of_flight_around_goal': record[10], 'time_from_goal_left_to_dropzone': record[11],
                     'time_flight_dropzone_to_landingzone': record[12], 'max_time_for_mission': record[13],
                     'timeinterval_of_catapulte': record[14], 'quantity_of_catapultes': record[15],
                     'coords_of_dropzone': record[16], 'coords_of_landingzone': record[17], 'available_uavs': record[18],
                     'types_of_available_uavs': record[19], 'cargo_info': record[20], 'co_type': record[21],
                     'goalzone_info': record[22], 'time_intervals_gazebo': record[23]}
    print(json_list)
    return jsonify(json_list)


@app.route('/uav/<tail_number>/')
def uavs(id):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM co WHERE uav = '{}';
                        """.format(id)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'uav_type': record[2], 'fuel_resource': record[3], 'time_for_prepare': record[4]}
    print(json_list)
    return jsonify(json_list)


@app.route('/uav_type/<id>/')
def uavs_type(id):
    cursor = connection_db.cursor()
    insert_query = """ SELECT * FROM co WHERE uav_type = '{}';
                        """.format(id)
    cursor.execute(insert_query)
    records = cursor.fetchall()
    print(records)
    json_list = {}
    for record in records:
        print(record)
        json_list = {'min_vel': record[1], 'max_vel': record[2], 'max_vertical_vel_up': record[3],
                     'min_vertical_vel_up': record[4], 'max_vertical_vel_down': record[5],
                     'min_vertical_vel_down': record[6], 'cargo_type': record[7], 'cargo_quantity': record[8],
                     'fuel_consume': record[9], 'radius_of_turn': record[10]}
    print(json_list)
    return jsonify(json_list)

serve(app, host='0.0.0.0', port=8001)