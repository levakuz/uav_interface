from flask import jsonify, make_response, render_template, Flask
import psycopg2
from waitress import serve
import json
connection_db = psycopg2.connect(user="postgres",
                              password="vfvfcdtnf",
                              host="127.0.0.1",
                              port="5432",
                              database="postgres_db")

app = Flask(__name__)


@app.route('/uav/<id>')
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


@app.route('/uav/<id>/greater_time/<time>')
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


@app.route('/uav/<id>/less_time/<time>')
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


@app.route('/uav/greater_time/<time>')
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


@app.route('/uav/less_time/<time>')
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


@app.route('/uav/greater_and_less_time/<time1>/<time2>')
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


serve(app, host='0.0.0.0', port=8001)