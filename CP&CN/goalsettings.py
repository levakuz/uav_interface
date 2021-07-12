import subprocess
import pika
import json
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
                                                               5672,
                                                               '/',
                                                               credentials, blocked_connection_timeout=0, heartbeat=0))
channel = connection.channel()


def get_goal_settings_input(ch, method, properties, body):
    print("here")
    new_msg = json.loads(body)
    # new_msg = {'directive_time_secs': 7000, 'time_out_of_launches': 20, 'simultaneous_launch_number': 3, 'start_point': {'latitude': 60.1, 'longitude': 60.3}, 'reset_point': {'latitude': 60.0, 'longitude': 60.0}, 'landing_point': {'latitude': 60.1, 'longitude': 60.3}, 'uavs': [{'flight_number': 'ss1', 'uav_type_name': 'orlan', 'flight_hours_resource': 10.0, 'time_to_start_secs': 0.0}, {'flight_number': 'ss2', 'uav_type_name': 'quadro', 'flight_hours_resource': 8.0, 'time_to_start_secs': 5.0}], 'uav_types': [{'uav_type_name': 'orlan', 'min_h_speed': 20.0, 'max_h_speed': 40.0, 'min_v_speed_up': 1.0, 'max_v_speed_up': 2.0, 'min_v_speed_down': 1.0, 'max_v_speed_down': 10.0, 'payload_type_name': 'RG', 'payload_number': 4, 'resource_consumption_per_hour': 1.0, 'uturn_radius': 100.0}, {'uav_type_name': 'quadro', 'min_h_speed': 0.0, 'max_h_speed': 10.0, 'min_v_speed_up': 1.0, 'max_v_speed_up': 1.0, 'min_v_speed_down': 1.0, 'max_v_speed_down': 1.0, 'payload_type_name': 'RG', 'payload_number': 2, 'resource_consumption_per_hour': 1.0, 'uturn_radius': 0.0}], 'payload': {'payload_type_name': 'RG', 'weight': 0.2, 'damage_radius': 50.0}, 'target_type': {'target_type_name': '1', 'max_speed': 20.0, 'min_acceleration': 10.0, 'max_acceleration': 10.0, 'length': 5, 'width': 20, 'height': 10, 'uturn_radius': 50.0, 'weapon': {'h_distance': 20.0, 'v_distance': 10.0, 'rate_of_fire': 10}}, 'targets_number': 3, 'targets_coords': [{'latitude': 59.826, 'longitude': 59.834}, {'latitude': 59.827, 'longitude': 59.844}], 'time_intervals': {'start': 0, 'order': 2400, 'marching': 2000, 'attack': 300, 'reset': 500, 'returning': 2000}, 'success_level': 0.9988, 'destination': {'dest_poligon': [{'latitude': 59.8, 'longitude': 59.9}, {'latitude': 59.9, 'longitude': 59.9}, {'latitude': 59.9, 'longitude': 59.8}, {'latitude': 59.8, 'longitude': 59.8}]}}
    # new_msg = str(new_msg)
    # new_msg.replace("\'", "\"")
    print(new_msg)
    with open('gsinput.json', 'w', encoding='utf-8') as f:
        json.dump(new_msg, f)
    cmd = ['./goalsettings']
    output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
    print(str(output))
    if "GoalSettingsStatus:finished" in str(output):
        with open('gsoutput.json') as f_in:
            output_goals = json.load(f_in)
        print(output_goals["uavs"])


        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(output_goals))
    else:
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps({"status": "failed"}))


channel.queue_declare(queue='get_goal_settings_input', durable=False)
channel.basic_consume(queue='get_goal_settings_input', on_message_callback=get_goal_settings_input, auto_ack=True)
channel.start_consuming()