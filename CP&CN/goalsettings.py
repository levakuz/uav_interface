import subprocess
import pika
import json
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                               5672,
                                                               '/',
                                                               credentials, blocked_connection_timeout=0, heartbeat=0))
channel = connection.channel()


def get_goal_settings_input(ch, method, properties, body):
    print("here")
    cmd = ['./goalsettings']
    output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
    print(str(output))
    if "GoalSettingsStatus:finished" in str(output):
        with open('gsoutput.json', 'r', encoding='utf-8') as g:
            output_goals = g.read()

        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             properties.correlation_id),
                         body=json.dumps(output_goals))


channel.queue_declare(queue='get_goal_settings_input', durable=False)
channel.basic_consume(queue='get_goal_settings_input', on_message_callback=get_goal_settings_input, auto_ack=True)

channel.start_consuming()