#!/usr/bin/env python
import json

import pika
import uuid


class FibonacciRpcClient(object):

    def __init__(self):
        credentials = pika.PlainCredentials('admin', 'admin')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
                                                                       5672,
                                                                       '/',
                                                                       credentials, blocked_connection_timeout=0,
                                                                       heartbeat=0))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='spawn_co_rpc',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return self.response


fibonacci_rpc = FibonacciRpcClient()
# recived_message = {"directive_time_secs": 11, "time_out_of_launches": 123, "simultaneous_launch_number": 123,
#                    "reset_point": 123, "landing_point": 123, "uavs": 123, "payload": 123, "target_type": 123,
#                    "dest_poligon": 123, "targets_number": 123, "targets_coords": 123, "time_intervals": 123}
# recived_message = {'key': "id","id": 1}
# # recived_message = {'id': 2, "time_zero": 1, "uavs":[{"flight_number":"ss1","launch_time":1619113475,"course":273.3,"arrival":1619116375},
# {"flight_number":"ss2","launch_time":1619111975,"course":273.3,"arrival":1619116375}]}
# recived_message = {"id": 1}
recived_message = {"key": "array", "id": [111,112,113,114,115,116,117,118,119]}
print(" [x] Requesting fib(30)")
response = fibonacci_rpc.call(json.dumps(recived_message))
print(" [.] Got %r" % json.loads(response))
