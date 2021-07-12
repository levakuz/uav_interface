#!/usr/bin/env python
import json

import pika
import uuid

class FibonacciRpcClient(object):

    def __init__(self):
        credentials = pika.PlainCredentials('admin', 'admin')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
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
            routing_key='add_mission_rpc',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return self.response


fibonacci_rpc = FibonacciRpcClient()
recived_message = {"directive_time_secs":20000,"time_out_of_launches":20,"simultaneous_launch_number":3,"start_point":{"latitude":60.1,"longitude":60.3},"reset_point":{"latitude":60.0,"longitude":60.0},"landing_point":{"latitude":60.1,"longitude":60.3},"uavs":[1], "payload":{"payload_type_name":"RG","weight":0.2,"damage_radius":50.0},"target_type": 1,"destination":{"dest_poligon":[{"latitude":59.8,"longitude":59.9},{"latitude":59.9,"longitude":59.9},{"latitude":59.9,"longitude":59.8},{"latitude":59.8,"longitude":59.8}],"targets_number":3,"targets_coords":[{"latitude":59.826,"longitude":59.834},{"latitude":59.827,"longitude":59.844}]},"time_intervals":{"start":0,"order":2400,"marching":2000,"attack":300,"reset":500,"returning":2000},"success_level":0.9988}

# recived_message = {'key': "id","id": 1}
# # recived_message = {'id': 2, "time_zero": 1, "uavs":[{"flight_number":"ss1","launch_time":1619113475,"course":273.3,"arrival":1619116375},
# {"flight_number":"ss2","launch_time":1619111975,"course":273.3,"arrival":1619116375}]}
# recived_message = {"id": 1}
# recived_message = {"tail_number": 111,  'fuel_resource': 1, 'time_for_prepare': 20, 'uav_role': 1}

# recived_message = {"key": "array", "id": [23,123,142,53,88,71,8,7], "TL":{"x": -15, "y": 30}, "BR": {"x": 5, "y": 5} }
# recived_message = {'name': 'Heh', 'vel': [0, 10], 'vertical_vel_up': [0, 10], 'vertical_vel_down': [0, 10], 'cargo_type': 1, 'cargo_quantity': 10, 'fuel_consume': 10, 'radius_of_turn': 3}
# recived_message = {"name": '1', 'range_horizontal': 20, 'range_vertical': 10, 'rapidity': 10}
# recived_message = {"name": '1', 'max_vel': 20, 'max_acc': 10, 'min_acc': 10, 'length': 5, 'height': 10, 'width': 20,'radius_of_turn': 50, 'weapon': 1}
# recived_message = {'co_type': 1}
# recived_message["tail_number"] = 123
# recived_message["uav_role"] = 1
# recived_message["fuel_resource"] = 123
# recived_message["time_for_prepare"] = 123
print(" [x] Requesting fib(30)")
response = fibonacci_rpc.call(json.dumps(recived_message))
print(" [.] Got %r" % json.loads(response))
