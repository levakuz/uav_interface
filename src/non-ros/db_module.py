import json
import psycopg2
import pika

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.65',
                                                               5672,
                                                               '/',
                                                               credentials))
connection_db = psycopg2.connect(user="postgres",
                              # пароль, который указали при установке PostgreSQL
                              password="password",
                              host="192.168.1.65",
                              port="5432",
                              database="postgres_db")

channel = connection.channel()
channel.queue_declare(queue='db_response', durable=False)


def db_response(ch, method, properties, body):
    cursor = connection_db.cursor()
    body = json.loads(body)
    if body["sql"] == "false":
        #Таблица окр. среды
        if body["table"] == "environment":
            if body["time"] == "none":
                if body["data"] == "wind_vel":
                    cursor.execute("SELECT wind_vel FROM environment;")
                    print("Результат", cursor.fetchall())
                    data = cursor.fetchall()
                elif body["data"] == "wind_coords":
                    cursor.execute("SELECT wind_coords FROM environment;")
                    print("Результат", cursor.fetchall())
                    data = cursor.fetchall()
                elif body["data"] == "temp":
                    cursor.execute("SELECT temp FROM environment;")
                    print("Результат", cursor.fetchall())
                    data = cursor.fetchall()
                elif body["data"] == "all":
                    cursor.execute("SELECT * FROM environment;")
                    data = cursor.fetchall()
            else:
                if body["data"] == "wind_vel":
                    cursor.execute("SELECT wind_vel FROM environment WHERE  time = '{}';".format(body["time"]))
                    print("Результат", cursor.fetchall())
                    data = cursor.fetchone()
                elif body["data"] == "wind_coords":
                    cursor.execute("SELECT wind_coords FROM environment WHERE  time = '{}';".format(body["time"]))
                    print("Результат", cursor.fetchall())
                    data = cursor.fetchone()
                elif body["data"] == "temp":
                    cursor.execute("SELECT temp FROM environment WHERE  time = '{}';".format(body["time"]))
                    print("Результат", cursor.fetchall())
                    data = cursor.fetchone()
                elif body["data"] == "all":
                    cursor.execute("SELECT * FROM environment WHERE  time = '{}';".format(body["time"]))
                    data = cursor.fetchone()
        #Таблица целевых объектов
        elif body["table"] == "co":
            if body["id"] == "all":
                cursor.execute("SELECT * FROM co;")
                print("Результат", cursor.fetchall())
                data = cursor.fetchall()
            else:
                cursor.execute("SELECT id, co_type FROM co WHERE  id = {};".format(body["id"]))
                print("Результат", cursor.fetchall())
                data = cursor.fetchone()
        #Таблица типов целевых объектов
        elif body["table"] == "co_type":
            if body["id"] == "all":
                cursor.execute("SELECT * FROM co_type;")
                print("Результат", cursor.fetchall())
                data = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM co WHERE  id = {};".format(body["id"]))
                print("Результат", cursor.fetchall())
                data = cursor.fetchone()
        #Таблица оружия целевых объектов
        elif body["table"] == "co_weapon":
            if body["id"] == "all":
                cursor.execute("SELECT * FROM co_weapon;")
                print("Результат", cursor.fetchall())
                data = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM co_weapon WHERE  id = {};".format(body["id"]))
                print("Результат", cursor.fetchall())
                data = cursor.fetchone()
        #Таблица динамических параметров
        elif body["table"] == "dynamic_params":
            if body["time"] == "all":
                cursor.execute("SELECT * FROM dynamic_params;")
                print("Результат", cursor.fetchall())
                data = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM co_weapon WHERE  time = {};".format(body["time"]))
                print("Результат", cursor.fetchall())
                data = cursor.fetchone()
        # Таблица миссиий
        elif body["table"] == "mission":
            if body["id"] == "all":
                cursor.execute("SELECT * FROM mission;")
                print("Результат", cursor.fetchall())
                data = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM mission WHERE  id = {};".format(body["id"]))
                print("Результат", cursor.fetchall())
                data = cursor.fetchone()
        # Таблица UAV
        elif body["table"] == "uav":
            if body["id"] == "all":
                cursor.execute("SELECT * FROM uav;")
                print("Результат", cursor.fetchall())
                data = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM uav WHERE  id = {};".format(body["id"]))
                print("Результат", cursor.fetchall())
                data = cursor.fetchone()
        # Таблица UAV_type
        elif body["table"] == "uav_type":
            if body["id"] == "all":
                cursor.execute("SELECT * FROM uav_type;")
                print("Результат", cursor.fetchall())
                data = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM uav_type WHERE  id = {};".format(body["id"]))
                print("Результат", cursor.fetchall())
                data = cursor.fetchone()
        else:
            print("Не указана таблица")
    else:
        cursor.execute(body["sql"])
        data = cursor.fetchall()
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                     properties.correlation_id),
                     body=json.dumps(data))


channel.basic_consume(queue='db_response', on_message_callback=db_response, auto_ack=True)


if __name__ == '__main__':
    channel.start_consuming()