
import psycopg2
from psycopg2 import Error

try:
    # Подключиться к существующей базе данных
    connection = psycopg2.connect(user="postgres",
                                  # пароль, который указали при установке PostgreSQL
                                  password="vfvfcdtnf",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres_db")

    # Создайте курсор для выполнения операций с базой данных
    cursor = connection.cursor()
    # SQL-запрос для создания новой таблицы
    create_table_query = '''CREATE TABLE MISSION
                          (ID TEXT PRIMARY KEY     NOT NULL,
                          MISSION_ACHIEVMENT           BOOL    NOT NULL,
                          TIME_START  TEXT    NOT NULL, 
                          ARRAY_OF_UAVS  INT    NOT NULL,
                          GR_POLYGON_COORDS TEXT NOT NULL,
                          GOALS_CUANTS INT NOT NULL,
                          GOALS_COORDS TEXT,
                          TIME_FROM_GET_GOAL_TO_START REAL NOT NULL,
                          TIME_FROM_START_TO_FORMATION REAL NOT NULL,
                          TIME_FROM_FORMATION_TO_ARRIVAL REAL NOT NULL,
                          TIME_OF_FLIGHT_AROUND_GOAL REAL NOT NULL,
                          TIME_FROM_GOAL_LEFT_TO_DROPZONE REAL NOT NULL,
                          TIME_FLIGHT_DROPZONE_TO_LANDINGZONE REAL NOT NULL,
                          MAX_TIME_FOR_MISSION REAL NOT NULL,
                          TIMEINTERVAL_OF_CATAPULTE REAL NOT NULL,
                          QUANTITY_OF_CATAPULTES INT NOT NULL,
                          COORDS_OF_DROPZONE TEXT NOT NULL,
                          COORDS_OF_LANDINGZONE TEXT NOT NULL,
                          AVAILABLE_UAVS TEXT NOT NULL,
                          TYPES_OF_AVAILABLE_UAVS TEXT NOT NULL,
                          CARGO_INFO TEXT NOT NULL,
                          CO_TYPE INT NOT NULL,
                          GOALZONE_INFO TEXT NOT NULL,
                          TIME_INTERVALS_GAZEBO TEXT); '''
    # Выполнение команды: это создает новую таблицу
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица успешно создана в PostgreSQL")

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")