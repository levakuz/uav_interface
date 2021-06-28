
import psycopg2
from psycopg2 import Error


def create_table_co(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE co
                              (ID INT PRIMARY KEY     NOT NULL,
                              co_type           INT NOT NULL); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица co успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_co_dynamic_params(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE co_dynamic_params
                              (
                              "time" time without time zone NOT NULL,
                              id integer NOT NULL,
                              vel text,
                              coords text
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица co_dynamic_params успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_co_type(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE co_type
                              (
                              id integer NOT NULL,
                              name text NOT NULL,
                              max_vel real NOT NULL,
                              max_acc real NOT NULL,
                              min_acc real NOT NULL,
                              length integer NOT NULL,
                              width integer NOT NULL,
                              height integer NOT NULL,
                              radius_of_turn real NOT NULL,
                              weapon integer NOT NULL
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица co_type успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_co_weapon(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE co_weapon
                              (
                              id integer NOT NULL,
                              name text NOT NULL,
                              range_horizontal real NOT NULL,
                              range_vertical real NOT NULL,
                              rapidity integer NOT NULL
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица co_weapon успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_mission_input(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE mission_input
                              (
                               id integer NOT NULL,
                               directive_time_secs integer NOT NULL,
                               time_out_of_launches integer NOT NULL,
                               simultaneous_launch_number integer NOT NULL,
                               reset_point text NOT NULL,
                               landing_point text NOT NULL,
                               uavs text NOT NULL,
                               payload text,
                               target_type integer NOT NULL,
                               dest_poligon text NOT NULL,
                               targets_number integer NOT NULL,
                               targets_coords text NOT NULL,
                               time_intervals text NOT NULL
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица mission_input успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_mission_output(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE mission_output
                              (
                               id integer NOT NULL,
                               time_zero integer NOT NULL,
                               uavs text NOT NULL
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица mission_output успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_uav(connection):
    try:
        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE uav
                              (
                               id integer NOT NULL,
                               tail_number integer NOT NULL,
                               uav_type integer NOT NULL,
                               fuel_resource integer NOT NULL,
                               time_for_prepare integer NOT NULL,
                               uav_role integer
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица uav успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_uav_dynamic_params(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE uav_dynamic_params
                              (
                               "time" text NOT NULL,
                               id integer NOT NULL,
                               coords text,
                               global_coords text,
                               altitude real,
                               battery real
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица uav_dynamic_params успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_uav_role(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE uav_role
                              (
                                id integer NOT NULL,
                                name text NOT NULL,
                                uav_type integer NOT NULL
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица uav_role успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_uav_type(connection):
    try:

        # Создайте курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE uav_type
                              (
                                id integer NOT NULL,
                                name text NOT NULL,
                                min_vel real NOT NULL,
                                max_vel real NOT NULL,
                                min_vertical_vel_up real NOT NULL,
                                max_vertical_vel_up real NOT NULL,
                                min_vertical_vel_down real NOT NULL,
                                max_vertical_vel_down real NOT NULL,
                                cargo_type integer NOT NULL,
                                cargo_quantity integer NOT NULL,
                                fuel_consume real NOT NULL,
                                radius_of_turn real NOT NULL
                              ); '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица uav_type успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == "__main__":
    connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="password",
                                      host="192.168.1.65",
                                      port="5432",
                                      database="postgres")
    create_table_co(connection)
    create_table_co_dynamic_params(connection)
    create_table_co_type(connection)
    create_table_co_weapon(connection)
    create_table_mission_input(connection)
    create_table_mission_output(connection)
    create_table_uav(connection)
    create_table_uav_dynamic_params(connection)
    create_table_uav_role(connection)
    create_table_uav_type(connection)
    connection.close()
