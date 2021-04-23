import MySQLdb
import pandas as pd

DB = ""
HOST = "localhost"
USER = "root"
PASSWORD = ""
PORT = 33069


class DataBase:

    def create_connection_and_cursor(self, db_name: str = "") -> None:
        self.conn = MySQLdb.connect(host=HOST, user=USER, password=PASSWORD, port=PORT, db=db_name)
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    def conn_and_cursor_exist(self) -> bool:
        try:
            self.conn
            self.cursor
            return True
        except AttributeError:
            return False

    def is_database_selected(self) -> bool:
        try:
            self.cursor.execute("CREATE TABLE temp_table (teste varchar(1))")
            self.cursor.execute("DROP TABLE temp_table")
            return True
        except Exception:
            return False

    def change_current_database(self, new_database_name: str) -> None:
        self.conn.select_db(new_database_name)

    def convert_list_to_sql_string(self, data: list) -> str:
        converted_to_sql_data = [f"'{value}'"
                                 if isinstance(value, str) and value.upper() != "DEFAULT" and value.upper() != "NULL"
                                 else str(value)
                                 for value in data]
        string_values = ",".join(converted_to_sql_data)
        return string_values

    def insert_data(self, table_to_insert: str, data: list) -> bool:
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if not isinstance(data, list):
            raise TypeError("Data is not a list!")

        string_values = self.convert_list_to_sql_string(data)
        sql = f"""INSERT INTO {table_to_insert} VALUES ({string_values})"""

        try:
            affected_rows = self.cursor.execute(sql)
            if affected_rows > 0:
                return True
        except:
            return False

        return False

    def check_operation_data_type(self, where_value: str=None, operation_value=None) -> str:
        if where_value==None:
            where_type = ""
        elif isinstance(where_value, str) and operation_value !=None:
            where_type = f""" WHERE {where_value} = {f"'{operation_value}'" if isinstance(operation_value, str) else operation_value}"""
        else:
            raise Exception("?!")

        return where_type

    def delete_data(self, table_to_delete: str, where_delete: str=None, delete_values=None) -> bool:
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")

        values_to_delete = self.check_operation_data_type(where_delete, delete_values)

        sql=f"""DELETE FROM {table_to_delete}{values_to_delete};"""
        try:
            affected_rows = self.cursor.execute(sql)
            if affected_rows > 0:
                return True
        except:
            return False

        return False

    def list_data(self, table_to_list: str, where_list: str=None, list_values=None) -> None:
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")

        values_to_list = self.check_operation_data_type(where_list, list_values)

        sql = f"""SELECT * FROM {table_to_list}{values_to_list};"""
        self.cursor.execute(sql)
        df = self.cursor.fetchall()

        for line in df:
            print(line)

    # def convert_dict_to_sql_string(self, data: dict, separator=",") -> str:
    #     converted_to_sql_data = []
    #
    #     for key, value in data.items():
    #         if isinstance(value, str) and value.upper() != "DEFAULT" and value.upper() != "NULL":
    #             converted_to_sql_data.append(f"{key} = '{value}'")
    #         else:
    #             converted_to_sql_data.append(f"{key} = {value}")
    #
    #     string_values = f"{separator}".join(converted_to_sql_data)
    #     return string_values




# tt = DataBase()
# tt.create_connection_and_cursor('Banco')
# tt.delete_data('pessoa', 'idade', 11)
# tt.list_data('pessoa')