import pyodbc


class MSSQLReader:
    def __init__(self, db_config):
        self.connection = None
        self.server = db_config

    def connect(self, server):
        connection_string = 'Driver={};Server={};Database={};Uid={};Pwd={};Port={};'.format(
            self.server["driver"], self.server["host"], self.server["database"], server["user"], server["password"],
            server["port"])
        self.connection = pyodbc.connect(connection_string)
        return self.connection

    def query(self, query):
        cursor = self.connection.cursor()
        print(query)
        cursor.execute(query)
        return cursor
