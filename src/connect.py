from psycopg2 import connect, sql, DatabaseError
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.engine import url
from sqlalchemy.orm import sessionmaker, scoped_session

class Connect:
    inspector = None
    cursor = None
    connection = None

    def __init__(self, database, user, password, port, host):
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.host = host
    
    def connect(self):
        engine = create_engine(url.URL(
            drivername = 'postgres',
            host = self.host,
            port = self.port,
            username = self.user,
            password = self.password,
            database = self.database
        ), echo=True)
        
        self.inspector = inspect(engine)

        self.connection = connect(database='postgres',
            user='postgres',
            password='postgres',
            port='5432',
            host='127.0.0.1'
        )

        self.cursor = self.connection.cursor()
    
    def close(self):
        self.connection.close()
