from psycopg2 import connect, sql, DatabaseError
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import url
import json
import logging
import datetime


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

class Dump:
    start_table:str
    id:int
    limit:int
    

    def __init__(self, connect, start_table=None, id=1, limit = 1):
        self.connect = connect
        self.start_table = start_table
        self.id = id
        self.limit = limit


    def escape_quotes(dic:dict):
        dd = json.dumps(dic)
        dd = dd.replace("'", "''")
        return json.loads(dd)
    

    def create_foreing_rows(self, tables, table_name, foreing_keys, values_parent, columns_parent, fm):
        for fk in foreing_keys:
            table_fk = fk['referred_table']
            fk_column = fk['referred_columns'][0]
            column = fk['constrained_columns'][0]
        
            create_row(table_fk, fm, tables, fk_column, values_parent[columns_parent.index(column)])
    

    def create_row(self, table, fm, tables, column=None, value=None):
        if column:
            placeholder_str = sql.SQL("SELECT * FROM {} WHERE {} = {}").format(
                sql.Identifier(table),
                sql.Identifier(column),
                sql.Placeholder()
            )

            self.connect.cursor.execute(placeholder_str, [value])
        else:
            placeholder_str = sql.SQL("SELECT * FROM {} limit 1").format(
                sql.Identifier(table)
            )

            self.connect.cursor.execute(placeholder_str)
        
        row = self.connect.cursor.fetchone()
    
        if row:
            columns_descr = self.connect.cursor.description
            column_names= []

            for c in columns_descr:
                if table == 'accounts_freetestcampaign' and c[0] == 'id':
                    continue

                column_names.append(c[0])
            
            foreing_keys = self.connect.inspector.get_foreign_keys(table)
            
            if len(foreing_keys):
                create_foreing_rows(tables, table, foreing_keys, row, column_names, fm)

            insert_prefix = 'INSERT INTO %s (%s) VALUES ' % (table, ', '.join(column_names))
            row_data = []
            for rd in row:
                if rd is None:
                    row_data.append('NULL')
                elif isinstance(rd, datetime.datetime):
                    row_data.append("'%s'" % (rd.strftime('%Y-%m-%d %H:%M:%S') ))
                elif isinstance(rd,dict):
                    row_data.append("'%s'" % json.dumps(escape_quotes(rd)))
                elif isinstance(rd,list):
                    row_data.append("'%s'" % str(rd).replace('[','{').replace(']','}'))
                elif isinstance(rd, memoryview):
                    #row_data.append("%s" % (bytes(rd)))
                    row_data.append("''")
                else:
                    row_data.append(repr(rd))


            fm.write('%s (%s);\n' % (insert_prefix, ', '.join(row_data)))
        
        if table in tables:
            tables.remove(table)


    def isInt(s):
        try: 
            return int(s)
        except ValueError:
            return None
    

    def run(self, tables:list, start_column_name = 'id', start_key_value=1):
        try:
            with open("dump_models.sql",'w+') as file:
                self.create_row(self.start_table,
                                file,
                                tables,
                                start_column_name,
                                start_key_value)
            
                while len(tables):
                    table = tables.pop()
                    if table != 'alembic_version': # remove alembic version if exists
                        create_row(table, file, tables)
        
        except DatabaseError as e:
            logging.error('Error {}'.format(e.pgerror))
            sys.exit(1)