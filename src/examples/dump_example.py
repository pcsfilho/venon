from psycopg2 import connect, sql, DatabaseError
import datetime
import json
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
url = 'postgresql://postgres:postgres@127.0.0.1:5432/postgres'

engine_cp = create_engine(url, echo=True)
Session = sessionmaker(bind=engine_cp)
session_cp = Session()
inspector = inspect(engine_cp)
conn = connect(database='postgres',
    user='postgres',
    password='postgres',
    port='5432',
    host='127.0.0.1'
)

cursor = conn.cursor()

def escape_quotes(dic:dict):
    dd = json.dumps(dic)
    dd = dd.replace("'", "''")
    return json.loads(dd)


def create_foreing_rows(tables, table_name, foreing_keys, values_parent, columns_parent, fm):
    for fk in foreing_keys:
        table_fk = fk['referred_table']
        fk_column = fk['referred_columns'][0]
        column = fk['constrained_columns'][0]
        
        create_row(table_fk, fm, tables, fk_column, values_parent[columns_parent.index(column)])


def create_row(table, fm, tables, column=None, value=None):
    if column:
        placeholder_str = sql.SQL("SELECT * FROM {} WHERE {} = {}").format(
            sql.Identifier(table),
            sql.Identifier(column),
            sql.Placeholder()
        )

        cursor.execute(placeholder_str, [value])
    else:
        placeholder_str = sql.SQL("SELECT * FROM {} limit 1").format(
            sql.Identifier(table)
        )

        cursor.execute(placeholder_str)
    
    row = cursor.fetchone()
    
    if row:
        columns_descr = cursor.description
        column_names= []

        for c in columns_descr:
            if table == 'accounts_freetestcampaign' and c[0] == 'id':
                continue

            column_names.append(c[0])
        
        foreing_keys = inspector.get_foreign_keys(table)
        
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

def main():
    try:
        start_table = input('Enter start table name: ')
        id = isInt(input('Enter profile id: '))
        only = input('Get only rows relationships from start table (y/n): ')

        if id:
            tables = inspector.get_table_names()
            with open("database/dump_models.sql",'w+') as f:
                create_row(start_table,f,tables, 'id', id)

                if only == 'n':
                    while len(tables):
                        table = tables.pop()
                        if table != 'alembic_version':
                            create_row(table, f, tables)
                    
                
    except DatabaseError as e:
        print('Error {}'.format(e.pgerror))
        sys.exit(1)

    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()