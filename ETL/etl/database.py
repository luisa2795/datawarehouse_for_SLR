from sqlalchemy import create_engine, exc
import pandas as pd
import sqlalchemy
from sqlalchemy.sql.schema import MetaData
import psycopg2


def initialize_engine(connection_params):
    #initialize SQLAlchemy engine with given connection parameters, enable logging the SQL output and use the future version  (2.0)
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
        connection_params['username'], connection_params['password'], connection_params['host'], connection_params['port'], connection_params['database']), 
        future=True)#echo=True, 
    #self._metadata=MetaData()
    psycop2connect=psycopg2.connect(dbname=connection_params['database'], user=connection_params['username'], password=connection_params['password'], host=connection_params['host'], port=connection_params['port'])
    return engine, psycop2connect

def load_full_table(engine, table):
    """Loads full table that is existing in the specified database table and returns it as dataframe.
    
    Args: 
        table (str): The name of the DB table to load.
        
    Returns: 
        A pandas dataframe of the entire table.
    
    Raises:
        ValueError: If the table does not exist in the DB.
        """
    #with self._engine.connect() as conn:
        #return (conn.execute(text("SELECT * from {}".format(table))))
    return (pd.read_sql_table(table, engine.connect()))
    

def insert_to_database(engine, data, table):
    """This function inserts data into a table of the database.

    Args: 
        data (Dataframe): The dataframe to be inserted into the database; it must follow the same schema as the database table
        table (str): The name of the table the data should be inserted into
    """
    try:
        data.to_sql(table, engine, if_exists='append', index=False)
    except exc.IntegrityError as error:
        print(error)

def update_row(engine, table, conditions, values):
    #TODO docstrings, call this in SCD2 and SCD1 method in authortransformator
    with engine.connect() as con:
        statement=table.update().values(values).where(conditions)
        con.execute(statement)

#TODO: maybe remove, currently unused
def execute_query(psycop2connect, sql_query):
    #TODO docstings to change single attributes of entries in the database
    #statement=sqlalchemy.text(sql_query)
    cur=psycop2connect.cursor()
    cur.execute(sql_query)

        

