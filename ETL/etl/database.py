from sqlalchemy import create_engine, exc, text
import pandas as pd
import sqlalchemy


def initialize_engine(connection_params):
    """Initializes SQLAlchemy engine with given connection parameters, 
    enable logging the SQL output and use the future version  (2.0)
    
    Args:
        connection_params (dict): The connection parameters for the database. 
            Must contain username, password, host, port and database values.
            
    Returns:
        An SQL Alchemy engine object.
        A Psycop2 connect object.
    """
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
        connection_params['username'], connection_params['password'], connection_params['host'], connection_params['port'], connection_params['database']), 
        future=True)#echo=True, 
    return engine

def load_full_table(engine, table):
    """Loads full table that is existing in the specified database table and returns it as dataframe.
    
    Args: 
        engine (SQL Alchemy engine object): The engine for the target database.
        table (str): The name of the DB table to load.
        
    Returns: 
        A pandas dataframe of the entire table.
    
    Raises:
        ValueError: If the table does not exist in the DB.
        """
    return (pd.read_sql_table(table, engine.connect()))

def load_df_from_query(engine, querystring):
    """Loads full table that is existing in the specified database table and returns it as dataframe.
    
    Args: 
        engine (SQL Alchemy engine object): The engine for the target database.
        querystring (str): The SQL SELECT statement to load the data.
        
    Returns: 
        A pandas dataframe of the selected data.
    """
    return (pd.read_sql_query(text(querystring), engine.connect()))
    

def insert_to_database(engine, data, table, if_exists='append'):
    """This function inserts data into a table of the database.

        Args: 
            engine (SQL Alchemy engine object): The engine for the target database.
            data (Dataframe): The dataframe to be inserted into the database; it must follow the same schema as the database table.
            table (str): The name of the table the data should be inserted into.

        Raises:
            Integrity error when the schemas do not match or table constraints are violated.
        """
    try:
        data.to_sql(table, engine, if_exists=if_exists, index=False)
    except exc.IntegrityError as error:
        print(error)



        

