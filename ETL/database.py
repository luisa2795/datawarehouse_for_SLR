from sqlalchemy import create_engine, exc
import pandas as pd
import sqlalchemy
from sqlalchemy.sql.schema import MetaData


class Database:
    """
    This class establishes a connection to a postgresql database, loads tables from it and inserts transformed data into its tables.
    
    :param connection_params: The dictionary of connection parameters for the database (username, password, host, port, database).
     """

    def __init__(self, connection_params):
        #initialize SQLAlchemy engine with given connection parameters, enable logging the SQL output and use the future version  (2.0)
        self._engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
            connection_params['username'], connection_params['password'], connection_params['host'], connection_params['port'], connection_params['database']), 
            echo=True, future=True)
        #self._metadata=MetaData()


    def load_full_table(self, table):
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
        return (pd.read_sql_table(table, self._engine.connect()))
        

    def insert_to_database(self, data, table):
        """This function inserts data into a table of the database.

        Args: 
            data (Dataframe): The dataframe to be inserted into the database; it must follow the same schema as the database table
            table (str): The name of the table the data should be inserted into
        """
        try:
            data.to_sql(table, self._engine, if_exists='append', index=False)
        except exc.IntegrityError as error:
            print(error)

    def execute_update_query(self, sql_query):
        #TODO docstings to change single attributes of entries in the database
        statement=sqlalchemy.text(sql_query)
        with self._engine.connect() as con:
            result=con.execute(statement)
            print(result.last_updated_params())

        

