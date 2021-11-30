import pandas as pd
import os
from database import Database

class BaseTransformator:
    """Extracts a file, transforms it and loads it to a target database. 
    
    Note:
        This is a base class for more specified transformators.
    
    Args:
        sourcepath (str): path to the folder containing all relevant source csv-files.
        connection_params (dict): dictionary containing connection parameters of target database for the DWH (username, password, host, port, database)
    
    Attributes:
        sourcepath (str): path to the folder containing all relevant source csv-files.
        _targetdb (Database): initialized target database
     """

    def __init__(self, sourcepath, connection_params):
         self.sourcepath=sourcepath
         self._targetdb = Database(connection_params)

    def load_sourcefile (self, filename): 
        source_df=pd.read_csv(os.path.join(self.sourcepath, filename))
        return source_df

    def load_full_targettable (self, tablename):
        return (self._targetdb.load_full_table(tablename))

    def write_to_dwh (self, data, tablename):
        self._targetdb.insert_to_database(data, tablename)
