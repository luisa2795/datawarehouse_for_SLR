import pandas as pd
import os

class Base_Transformator:
    """Extracts a file, transforms it and loads it to a target database. 
    
    Note:
        This is a base class for more specified transformators.
    
    Args:
        #TODO: remove? sourcepath (str): path to the folder containing all relevant source csv-files.
    
    Attributes:
        TODO
    
     """

    def __init__(self, sourcepath):
         self.sourcepath=sourcepath

    def load_sourcefile (self, filename): 
        source_df=pd.read_csv(os.path.join(self.sourcepath, filename))
        return source_df