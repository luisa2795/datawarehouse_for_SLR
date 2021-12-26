import pandas as pd
import os
from variables import sourcepath


def load_sourcefile (filename): 
    """Loads a .csv-sourcefile from the folder specified in the global variable sourcepath. 
    
    Args:
        filename(str): the name of the file to load, must be a .csv-file.
        
    Returns:
        The data of the specified file as a pandas Dataframe.
    """
    source_df=pd.read_csv(os.path.join(sourcepath, filename))
    return source_df

