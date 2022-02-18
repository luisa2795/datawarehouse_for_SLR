import pandas as pd
import os
import re
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

def word_to_int(word):
    """Formats strings to integers if they only contain numbers or they have punctuation that is likely a thousands separator.
    
    Args:
        word (str): any word from a sentence.
    
    Returns:
        the corrensponding integer, if the word could be translated to one, otherwise no return-value.
    """
    if bool(re.fullmatch("[0-9]+([,.][0-9]{3})*?", word)):
        return(int(re.sub("[,.]", "", word)))
    else:
        pass

def word_to_float(word):
    """Formats strings to floats if possible.
    
    Args:
        word (str): any word from a sentence.
    
    Returns:
        the corrensponding float, if the word could be translated to one, otherwise no return-value.
    """
    try:
        return (float(word))
    except:
        pass