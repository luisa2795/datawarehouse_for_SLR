import pandas as pd
import os
import re
import roman
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

def split_into_lists_of_two_strings(names):
    """Takes list of names and splits it into sublists of length 2.
    If the length of the split and flattened list is uneven, the last string is dropped.
    
    Args:
        names(list): list of names of multiple authors.

    Returns:
        list of sublists with each sublist containing surname and firstname of one author.
    """
    #split strings upon ' '  and flatten the resulting list of sublists
    names2=[item for sublist in [s.split(' ') for s in names] for item in sublist]
    all=[]
    while len(names2) > 2:
        auth=names2[:2]
        all.append(auth)
        names2=names2[2:]
    return all

def volume_to_int(volume):
    """Helper function, transforming the volume of a journal to an integer as required in DB schema.
    
    Args:
        volume (object): value from the volume column of dtype object, containing the info in numeric format, as string or as Roman number. 
    
    Returns:
        Volume as integer, if the transformation was not successful the dummy value 0 is returned.
    """
    try:
        vol=int(volume)
    except:
        try:
            vol=roman.fromRoman(volume)
        except:
            vol=0
    if vol>10000:
        vol=0
    return vol

def issue_to_int(issue):
    """Helper function, transforming issue of journal to an integer.
    
    Args:
        issue (object): value from issue column of dtype object, containing the info either as numeric value, as list or as string.
        
    Returns:
        Issue as integer, if the transformation was not successful the dummy value 0 is returned.
    """
    try: 
        iss=int(issue)
    except:
        try:
            iss=int(issue[0])
        except:
            iss=0
    return iss

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