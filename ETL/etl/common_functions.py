import pandas as pd
import os
from variables import sourcepath


def load_sourcefile (filename): 
    source_df=pd.read_csv(os.path.join(sourcepath, filename))
    return source_df

