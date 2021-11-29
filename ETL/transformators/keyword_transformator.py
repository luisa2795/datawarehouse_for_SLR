import pandas as pd
from ETL.transformators.base_transfomator import Base_Transformator 
from database import Database as db

class Keyword_Transformator (Base_Transformator):
    """Extracts, Transforms and Loads the keywords dimension.
    
    Args:
        xyz (str):XYZ String for the instantiation.
    
    Attributes:

    
     """

    def __init__(self, sourcepath):
        #init is inherited from base class
        super().__init__(sourcepath)
        self._unique_keywords=None


    def load_unique_keywords (self, keywords_filename): 
        source_keyw=super().load_sourcefile('keywords.csv')
        source_keyw["low_keyw"]=source_keyw["keyword"].str.lower()
        self._unique_keywords=source_keyw.low_keyw.unique()




