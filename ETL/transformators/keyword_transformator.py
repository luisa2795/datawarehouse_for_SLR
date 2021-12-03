import pandas as pd
from transformators.base_transformator import BaseTransformator 

class KeywordTransformator (BaseTransformator):
    """Extracts, Transforms and Loads the keywords dimension.
    
    Args:
        sourcepath (str): path to the folder containing all relevant source csv-files. [inherited]
        connection_params (dict): dictionary containing connection parameters of target database for the DWH (username, password, host, port, database). [inherited]
    
    Attributes:
        sourcepath (str): path to the folder containing all relevant source csv-files. [inherited]
        _targetdb (Database): initialized target database. [inherited]
        _unique_keywords (Series): unique lowercased keywords from the source csv-file.
        _keywords_in_dwh (DataFrame): dataframe copy of current data in the dim_keyword table in the DWH.
     """

    def __init__(self, sourcepath, connection_params):
        #init is inherited from base class
        super().__init__(sourcepath, connection_params)
        self._unique_keywords=None
        self._keywords_in_dwh = self._targetdb.load_full_table('dim_keyword')


    def load_unique_keywords(self): 
        """Loads all keywords from source file, lowercases them and saves unique keywords to _unique_keywords class attribute.
        
        """
        source_keyw=super().load_sourcefile('keywords.csv')
        source_keyw["low_keyw"]=source_keyw["keyword"].str.lower()
        self._unique_keywords=pd.Series(source_keyw.low_keyw.unique())

    def write_delta_keywords_to_dwh(self):
        """Finds keywords in source table that are not yet represented in the DWH. 
        Then those keywords are appended as new rows to the dim_keyword table with subsequent primary keys.
    
        """
        if not self._unique_keywords.empty:
            #determine highest primary key in the table
            max_pk=max(self._keywords_in_dwh.keyword_pk, default=0)
            #determine which keywords have not yet been inserted into table
            delta_keywords=self._unique_keywords[~self._unique_keywords.isin(self._keywords_in_dwh.keyword_string)]
            #create df from delta keywords and a consecutive key, starting from max_pk +1
            delta_keyword_df=pd.DataFrame(data=list(range(max_pk+1, max_pk+1+delta_keywords.size)),columns=['keyword_pk'])
            delta_keyword_df['keyword_string']=delta_keywords
            self.write_to_dwh(delta_keyword_df, 'dim_keyword')
        else:
            raise AttributeError('Please load unique keywords first.')




