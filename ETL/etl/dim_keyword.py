import pandas as pd
import common_functions as cof


def extract_unique_keywords_from_file(): 
    """Loads all keywords from source file, lowercases them and returns unique series of keywords.

    Returns:
        Series of unique keywords.    
    """
    source_keyw=cof.load_sourcefile('keywords.csv')
    source_keyw["low_keyw"]=source_keyw["keyword"].str.lower()
    unique_keywords=pd.Series(source_keyw.low_keyw.unique())
    return unique_keywords

def transform_delta_keywords(unique_keywords, keywords_in_dwh):
    """Finds keywords in source table that are not yet represented in the DWH. 
    Then those keywords are returned as new rows with subsequent primary keys, ready for loading.

    """
    #determine which keywords have not yet been inserted into table
    delta_keywords=unique_keywords[~unique_keywords.isin(keywords_in_dwh.keyword_string)]
    #determine highest primary key in the table
    max_pk=max(keywords_in_dwh.keyword_pk, default=0)
    #create df from delta keywords and a consecutive key, starting from max_pk +1
    delta_keyword_df=pd.DataFrame(data=list(range(max_pk+1, max_pk+1+delta_keywords.size)),columns=['keyword_pk'])
    delta_keyword_df['keyword_string']=delta_keywords
    return delta_keyword_df




