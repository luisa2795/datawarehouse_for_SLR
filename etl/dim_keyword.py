import pandas as pd
import etl.common_functions as cof


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

    Args:
        unique_keywords (Series): unique keywords from the source file.
        keywords_in_dwh (DataFrame): df of the current data present in the DB table dim_keyword.
    
    Returns:
        DataFrame of delta keyword rows with subsequent primary keys, ready to be added to DB table.
    """
    #determine which keywords have not yet been inserted into table
    delta_keywords=unique_keywords[~unique_keywords.isin(keywords_in_dwh.keyword_string)]
    #determine highest primary key in the table
    max_pk=max(keywords_in_dwh.keyword_pk, default=0)
    #create df from delta keywords and a consecutive key, starting from max_pk +1
    delta_keyword_df=pd.DataFrame(data=list(range(max_pk+1, max_pk+1+delta_keywords.size)),columns=['keyword_pk'])
    delta_keyword_df['keyword_string']=delta_keywords
    #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
    if max_pk==0:
        delta_keyword_df=delta_keyword_df.append({'keyword_pk': 0, 'keyword_string': 'MISSING'}, ignore_index=True)
    return delta_keyword_df




