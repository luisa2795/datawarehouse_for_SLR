import pandas as pd
import etl.common_functions as cof
import roman

def extract_unique_journals_from_files():
    """Loads unique journals from papers and references and triggers cleaning and removal of duplicates.

    Returns:
        DataFrame of cleaned and unique journals from source files.
    """
    from_papers=cof.load_sourcefile('papers_final.csv')[['journal', 'volume', 'issue', 'publisher', 'place']]
    from_references=cof.load_sourcefile('unique_references.csv')[['journal', 'volume', 'issue', 'publisher', 'place']]
    all_journals=from_references.append(from_papers, ignore_index=True).rename(columns={'journal': 'title'})
    all_journals.dropna(axis=0, how='all', inplace=True)
    all_journals.fillna({'title': 'MISSING', 'volume':0, 'issue': 0, 'publisher': 'MISSING', 'place': 'MISSING'}, inplace=True)
    all_journals.volume=all_journals.volume.apply(lambda v: _volume_to_int(v))
    all_journals.issue=all_journals.issue.apply(lambda i: _issue_to_int(i))
    all_journals.drop_duplicates(inplace=True)
    return all_journals

def transform_delta_journals(source_journals, journals_in_dwh):
    """Finds journals in source table that are not yet represented in the DWH and adds a column of subsequent primary keys.

    Args:
        source_journals (DataFrame): journals from the source files.
        journals_in_dwh (DataFrame): df of the current table dim_journal.
    
    Returns:
        DataFrame containing delta journals not yet present in DB in a transformed format, with pk, ready to load.
    """
    #determine which journals have not yet been inserted into table
    outer=pd.merge(source_journals, journals_in_dwh, how='outer')[['title', 'volume', 'issue', 'publisher', 'place']]
    delta_journals=pd.concat([outer,journals_in_dwh]).drop_duplicates(keep=False)
    #add a consecutive key, starting from max_pk +1
    max_pk=max(journals_in_dwh.journal_pk, default=0)
    delta_journals['journal_pk']=list(range(max_pk+1, max_pk+1+delta_journals.index.size))
    #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
    if max_pk==0:
        delta_journals=delta_journals.append({'journal_pk': 0, 'title': 'MISSING', 'volume':0, 'issue': 0, 'publisher': 'MISSING', 'place': 'MISSING'}, ignore_index=True)
    return delta_journals

def _volume_to_int(volume):
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

def _issue_to_int(issue):
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
