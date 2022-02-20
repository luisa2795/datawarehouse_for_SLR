import pandas as pd
from gensim.parsing.preprocessing import strip_numeric
import re
import numpy as np
import etl.common_functions as cof


def extract_unique_authors_from_files():
    """Loads data from authors and unique_references sourcefiles, merges them and merges duplicate authors.
    
    Returns:
        Dataframe of cleaned and conformed authors.
    """
    from_authors=_clean_authors_from_authors()
    from_references=_clean_authors_from_references()
    unique_authors=pd.merge(from_authors, from_references, how='outer', on=['surname', 'firstname', 'middlename'], suffixes=[None, '_ref'])[['surname', 'firstname', 'middlename', 'email', 'department', 'institution', 'country']]
    return unique_authors


def tramsform_delta_authors(source_authors, authors_in_dwh):
    """Finds authors in source table that are not yet represented in the DWH. 
    Then those authors are appended as new rows to the dim_author table with subsequent primary keys.
    changes in the columns 'email', 'department', 'institution', 'country' are ignored (SCD0 do nothing)
    Args:
        source_authors (DataFrame): cleaned and conformed authors from source files.
        authors_in_dwh (DataFrame): currently present rows in database table dim_author.
    Returns:
        Dataframe to append to dim_author db table.
    """

    #fill NaN and Null values to not violate NOT NULL constraint in DWH
    source_authors.fillna(value='MISSING', inplace=True)
    #perform a left join on the fresh source data and the DWH data on the author names
    left=pd.merge(source_authors, authors_in_dwh, how = 'left', on=['surname', 'firstname', 'middlename'], suffixes=[None, '_db'])
    #get the rows that were not previously present in the DWH
    completely_new=left[left.author_pk.isna()][['surname', 'firstname', 'middlename', 'email', 'department', 'institution', 'country']]
    #and already insert it into the DWH if it is not empty
    max_pk=max(authors_in_dwh.author_pk, default=0)
    if not completely_new.empty:
        completely_new['author_pk']=list(range(max_pk+1, max_pk+1+completely_new.index.size))
        #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
        if max_pk==0:
            dummy_author={'author_pk': 0, 'surname': 'MISSING', 'firstname': 'MISSING', 'middlename': 'MISSING', 'email': 'MISSING', 'department': 'MISSING', 'institution': 'MISSING', 'country': 'MISSING'}
            completely_new=pd.concat([completely_new, pd.DataFrame([dummy_author])], ignore_index=True)
            #completely_new=completely_new.append({'author_pk': 0, 'surname': 'MISSING', 'firstname': 'MISSING', 'middlename': 'MISSING', 'email': 'MISSING', 'department': 'MISSING', 'institution': 'MISSING', 'country': 'MISSING'}, ignore_index=True)
    return completely_new

def _clean_authors_from_references():
    """Loads and cleans the source data from the unique_references.csv file to the desired format.
    
    Returns:
        The cleaned DataFrame containing surname, firstname and middlename ('MISSING' in all cases) of reference authors.
    """
    references_df=cof.load_sourcefile('unique_references.csv')
    #create a new dataframe of authors where each author gets an own row and empty rows are discarded
    ref_aut=pd.Series(references_df['authors'].str.split('; ').explode(ignore_index=True).str.split(', ').dropna())
    #remove the 'Van' if existing in strings that are longer tan 2, as checks have shown these are most probably parsing errors
    ref_aut.apply(lambda l: (l.remove('Van') if 'Van' in l else l) if len(l)>2 else l)
    #keep only the rows with a length of 2 now:
    keep=ref_aut[ref_aut.str.len()==2]
    #those that are longer must be changed
    change1=ref_aut[ref_aut.str.len()>2]
    #first split every string in list from each other, then split into sublist pairs of two
    change1=change1.apply(lambda l: cof.split_into_lists_of_two_strings(l))
    #and append now corrected series again to keep
    keep=pd.concat([keep, change1.explode()], ignore_index=True)
    #those that are shorter must be changed
    change2=ref_aut[ref_aut.str.len()<2]
    #those that are shorter than 2 letters can be dropped
    change2=change2[change2.explode().str.len()>2]
    #many rows contain names that are just not separated by comma and therefore not recognized. lets split them into sublist of two strings each
    change2=change2.apply(lambda l: cof.split_into_lists_of_two_strings(l))
    #unnest lists to new rows
    change2=change2.explode()
    #keep only those that contain two strings
    change2=change2[change2.str.len()==2]
    #and append the transformed rows to keep
    keep=pd.concat([keep, change2], ignore_index=True)
    #generate a new dataframe
    ref_aut_df=pd.DataFrame(keep)
    #split up authors column into surname and firstname
    ref_aut_df[['surname', 'firstname']]=pd.DataFrame(ref_aut_df.authors.tolist(), index=ref_aut_df.index)
    #keep only those two columns and add an empty column for middlename
    reference_authors=ref_aut_df[['surname', 'firstname']]
    reference_authors['middlename']=None
    #last, remove duplicate authors
    reference_authors.drop_duplicates(inplace=True, ignore_index=True)
    return reference_authors

def _clean_authors_from_authors():
    """Loads and cleans the source data from the authors.csv file to the desired format.
    
    Returns:
        The cleaned DataFrame of conformed and aggregated authors.
    """
    authors_df=cof.load_sourcefile('authors.csv').rename(columns={'departments': 'department', 'institutions': 'institution', 'countries': 'country'})
    #some cells in the source data still contain numbers, html tags or @ tags, these are removed
    authors_df.fullname=authors_df.fullname.apply(lambda f: _remove_numbers_tags_and_signs(f))
    #then split the fullname again into the columns first-, middle- and surname
    authors_df[['surname', 'firstname', 'middlename']]=pd.DataFrame(authors_df.fullname.apply(lambda fn: _split_fullname(fn)).to_list(), index=authors_df.index)
    #merge duplicate authors, if fullnames are identical. From email and institute information take the majority, if any, otherwise impute 'MISSING'
    aggregate_functions={'surname': 'first', 
    'firstname': 'first', 
    'middlename': 'first', 
    'email': lambda x: _try_impute_missing(x.mode()),
    'department': lambda x: _try_impute_missing(x.mode()), 
    'institution': lambda x: _try_impute_missing(x.mode()), 
    'country': lambda x: _try_impute_missing(x.mode())}
    authors_df= authors_df.groupby('fullname')[['firstname', 'middlename', 'surname', 'email','department', 'institution', 'country']].agg(aggregate_functions)
    #TODO: merge authors if surname and fullname are identical and for the rest of the values everything is missing, current merging allows two entries (Abbott, Pamela, MISSING) and (Abbott, Pamela, Y)
    return authors_df

def _remove_numbers_tags_and_signs(fullname):
    """removes numbers, tags, semicolons and round brackets.
    
    Args:
        fullname (str): the fullname of an author entry.
    
    Returns:
        The cleaned string.
    """
    fullname=strip_numeric(fullname)
    fullname=re.sub('&\w+', '', fullname)
    fullname=re.sub('@\w+', '', fullname)
    fullname=re.sub('\| ', '', fullname)
    fullname=re.sub('[;().|]', '', fullname)
    fullname=fullname.strip()
    return fullname

def _split_fullname(fullname):
    """Splits a fullname into first-, middle- and surname.
    
    Args:
        fullname(str): the string of the fullname.
    
    Returns:
        The strings surname, firstname and middlename.
    """
    fn_list=fullname.split(', ')
    fn_surname=fn_list[0]
    if len(fn_list)>1:
        first_middle=fn_list[1].split(' ')
        firstname=first_middle[0]
        if len(first_middle)>1:
            middlename=first_middle[1]
        else:
            middlename=np.nan
    else:
        firstname=np.nan
        middlename=np.nan
    return fn_surname, firstname, middlename

def _try_impute_missing (item):
    """This  function is needed when loading authors for the first time and removing duplicates.
    When some attributes of the same author are equally often present in the source data, take the first option. 
    If an attribute is missing in all entries of the same author, return NaN.
    
    Args:
        item(list or str): The attribute mode, it is a list if there are several most frequent values for one attribute.
        
    Returns:
        Atomic attribute value, either the first option or NaN.
    """
    try:
        return item[0]
    except:
        return np.nan