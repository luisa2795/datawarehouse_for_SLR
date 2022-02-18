import pandas as pd
from gensim.parsing.preprocessing import strip_numeric
import re
import numpy as np
import etl.common_functions as cof
import etl.database as db


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

    SLOWLY CHANGING DIMENSIONS:
    If the author changed Department, Institute or county, the old entry is set to expired and a new row is inserted in the table
    If only the email adress changes, it is overwritten in the existing row.

    Args:
        source_authors (DataFrame): cleaned and conformed authors from source files.
        authors_in_dwh (DataFrame): currently present rows in database table dim_author.
    Returns:
        Dataframe to append to dim_author db table.
        Dataframe of candidates for slowly changing dimensions type 2.
        Dataframe of candidates for slowly changing dimensions type 2.
    """
    #add the columns needed for SCD capturing
    #JUST FOR TESTING, TODO: remove
    #self._unique_authors['row_effective_date']=(pd.to_datetime('today')+pd.DateOffset(days=3)).normalize()
    source_authors['row_effective_date']=pd.to_datetime('today').normalize()
    source_authors['row_expiration_date']=pd.Timestamp.max.normalize()
    source_authors['current_row_indicator']='Current'
    #fill NaN and Null values to not violate NOT NULL constraint in DWH
    source_authors.fillna(value='MISSING', inplace=True)
    #perform a left join on the fresh source data and the DWH data on the author names
    left=pd.merge(source_authors, authors_in_dwh, how = 'left', on=['surname', 'firstname', 'middlename'], suffixes=[None, '_db'])
    #get the rows that were not previously present in the DWH
    completely_new=left[left.current_row_indicator_db.isna()][['surname', 'firstname', 'middlename', 'email', 'department', 'institution', 'country', 'row_effective_date', 'row_expiration_date', 'current_row_indicator']]
    #and already insert it into the DWH if it is not empty
    max_pk=max(authors_in_dwh.author_pk, default=0)
    if not completely_new.empty:
        completely_new['author_pk']=list(range(max_pk+1, max_pk+1+completely_new.index.size))
        #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
        if max_pk==0:
            completely_new=completely_new.append({'author_pk': 0, 'surname': 'MISSING', 'firstname': 'MISSING', 'middlename': 'MISSING', 'email': 'MISSING', 'department': 'MISSING', 'institution': 'MISSING', 'country': 'MISSING', 'row_effective_date': pd.to_datetime('today').normalize(), 'row_expiration_date': pd.Timestamp.max.normalize(), 'current_row_indicator': 'Current'}, ignore_index=True)
        #increase max_pk by the size of the just inserted dataframe
        max_pk=max_pk+1+completely_new.index.size
    #get the rows that were in the DWH before already
    maybe_changed=left[~left.current_row_indicator_db.isna()]
    if not maybe_changed.empty:
        #check whether the information about 'departments', 'institutions', 'countries' in the intersecting rows has really changed and if it is not just all NaN - here we would want to add a new row (type 2 SCD)
        SCD2_change=maybe_changed[(
            (maybe_changed['department']!=maybe_changed['department_db']) | 
            (maybe_changed['institution']!=maybe_changed['institution_db']) | 
            (maybe_changed['country']!=maybe_changed['country_db'])
            ) & 
            (~maybe_changed[['department', 'institution', 'country', 'department_db', 'institution_db', 'country_db']].isnull().all(1))
            ]
    
        #find changes in email but no other changes
        SCD1_change=maybe_changed[
            (maybe_changed['email']!=maybe_changed['email_db']) & 
            (~maybe_changed.index.isin(SCD2_change.index)) & 
            (~maybe_changed['email'].isnull())
            ]
    else:
        SCD2_change=pd.DataFrame()
        SCD1_change=pd.DataFrame()
    return completely_new, SCD2_change, SCD1_change

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
    change1=change1.apply(lambda l: _split_into_lists_of_two_strings(l))
    #and append now corrected series again to keep
    keep=keep.append(change1.explode())
    #those that are shorter must be changed
    change2=ref_aut[ref_aut.str.len()<2]
    #those that are shorter than 2 letters can be dropped
    change2=change2[change2.explode().str.len()>2]
    #many rows contain names that are just not separated by comma and therefore not recognized. lets split them into sublist of two strings each
    change2=change2.apply(lambda l: _split_into_lists_of_two_strings(l))
    #unnest lists to new rows
    change2=change2.explode()
    #keep only those that contain two strings
    change2=change2[change2.str.len()==2]
    #and append the transformed rows to keep
    keep=keep.append(change2)
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
    return authors_df

def _split_into_lists_of_two_strings(names):
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

def update_SCD2_attributes(psycop2connect, SCD2_data, engine):
    #TODO docstrings
    authors_in_dwh=db.load_full_table(engine, 'dim_author')
    max_pk=max(authors_in_dwh.author_pk, default=0)
    for index, row in SCD2_data.iterrows():
        #set existing entry to expired
        #TODO: SQL statement looks good, why does it not get executed on Db server? Test with authors_SCD.csv testset
        sql_update_expired="""UPDATE dim_author SET row_expiration_date='{}', current_row_indicator='Expired' WHERE surname='{}' AND firstname='{}' AND middlename='{}'""".format(
            row['row_effective_date'].date(), row['surname'], row['firstname'], row['middlename']
        )
        db.execute_query(psycop2connect, sql_update_expired)
        #insert a new row with fresh attributes
        max_pk+=1
        sql_insert_current="""INSERT INTO dim_author VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(
            max_pk, row['surname'], row['firstname'], row['middlename'], 
            row['email'], row['department'], row['institution'], row['country'],
            row['row_effective_date'].date(), row['row_expiration_date'].date(), row['current_row_indicator']
        )
        db.execute_query(psycop2connect, sql_insert_current)

def update_SCD1_attributes(psycop2connect, SDC1_data):
    #TODO docstrings
    for index, row in SDC1_data.iterrows():
        #TODO:SQL statement looks good, why does it not get executed on Db server? Test with authors_SCD.csv testset
        sql_update_mail="""UPDATE dim_author SET email='{}' WHERE surname='{}' AND firstname='{}' AND middlename='{}'""".format(
            row['email'], row['surname'], row['firstname'], row['middlename']
        )
        db.execute_query(psycop2connect, sql_update_mail)