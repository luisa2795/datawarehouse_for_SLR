import pandas as pd
from gensim.parsing.preprocessing import strip_numeric
import re
import numpy as np
from transformators.base_transformator import BaseTransformator 


class AuthorTransformator (BaseTransformator):
    """Extracts, Transforms and Loads the author dimension.
    
    Args:
        sourcepath (str): path to the folder containing all relevant source csv-files. [inherited]
        connection_params (dict): dictionary containing connection parameters of target database for the DWH (username, password, host, port, database). [inherited]
    
    Attributes:
        sourcepath (str): path to the folder containing all relevant source csv-files. [inherited]
        _targetdb (Database): initialized target database. [inherited]
        #_unique_keywords (Series): unique lowercased keywords from the source csv-file.
        #_keywords_in_dwh (DataFrame): dataframe copy of current data in the dim_keyword table in the DWH.
     """

    def __init__(self, sourcepath, connection_params):
        #init is inherited from base class
        super().__init__(sourcepath, connection_params)
        self._unique_authors=None
        self._authors_in_dwh = self._targetdb.load_full_table('dim_author')
        self._max_pk=max(self._authors_in_dwh.author_pk, default=0)

    def load_unique_authors(self):
        """Loads data from authors and unique_references sourcefiles, merges them and merges duplicate authors.
        """
        from_authors=self.load_sourcefile('authors.csv')
        auth_authors=self._clean_authors_from_authors(from_authors)
        from_references=self.load_sourcefile('unique_references.csv')
        reference_authors=self._clean_authors_from_references(from_references)
        self._unique_authors=auth_authors.append(reference_authors, sort=False, ignore_index=True)

    def write_delta_authors_to_dwh(self):
        """Finds authors in source table that are not yet represented in the DWH. 
        Then those authors are appended as new rows to the dim_author table with subsequent primary keys.

        SLOWLY CHANGING DIMENSIONS
    
        """
        if not self._unique_authors.empty:
            #add the columns needed for SCD capturing
            self._unique_authors['row_effective_date']=pd.to_datetime('today').normalize()
            self._unique_authors['row_expiration_date']=pd.Timestamp.max.normalize()
            self._unique_authors['current_row_indicator']='Current'
            
            #perform a left join on the fresh source data and the DWH data on the author names
            left=pd.merge(self._unique_authors, self._authors_in_dwh, how = 'left', on=['surname', 'firstname', 'middlename'], suffixes=[None, '_db'])
            #get the rows that were not previously present in the DWH
            completely_new=left[left.current_row_indicator_db.isna()]
            [['surname', 'firstname', 'middlename', 'email', 'departments', 'institutions', 'countries', 'row_effective_date', 'row_expiration_date', 'current_row_indicator']]
            #and already insert it into the DWH
            completely_new['author_pk']=list(range(self._max_pk+1, self._max_pk+1+completely_new.size))
            completely_new.rename(columns={'departments': 'department', 'institutions': 'institution', 'countries': 'country'})
            self.write_to_dwh(completely_new, 'dim_author')
            #increase max_pk by the size of the just inserted dataframe
            self._max_pk=self._max_pk+1+completely_new.size

            #get the rows that were in the DWH before already
            maybe_changed=left[~left.current_row_indicator_db.isna()]
            #check whether the information about 'departments', 'institutions', 'countries' in the intersecting rows has really changed and if it is not just all NaN - here we would want to add a new row (type 2 SCD)
            SCD2_change=maybe_changed[(
                (maybe_changed['departments']!=maybe_changed['departments_db']) | 
                (maybe_changed['institutions']!=maybe_changed['institutions_db']) | 
                (maybe_changed['countries']!=maybe_changed['countries_db'])
                ) & 
                (~maybe_changed[['departments', 'institutions', 'countries', 'departments_db', 'institutions_db', 'countries_db']].isnull().all(1))
                ]
            #here we will change in the existing row the expiration date to today and mark the entry as 'Expired', then add a new row with the fresh institute data as it is in the source
            self._update_SCD2_attributes(SCD2_change)
            
            #find changes in email but no other changes
            SCD1_change=maybe_changed[
                (maybe_changed['email']!=maybe_changed['email_db']) & 
                (~maybe_changed.index.isin(SCD2_change.index)) & 
                (~maybe_changed['email'].isnull())
                ]
            #here we will just overwrite the email with the email from source data
            self._update_SCD1_attributes(SCD1_change)
        else:
            raise AttributeError('Please load unique authors first.')

    def _clean_authors_from_references(self, references_df):
        #create a new dataframe of authors where each author gets an own row and empty rows are discarded
        ref_aut=pd.Series(references_df['authors'].str.split('; ').explode(ignore_index=True).str.split(', ').dropna())
        #remove the 'Van' if existing in strings that are longer tan 2, as checks have shown these are most probably parsing errors
        ref_aut.apply(lambda l: (l.remove('Van') if 'Van' in l else l) if len(l)>2 else l)
        #keep only the rows with a length of 2 now:
        keep=ref_aut[ref_aut.str.len()==2]
        #those that are longer must be changed
        change1=ref_aut[ref_aut.str.len()>2]
        #first split every string in list from each other, then split into sublist pairs of two
        change1=change1.apply(lambda l: self._split_into_lists_of_two_strings(l))
        #and append now corrected series again to keep
        keep.append(change1.explode())
        #those that are shorter must be changed
        change2=ref_aut[ref_aut.str.len()<2]
        #those that are shorter than 2 letters can be dropped
        change2=change2[change2.explode().str.len()>2]
        #many rows contain names that are just not separated by comma and therefore not recognized. lets split them into sublist of two strings each
        change2=change2.apply(lambda l: self._split_into_lists_of_two_strings(l))
        #unnest lists to new rows
        change2=change2.explode()
        #keep only those that contain two strings
        change2=change2[change2.str.len()==2]
        #and append the transformed rows to keep
        keep.append(change2)
        #generate a new dataframe
        ref_aut_df=pd.DataFrame(keep)
        #split up authors column into surname and firstname
        ref_aut_df[['surname', 'firstname']]=pd.DataFrame(ref_aut_df.authors.tolist(), index=ref_aut_df.index)
        #keep only those two columns
        reference_authors=ref_aut_df[['surname', 'firstname']]
        #last, remove duplicate authors
        reference_authors.drop_duplicates(inplace=True, ignore_index=True)
        return reference_authors

    def _clean_authors_from_authors(self, authors_df):
        #some cells in the source data still contain numbers, html tags or @ tags, these are removed
        authors_df.fullname=authors_df.fullname.apply(lambda fn: self._remove_numbers_amp_and_at_tags(fn))
        #then split the fullname again into the columns first-, middle- and surname
        authors_df[['surname', 'firstname', 'middlename']]=pd.DataFrame(authors_df.fullname.apply(lambda fn: self._split_fullname(fn)).to_list(), index=authors_df.index)
        #merge duplicate authors, if fullnames are identical. From email and institute information take the majority, if any, otherwise impute 'MISSING'
        aggregate_functions={'surname': 'first', 
        'firstname': 'first', 
        'middlename': 'first', 
        'email': lambda x: self._try_impute_missing(x.mode()),
        'departments': lambda x: self._try_impute_missing(x.mode()), 
        'institutions': lambda x: self._try_impute_missing(x.mode()), 
        'countries': lambda x: self._try_impute_missing(x.mode())}
        authors_df= authors_df.groupby('fullname')[['firstname', 'middlename', 'surname', 'email','departments', 'institutions', 'countries']].agg(aggregate_functions)
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

    def _remove_numbers_amp_and_at_tags(fullname):
        """removes numbers, &amp tags, @institutions, semicolons and round brackets.
        
        Args:
            fullname(str): the fullname of an author entry.
        
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
        #TODO docstrings
        try:
            return item[0]
        except:
            return np.nan

    def _update_SCD2_attributes(self, SCD2_data):
        #TODO docstrings
        for index, row in SCD2_data.iterrows():
            #set existing entry to expired
            sql_update_expired="""UPDATE dim_author SET row_expiration_date={}, current_row_indicator='Expired' WHERE surname={}, firstname={}, middlename={}""".format(
                row['row_effective_date'], row['surname'], row['firstname'], row['middlename']
            )
            self._targetdb.execute_update_query(sql_update_expired)
            #insert a new row with fresh attributes
            sql_insert_current="""INSERT INTO dim_author VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})""".format(
                self._max_pk, row['surname'], row['firstname'], row['middlename'], 
                row['email'], row['departments'], row['institutions'], row['countries'],
                row['row_effective_date'], row['row_expiration_date'], row['current_row_indicator']
            )
            self._targetdb.execute_update_query(sql_insert_current)
            self._max_pk+=1

    def _update_SCD1_attributes(self, SDC1_data):
        #TODO docstrings
        for index, row in SCD1_data.iterrows():
            #overwrite existing entry email
            sql_update_mail="""UPDATE dim_author SET email={} WHERE surname={}, firstname={}, middlename={}""".format(
                row['email'], row['surname'], row['firstname'], row['middlename']
            )
            self._targetdb.execute_update_query(sql_update_mail)