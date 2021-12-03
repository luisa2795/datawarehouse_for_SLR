import pandas as pd
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
        #self._keywords_in_dwh = self._targetdb.load_full_table('dim_keyword')

    def load_unique_authors(self):
        """Loads data from authors and unique_references sourcefiles, merges them and merges duplicate authors.
        """
        from_authors=self.load_sourcefile('authors.csv')
        auth_authors=self._clean_authors_from_authors(from_authors)
        from_references=self.load_sourcefile('unique_references.csv')
        reference_authors=self._clean_authors_from_references(from_references)

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

    def _clean_authors_from_authors(authors_df):
        pass

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