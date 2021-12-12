import pandas as pd
import common_functions as cof


    # def __init__(self, sourcepath, connection_params):
    #     super().__init__(sourcepath, connection_params)
    #     self._unique_papers=None
    #     self._papers_in_dwh = self._targetdb.load_full_table('dim_paper')
    #     self._max_pk=max(self._papers_in_dwh.author_pk, default=0)
    
def extract_unique_papers():
    """Loads data from papers_final and unique_references sourcefiles, merges them and merges duplicate papers.
    """
    from_papers=cof.load_sourcefile('papers_final.csv')
    #auth_authors=self._clean_authors_from_authors(from_authors)
    from_references=cof.load_sourcefile('unique_references.csv')
    #reference_authors=self._clean_authors_from_references(from_references)
    #self._unique_authors=pd.merge(auth_authors, reference_authors, how='outer', on=['surname', 'firstname', 'middlename'], suffixes=[None, '_ref'])[['surname', 'firstname', 'middlename', 'email', 'department', 'institution', 'country']]

