import pandas as pd
from transformators.base_transformator import BaseTransformator 
import roman

class JournalTransformator (BaseTransformator):
    """"
    xyz
    """
    def __init__(self, sourcepath, connection_params):
        super().__init__(sourcepath, connection_params)
        self._unique_journals=None
        self._journals_in_dwh=self._targetdb.load_full_table('dim_journal')
        self._max_pk=max(self._journals_in_dwh.journal_pk, default=0)

    def load_unique_journals(self):
        """Loads uniqe journals from papers and references and triggers cleaning and removal of duplicates.
        """
        from_papers=self.load_sourcefile('papers_final.csv')[['journal', 'volume', 'issue', 'publisher', 'place']]
        from_references=self.load_sourcefile('unique_references.csv')[['journal', 'volume', 'issue', 'publisher', 'place']]
        all_journals=from_references.append(from_papers, ignore_index=True).rename(columns={'journal': 'title'})
        all_journals.dropna(axis=0, how='all', inplace=True)
        all_journals.fillna({'title': 'MISSING', 'volume':0, 'issue': 0, 'publisher': 'MISSING', 'place': 'MISSING'}, inplace=True)
        all_journals.volume=all_journals.volume.apply(lambda v: self._volume_to_int(v))
        all_journals.issue=all_journals.issue.apply(lambda i: self._issue_to_int(i))
        all_journals.drop_duplicates(inplace=True)
        self._unique_journals=all_journals

    def write_delta_journals_to_dwh(self):
        """Finds journals in source table that are not yet represented in the DWH. 
        Then those journals are appended as new rows to the dim_journal table with subsequent primary keys.
    
        """
        if not self._unique_journals.empty:
            #determine which journals have not yet been inserted into table
            outer=pd.merge(self._unique_journals, self._journals_in_dwh, how='outer')[['title', 'volume', 'issue', 'publisher', 'place']]
            delta_journals=pd.concat([outer,self._journals_in_dwh]).drop_duplicates(keep=False)
            #add a consecutive key, starting from max_pk +1
            delta_journals['journal_pk']=list(range(self._max_pk+1, self._max_pk+1+delta_journals.index.size))
            self.write_to_dwh(delta_journals, 'dim_journal')
        else:
            raise AttributeError('Please load unique journals first.') 


    def _volume_to_int(self, volume):
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

    def _issue_to_int(self, issue):
        try: 
            iss=int(issue)
        except:
            try:
                iss=int(issue[0])
            except:
                iss=0
        return iss
    