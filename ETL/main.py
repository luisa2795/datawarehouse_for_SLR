from database import Database
from transformators.keyword_transformator import KeywordTransformator
from transformators.author_transformator import AuthorTransformator
from transformators.journal_transformator import JournalTransformator
from db_credentials	import dwh_db_connection_params
from database import Database
from variables import sourcepath
testpath='/data/causeminer/luisa_waack/datawarehouse_for_SLR/ETL/testfiles'

if __name__ == "__main__":
    
    process_step = input('Which process step should be executed? ')

    if process_step == 'Keyword ETL':
        keyw_trans=KeywordTransformator(sourcepath, dwh_db_connection_params)
        keyw_trans.load_unique_keywords()
        keyw_trans.write_delta_keywords_to_dwh()
    
    elif process_step == 'Author ETL':
        auth_trans=AuthorTransformator(sourcepath, dwh_db_connection_params)
        auth_trans.load_unique_authors()
        #TODO: solve SCD issues, see todos in AuthorTransformator
        auth_trans.write_delta_authors_to_dwh()
        
    elif process_step == 'Journal ETL':
        jour_trans=JournalTransformator(sourcepath, dwh_db_connection_params)
        jour_trans.load_unique_journals()
        jour_trans.write_delta_journals_to_dwh()

    else:
        pass

