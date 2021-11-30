from database import Database
from transformators.keyword_transformator import KeywordTransformator
from db_credentials	import dwh_db_connection_params
from database import Database
from variables import sourcepath

if __name__ == "__main__":
    
    keyw_trans=KeywordTransformator(sourcepath, dwh_db_connection_params)
    keyw_trans.load_unique_keywords()
    keyw_trans.write_delta_keywords_to_dwh()
    

