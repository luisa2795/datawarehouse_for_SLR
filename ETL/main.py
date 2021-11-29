from ETL.database import Database
from ETL.transformators.keyword_transformator import Keyword_Transformator
from db_credentials	import dwh_db_connection_params
from database import Database
from variables import sourcepath

if __name__ == "__main__":
    
    dwh_db = Database(dwh_db_connection_params)
    keyw_trans=Keyword_Transformator(sourcepath)
    