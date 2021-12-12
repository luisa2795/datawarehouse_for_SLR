import etl.database as db
import etl.dim_keyword as keyw
import etl.dim_author as auth
import etl.dim_journal as jour
import etl.dim_paper as pape
from db_credentials	import dwh_db_connection_params


if __name__ == "__main__":
    
    process_step = input('Which process step should be executed? ')
    #initialize engine
    eng, psycop2connect=db.initialize_engine(connection_params=dwh_db_connection_params)

    if process_step == 'Keyword ETL':
        keywords_in_dwh = db.load_full_table(eng, 'dim_keyword')
        unique_source_keywords=keyw.extract_unique_keywords_from_file()
        delta_keywords=keyw.transform_delta_keywords(unique_source_keywords, keywords_in_dwh)
        db.insert_to_database(eng, delta_keywords, 'dim_keyword')
    
    elif process_step == 'Author ETL':
        authors_in_dwh = db.load_full_table(eng, 'dim_author')
        source_authors=auth.extract_unique_authors_from_files()
        new, SCD2, SCD1=auth.tramsform_delta_authors(source_authors, authors_in_dwh)
        db.insert_to_database(eng, new, 'dim_author')
        auth.update_SCD2_attributes(psycop2connect, SCD2, authors_in_dwh)
        auth.update_SCD1_attributes(psycop2connect, SCD1)
        #TODO: solve SCD issues, see todos in AuthorTransformator
        
    elif process_step == 'Journal ETL':
        journals_in_dwh=db.load_full_table(eng, 'dim_journal')
        source_journals=jour.extract_unique_journals_from_files()
        delta_journals=jour.transform_delta_journals(source_journals, journals_in_dwh)
        db.insert_to_database(eng, delta_journals, 'dim_journal')
        
    else:
        pass

