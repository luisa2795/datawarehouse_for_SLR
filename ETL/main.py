import etl.database as db
import etl.common_functions as cof
import etl.dim_keyword as keyw
import etl.dim_author as auth
import etl.dim_journal as jour
import etl.dim_paper as pape
import etl.dim_paragraph as para
import etl.dim_sentence as sent
from db_credentials	import dwh_db_connection_params
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

#TODO write down which step depends on which other step
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
        #TODO: solve SCD issues, see todos in AuthorTransformator
        auth.update_SCD2_attributes(psycop2connect, SCD2, eng)
        auth.update_SCD1_attributes(psycop2connect, SCD1)
            
    elif process_step == 'Journal ETL':
        journals_in_dwh=db.load_full_table(eng, 'dim_journal')
        source_journals=jour.extract_unique_journals_from_files()
        delta_journals=jour.transform_delta_journals(source_journals, journals_in_dwh)
        db.insert_to_database(eng, delta_journals, 'dim_journal')
        
    elif process_step == 'Paper ETL':
        articles_df, references_df=pape.extract_all_papers()
        #first prepare papers from 'papers_final'
        articles_prep=pape.transform_papers(articles_df, eng)
        references_prep=pape.transform_references(references_df, eng)
        final_source_papers=pape.merge_all_papers(references_prep, articles_prep)
        papers_in_dwh=db.load_full_table(eng, 'dim_paper')
        delta_papers, delta_keywordgroup, delta_keywordbridge, delta_authorgroup, delta_authorbridge=pape.find_delta_papers(final_source_papers, papers_in_dwh)
        #insert everything to db tables. Attention, order matters here to not violate foreign key constraints!
        #TODO: make all inserts one transaction that rolls back when one insert fails
        db.insert_to_database(eng, delta_keywordgroup, 'dim_keywordgroup')
        db.insert_to_database(eng, delta_keywordbridge, 'bridge_paper_keyword')
        db.insert_to_database(eng, delta_authorgroup, 'dim_authorgroup')
        db.insert_to_database(eng, delta_authorbridge, 'bridge_paper_author')
        db.insert_to_database(eng, delta_papers, 'dim_paper')

    elif process_step== 'Paragraph ETL':
        paragraphs_in_dwh=db.load_full_table(eng, 'dim_paragraph')
        source_paragraphs=para.extract_unique_paragraphs_from_file()
        transformed_paragraphs=para.transform_paragraphs(source_paragraphs, eng)
        delta_paragraphs=para.find_delta_paragraphs(transformed_paragraphs, paragraphs_in_dwh)
        db.insert_to_database(eng, delta_paragraphs, 'dim_paragraph')

    elif process_step == 'Sentence ETL':
        sentences_in_dwh=db.load_full_table(eng, 'dim_sentence')
        source_sentences=sent.extract_sentences_from_files()
        transformed_sentences=sent.transform_sentences(source_sentences, eng)
        delta_citationgroup, delta_sentence_citation_bridge, delta_sentences=sent.find_delta_sentences(transformed_sentences, sentences_in_dwh)
        db.insert_to_database(eng, delta_citationgroup, 'dim_citationgroup')
        db.insert_to_database(eng, delta_sentence_citation_bridge, 'bridge_sentence_citation')
        db.insert_to_database(eng, delta_sentences, 'dim_sentence')

    else:
        pass


