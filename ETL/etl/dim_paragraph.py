import pandas as pd
import etl.common_functions as cof
import etl.database as db

def extract_unique_paragraphs_from_file():
    source_para=cof.load_sourcefile('paragraphs.csv')[['para_id', 'article_id','last_section_title', 'last_subsection_title', 'paragraph_type']]
    return source_para

def transform_paragraphs(source_paragraphs, engine):
    papers_in_dwh=db.load_full_table(engine, 'dim_paper')[['paper_pk', 'article_source_id']]
    transformed_para=pd.merge(source_paragraphs, papers_in_dwh, how='left', left_on='article_id', right_on='article_source_id').drop(columns=['article_source_id', 'article_id'])
    transformed_para.fillna({'last_section_title': 'MISSING', 'last_subsection_title': 'MISSING', 'paragraph_type': 'MISSING', 'paper_pk': 0}, axis=0, inplace=True)
    return transformed_para

def find_delta_paragraphs(source_para_trans, para_in_dwh):
    #determine which paragraphs have not yet been inserted into table
    source_para_trans.rename(columns={'para_id': 'para_source_id', 'last_section_title':'heading', 'last_subsection_title': 'subheading'}, inplace=True)
    outer=pd.merge(source_para_trans, para_in_dwh, how='outer')[['para_source_id', 'heading', 'subheading', 'paragraph_type', 'paper_pk']]
    delta_para=pd.concat([outer,para_in_dwh]).drop_duplicates(keep=False)
    #add a consecutive key, starting from max_pk +1
    max_pk=max(para_in_dwh.paragraph_pk, default=0)
    delta_para['paragraph_pk']=list(range(max_pk+1, max_pk+1+delta_para.index.size))
    #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
    if max_pk==0:
        delta_para=delta_para.append({'paragraph_pk': 0, 'para_source_id': '0', 'heading': 'MISSING', 'subheading': 'MISSING', 'paragraph_type': 'MISSING', 'paper_pk': 0}, ignore_index=True)
    return delta_para
