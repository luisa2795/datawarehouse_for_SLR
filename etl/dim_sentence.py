import etl.common_functions as cof
import etl.database as db 
import pandas as pd

def extract_sentences_from_files():
    """Extracts sentences as df from the source file sentences.csv.
    Empty sentences or those that contain no content related meaning, like tags, tables and headers are dropped.
    
    Returns:
        DataFrame of meaningful sentences from the source file.
    """
    source_sentences=cof.load_sourcefile('sentences.csv')[['sentence_id', 'para_id', 'sentence', 'sentence_type']]
    source_sentences.dropna(axis=0, subset=['sentence'], inplace=True)
    #drop sentences that only contain of whitespaces
    source_sentences=source_sentences[~source_sentences.sentence.str.isspace()]
    #drop entries that have most likely no meaningful entities attached to them
    source_sentences=source_sentences[source_sentences.sentence_type.apply(lambda s: False if s in ['TAG', 'TABLE', 'EMPTY', 'FORMULA', 'TABLE_HEADER', 'FIGURE_HEADER', 'FIGURE', 'HYP_NUMBER', 'RQ_NUMBER'] else True)]
    return source_sentences

def transform_sentences(source_sentences, engine):
    """Transforms sentences to contain a paper_pk that points to the paper that is eventually referenced in that sentence and a paragraph_pk of the containing paragraph.
    
    Args:
        source_sentences (DataFrame): df of sentences from the souce file.
        engine (SQLAlchemy engine): engine object to connect to the target DB.
    
    Returns:
        Dataframe of sentences with paragraph_pk and citation paper_pk.
    """
    #load foreign keys from citations papers
    citations=cof.load_sourcefile('citations.csv')[['sentence_id', 'reference_citekey']]
    papers_in_dwh=db.load_full_table(engine, 'dim_paper')[['citekey', 'paper_pk']]
    citations_with_pk=pd.merge(citations, papers_in_dwh, how='left', left_on='reference_citekey', right_on='citekey')[['sentence_id', 'paper_pk']]
    sentences_with_reference_pk=pd.merge(source_sentences, citations_with_pk, how='left', on='sentence_id')
    #get paragraph_pk as foreign key
    paragraphs_in_dwh=db.load_full_table(engine, 'dim_paragraph')[['paragraph_pk', 'para_source_id']]
    sentences_with_para_pk=pd.merge(sentences_with_reference_pk, paragraphs_in_dwh, how='left', left_on='para_id', right_on='para_source_id').drop(columns=['para_id', 'para_source_id'])
    #add some strategies for missing values
    sentences_with_para_pk.fillna({'sentence_id': '0', 'sentence': 'MISSING', 'sentence_type': 'MISSING', 'paper_pk': 0, 'paragraph_pk': 0}, axis=0, inplace=True)
    return sentences_with_para_pk

def find_delta_sentences(transformed_sentences, sentences_in_dwh):
    """Finds delta of sentences in source file and those present in the DB table dim_sentence. For the delta rows, a citationgroup_pk is added.
    
    Args:
        transformed_sentences (DataFrame): transformed source sentences.
        sentences_in_dwh (DataFrame): sentences currently present in the DB table dim_sentence.
    Returns: 
        DataFrame of delta citationgroups, ready to be inserted into dim_citationgroup.
        DataFrame of delta sentence_citation combinations, ready to be inserted into bridge_sentence_citation.
        DataFrame of delta sentences, ready to be inserted into dim_sentence.
    """
    #get maximum primary key currently in db and group pk for citations
    max_pk=max(sentences_in_dwh.sentence_pk, default=0)
    max_citationgroup_pk=max(sentences_in_dwh.citationgroup_pk, default=0)
    #find subset of entries not yet present in dwh
    delta_sentences=transformed_sentences[transformed_sentences.sentence_id.apply(lambda i: False if  i in sentences_in_dwh.sentence_source_id.to_list() else True)]
    #assign citationgroup_pk
    delta_sentences['citationgroup_pk']=delta_sentences.groupby(by='sentence_id').ngroup(ascending=True)+max_citationgroup_pk+1
    #separate citation_paper_bridge and dim_citationgroup
    delta_bridge_sentence_citation=delta_sentences[['citationgroup_pk', 'paper_pk']].drop_duplicates()
    delta_citationgroup=pd.DataFrame(delta_sentences['citationgroup_pk']).drop_duplicates()
    #now drop unnecessary columns, remove then the duplicated sentence rows and rename columns so they fit to the db table
    delta_sentences=delta_sentences.drop(columns=['paper_pk']).drop_duplicates().rename({'sentence_id': 'sentence_source_id', 'sentence': 'sentence_string'}, axis=1)
    #add primary_key
    delta_sentences['sentence_pk']=list(range(max_pk+1, max_pk+1+delta_sentences.index.size))
     #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
    if max_pk==0:
        dummy_sent={'sentence_pk': 0, 'sentence_source_id': '0', 'sentence_string': 'MISSING', 'sentence_type': 'MISSING', 'citationgroup_pk': 0, 'paragraph_pk': 0}
        delta_sentences=pd.concat([delta_sentences, pd.DataFrame([dummy_sent])], ignore_index=True)
    if max_citationgroup_pk==0:
        delta_bridge_sentence_citation=pd.concat([delta_bridge_sentence_citation, pd.DataFrame([{'citationgroup_pk': 0, 'paper_pk': 0}])], ignore_index=True)
        delta_citationgroup=pd.concat([delta_citationgroup, pd.DataFrame([{'citationgroup_pk': 0}])], ignore_index=True)
    return delta_citationgroup, delta_bridge_sentence_citation, delta_sentences


