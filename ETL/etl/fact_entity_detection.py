import etl.common_functions as cof
import etl.database as db
import pandas as pd

def extract_unique_facts_from_file():
    source_facts=cof.load_sourcefile('entities.csv')
    #as some sentences have duplicate entities, we drop these now. TODO: introduce fact measure 'entity count'?
    source_facts.drop_duplicates(inplace=True)
    return source_facts

def transform_delta_facts(source_facts, facts_in_dwh, engine):
    #first get sentence_pk and entity_pk and substitute the names in the source facts with it
    dim_sentence=db.load_full_table(engine, 'dim_sentence')
    dim_entity=db.load_full_table(engine, 'dim_entity')
    source_facts=pd.merge(source_facts, dim_sentence, how='left', left_on='sentence_id', right_on='sentence_source_id')[['ent_id', 'sentence_pk']]
    source_facts=pd.merge(source_facts, dim_entity, how='left', left_on='ent_id', right_on='entity_name')[['entity_pk', 'sentence_pk']]
    #find delta of facts in dwh vs source facts
    outer=pd.merge(source_facts, facts_in_dwh, how='outer')
    delta_facts=pd.concat([outer,facts_in_dwh]).drop_duplicates(keep=False)
    delta_facts.dropna(axis=0, how='any', inplace=True)
    return delta_facts


