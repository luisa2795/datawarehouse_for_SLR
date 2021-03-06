import etl.common_functions as cof
import etl.database as db
import pandas as pd

def extract_unique_facts_from_file():
    """Extracts facts about entity detections in a sentence from the source file entities.csv.
    
    Returns: 
        DataFrame of entities without duplicates.
    """
    source_facts=cof.load_sourcefile('entities.csv')
    #introduce fact measure 'entity count' so that duplicates are captured (one sentence can contain the same entitiy more than once)
    source_facts=source_facts.groupby(['sentence_id', 'ent_id']).size().reset_index().rename(columns={0:'entity_count', 'entity': 'entity_instance'})
    return source_facts

def transform_delta_facts(source_facts, facts_in_dwh, engine):
    """Exchanges entity and sentence for their foreign keys and finds delta of facts in the source file vs those in the DB.
    
    Args:
        source_facts (DataFrame): df of source entities.
        facts_in_dwh (DataFrame): df of facts currently present in the DB table fact_entity_detection.
        engine (SQLAlchemy engine): engine object to connect to the target DB.
    Returns:
        DataFrame of transformed delta rows of facts, ready to load into fact_entity_detection table.
    """
    #first get sentence_pk and entity_pk and substitute the names in the source facts with it
    dim_sentence=db.load_full_table(engine, 'dim_sentence')
    dim_entity=db.load_full_table(engine, 'dim_entity')
    source_facts=pd.merge(source_facts, dim_sentence, how='left', left_on='sentence_id', right_on='sentence_source_id')[['ent_id', 'sentence_pk', 'entity_count']]
    source_facts=pd.merge(source_facts, dim_entity, how='left', left_on='ent_id', right_on='entity_name')[['entity_pk', 'sentence_pk', 'entity_count']]
    #find delta of facts in dwh vs source facts
    outer=pd.merge(source_facts, facts_in_dwh, how='outer')
    delta_facts=pd.concat([outer,facts_in_dwh]).drop_duplicates(keep=False)
    delta_facts.dropna(axis=0, how='any', inplace=True)
    return delta_facts


