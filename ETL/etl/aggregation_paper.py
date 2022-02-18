import etl.database as db
import pandas as pd
import re
import etl.common_functions as cof

def extract_source_data(engine):
    """Extracts source data about papers and sentences with entities from the data warehouse.
    
    Args:
        engine (SQL Alchemy engine): engine to connect to the target DB.

    Returns:
        DataFrame of sentences, paragraph headings and entities that were detected in these sentences.
        DataFrame of the paper dimension in the data warehouse.
    """
    sql_query='select paper_pk, heading, paragraph_type,  sentence_string, sentence_type, entity_count, entity_label, entity_name from (select sentence_string, sentence_type, paragraph_pk, entity_count, entity_label, entity_name from (select sentence_pk, entity_count, entity_label, entity_name from fact_entity_detection fed inner join dim_entity de on fed.entity_pk = de.entity_pk) as fact_ent_join left join dim_sentence ds on fact_ent_join.sentence_pk=ds.sentence_pk) as sent_ent_join left join dim_paragraph dp on sent_ent_join.paragraph_pk=dp.paragraph_pk'
    sentences_with_ents=db.load_df_from_query(engine, sql_query)
    papers_in_dwh=db.load_full_table(engine, 'dim_paper')
    return sentences_with_ents, papers_in_dwh

def calc_agg_columns(sentences_with_ents, papers_in_dwh):
    """Adds an aggregation column for each entity category to the paper DataFrame, plus two numeric columns (participant number and metric value). 
    The values of the new columns are aggregated by different strategies, chosen after the most likely approach to select the most relevant entity for a paper.
    
    Args:
        sentences_with_ents (DataFrame): Df of sentences, paragraph headings and entities that were detected in these sentences.
        papers_in_dwh (DataFrame): Df of the paper dimension in the data warehouse.

    Returns: 
        DataFrame of papers with aggregated entities.
    """
    #model_element
    model_element=sentences_with_ents[sentences_with_ents.entity_label=='MODEL_ELEMENT']
    me=model_element.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.mode()[0]).reset_index()
    papers_me=pd.merge(papers_in_dwh, me, how='left', on='paper_pk').rename(columns={'entity_name': 'model_element'}).fillna('MISSING')
    #level
    level=sentences_with_ents[sentences_with_ents.entity_label=='LEVEL']
    level=level.apply(lambda r: weigh_entity_by_sentence_type(['ABSTRACT'], r), axis=1)
    le=level.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_le=pd.merge(papers_me, le, how='left', on='paper_pk').rename(columns={'entity_name': 'level'}).fillna('MISSING')
    #participants
    participants=sentences_with_ents[sentences_with_ents.entity_label=='PARTICIPANTS']
    participants_w=participants.apply(lambda r: weigh_entity_by_heading(heading_pattern=".*data.*|.*participa.*|.*sample.*|.*method.*", row=r, weigh_sentences=True), axis=1)
    pa=participants_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_pa=pd.merge(papers_le, pa, how='left', on='paper_pk').rename(columns={'entity_name': 'participants'}).fillna('MISSING')
    #no_of_participants
    ents_agg_pa=pd.merge(participants_w[['paper_pk', 'sentence_string', 'entity_name']], papers_pa[['paper_pk', 'participants']], how='left', on='paper_pk')
    cite_sep='START_CITE .* END_CITE' 
    ents_agg_pa['participant_number']=ents_agg_pa.apply(lambda row: list(filter(None, [cof.word_to_int(word) for word in [item for sublist in [re.split(cite_sep, sent)[0].split() for sent in row['sentence_string']] for item in sublist]])) if row['entity_name'][0]==row['participants'] else [], axis=1)
    nop=ents_agg_pa.groupby(by='paper_pk')[['participant_number']].agg(lambda x: x.explode().mode()).reset_index()
    nop['no_of_participants']=nop.participant_number.apply(lambda x: x if isinstance(x, int) else(x[0] if len(x)!=0 else 0)) 
    papers_nop=pd.merge(papers_pa, nop[['paper_pk', 'no_of_participants']], how='left', on='paper_pk').fillna(0)
    #collection_method
    collection_method=sentences_with_ents[sentences_with_ents.entity_label=='COLLECTION_METHOD']
    collection_method_w=collection_method.apply(lambda r: weigh_entity_by_heading(heading_pattern=".*data.*|.*collect.*|.*method.*|.*abstract.*|.*abstract.*|.*approach.*|.*procedure.*|.*design.*|.*experiment.*", row=r, exclude_entities=['data collection method']), axis=1)
    cm=collection_method_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_cm=pd.merge(papers_nop, cm, how='left', on='paper_pk').rename(columns={'entity_name': 'collection_method'}).fillna('MISSING')
    #sampling
    sampling=sentences_with_ents[sentences_with_ents.entity_label=='SAMPLING']
    sampling_w=sampling.apply(lambda r: weigh_entity_by_heading(heading_pattern=".*data.*|.*collect.*|.*method.*|.*abstract.*|.*sampling.*|.*sample.*|.*design.*", row=r, exclude_entities=['sampling']), axis=1)
    sa=sampling_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_sa=pd.merge(papers_cm, sa, how='left', on='paper_pk').rename(columns={'entity_name': 'sampling'}).fillna('MISSING')
    #analysis_method
    analysis_method=sentences_with_ents[sentences_with_ents.entity_label=='ANALYSIS_METHOD']
    analysis_method_w=analysis_method.apply(lambda r: weigh_entity_by_heading(heading_pattern=".*result.*|.*method.*|.*abstract.*|.*analys.*|.*discuss.*|.*conclusion.*", row=r), axis=1)
    am=analysis_method_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_am=pd.merge(papers_sa, am, how='left', on='paper_pk').rename(columns={'entity_name': 'analysis_method'}).fillna('MISSING')
    #sector
    sector=sentences_with_ents[sentences_with_ents.entity_label=='SECTOR']
    sector_w=sector.apply(lambda r: weigh_entity_by_sentence_type(['ABSTRACT'], r), axis=1)
    se=sector_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_se=pd.merge(papers_am, se, how='left', on='paper_pk').rename(columns={'entity_name': 'sector'}).fillna('MISSING')
    #region
    region=sentences_with_ents[sentences_with_ents.entity_label=='REGION']
    region_w=region.apply(lambda r: weigh_entity_by_sentence_type(['ABSTRACT'], r), axis=1)
    reg=region_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_reg=pd.merge(papers_se, reg, how='left', on='paper_pk').rename(columns={'entity_name': 'region'}).fillna('MISSING')
    #metric
    metric=sentences_with_ents[sentences_with_ents.entity_label=='METRIC']
    metric_w=metric.apply(lambda r: weigh_entity_by_heading(heading_pattern=".*result.*|.*method.*|.*abstract.*|.*analys.*|.*discuss.*|.*conclusion.*|.*test.*|.*metric.*", row=r), axis=1)
    me=metric_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_me=pd.merge(papers_reg, me, how='left', on='paper_pk').rename(columns={'entity_name': 'metric'}).fillna('MISSING')
    #metric_value
    ents_agg_me=pd.merge(metric_w[['paper_pk', 'sentence_string', 'entity_name']], papers_me[['paper_pk', 'metric']], how='left', on='paper_pk')
    ents_agg_me['all_values']=ents_agg_me.apply(lambda row: list(filter(None, [cof.word_to_float(word) for word in [item for sublist in [re.split(cite_sep, sent)[0].split() for sent in row['sentence_string']] for item in sublist]])) if row['entity_name'][0]==row['metric'] else [], axis=1)
    mv=ents_agg_me.groupby(by='paper_pk')[['all_values']].agg(lambda x: x.explode().mode()).reset_index()
    mv['metric_value']=mv.all_values.apply(lambda x: x if isinstance(x, float) else(x[0] if len(x)!=0 else 0)) 
    papers_mv=pd.merge(papers_me, mv[['paper_pk', 'metric_value']], how='left', on='paper_pk').fillna(0)
    #conceptual_method
    conceptual_method=sentences_with_ents[sentences_with_ents.entity_label=='CONCEPTUAL_METHOD']
    conceptual_method_w=conceptual_method.apply(lambda r: weigh_entity_by_heading(heading_pattern=".*result.*|.*method.*|.*abstract.*|.*analys.*|.*discuss.*|.*conclusion.*", row=r), axis=1)
    cm=conceptual_method_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_cm=pd.merge(papers_mv, cm, how='left', on='paper_pk').rename(columns={'entity_name': 'conceptual_method'}).fillna('MISSING')
    #topic
    topic=sentences_with_ents[sentences_with_ents.entity_label=='TOPIC']
    to=topic.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.mode()[0]).reset_index()
    papers_to=pd.merge(papers_cm, to, how='left', on='paper_pk').rename(columns={'entity_name': 'topic'}).fillna('MISSING')
    #technology
    technology=sentences_with_ents[sentences_with_ents.entity_label=='TECHNOLOGY']
    te=technology.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.mode()[0]).reset_index()
    papers_te=pd.merge(papers_to, te, how='left', on='paper_pk').rename(columns={'entity_name': 'technology'}).fillna('MISSING')
    #theory
    theory=sentences_with_ents[sentences_with_ents.entity_label=='THEORY']
    theory_w=theory.apply(lambda r: weigh_entity_by_heading(heading_pattern="    ", row=r, exclude_entities=['theory']), axis=1)
    th=theory_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_th=pd.merge(papers_te, th, how='left', on='paper_pk').rename(columns={'entity_name': 'theory'}).fillna('MISSING')
    #paradigm
    paradigm=sentences_with_ents[sentences_with_ents.entity_label=='PARADIGM']
    para=paradigm.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.mode()[0]).reset_index()
    papers_para=pd.merge(papers_th, para, how='left', on='paper_pk').rename(columns={'entity_name': 'paradigm'}).fillna('MISSING')
    #company_type
    company_type=sentences_with_ents[sentences_with_ents.entity_label=='COMPANY_TYPE']
    ct=company_type.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.mode()[0]).reset_index()
    papers_ct=pd.merge(papers_para, ct, how='left', on='paper_pk').rename(columns={'entity_name': 'company_type'}).fillna('MISSING')
    #validity
    validity=sentences_with_ents[sentences_with_ents.entity_label=='VALIDITY']
    validity_w=validity.apply(lambda r: weigh_entity_by_heading(heading_pattern=".*result.*|.*method.*|.*measure.*|.*valid.*|.*discuss.*|.*conclusion.*", row=r, exclude_entities=['validity']), axis=1)
    va=validity_w.groupby(by='paper_pk')[['entity_name']].agg(lambda x: x.explode().mode()[0]).reset_index()
    papers_va=pd.merge(papers_ct, va, how='left', on='paper_pk').rename(columns={'entity_name': 'validity'}).fillna('MISSING')
    return papers_va


def weigh_entity_by_sentence_type (increased_weights_types, row):
    """Increases the weight of an entity to 150%, if it occurs in a sentence of a given type.
    
    Args:
        increased_weights_types (list): List of sentence types which have an increased influence on an entity weight in the aggreation.
        row (df row): DataFrame row containing the values for sentence_type and entity_name.
    
    Returns:
        Row with increased weights. If the entity occured in a sentence of the increased importance, it is repeated 15 times, otherwise it is repeated 10 times.
    """
    if row['sentence_type'] in increased_weights_types:
        row['entity_name']=[row['entity_name']] * 15  * row['entity_count']
    else:
        row['entity_name']=[row['entity_name']] * 10 * row['entity_count']
    return row

def weigh_entity_by_heading (heading_pattern, row, exclude_entities=[], weigh_sentences=False):
    """Increases the weight of an entity to 150%, if it occurs in a sentence from a paragraph with a specific heading pattern.
    Entity names can be specified to be excluded, which would result in a decrease of their weight to 10%. 
    The sentence string can be weighed by the same logic if required.
    
    Args:
        heading_pattern (list): List of regex patterns which would indicated a heading that increases an entity's weight in the aggreation.
        row (df row): DataFrame row containing the values for sentence_string, heading and entity_name.
        exclude_entities (list): List of entities whose weight shall be decreased to 10% in any case.
        weigh_sentences (bool): Boolean indicating whether to weigh sentences as well (True) or not (False).
    
    Returns:
        Row with increased weights. If the entity occured under a heading of increased importance, it is repeated 15 times, otherwise it is repeated 10 times. 
        If it was within the list of entities to be excluded, it is not repeated at all. Sentences are weighed accordingly if weigh_sentences was True.
    """
    if row['entity_name'] in exclude_entities:
        row['entity_name']=[row['entity_name']]
        if weigh_sentences:
            row['sentence_string']=[row['sentence_string']] * row['entity_count']
    elif bool(re.match(heading_pattern, row['heading'], re.IGNORECASE)):
        row['entity_name']=[row['entity_name']] * 15 * row['entity_count']
        if weigh_sentences:
            row['sentence_string']=[row['sentence_string']] * 15 * row['entity_count']
    else:
        row['entity_name']=[row['entity_name']] * 10 * row['entity_count']
        if weigh_sentences:
            row['sentence_string']=[row['sentence_string']] * 10 * row['entity_count']
    return row