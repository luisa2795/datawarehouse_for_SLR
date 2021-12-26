import etl.common_functions as cof
import pandas as pd

def extract_entities_from_file():
    """Extracts entities from sourcefiles entities.csv and removes duplicates.
    
    Returns:
        DataFrame with source entities label, id and path.
    """
    for_map_and_dim=cof.load_sourcefile('entities.csv')[['label', 'ent_id', 'ent_path']]
    for_map_and_dim.drop_duplicates(inplace=True)
    return for_map_and_dim

def transform_delta_entities(source_entities, entities_in_dwh):
    """Finds delta between entities in the DB table and source entities and transforms the entities not yet present in DB.
    
    Args:
        source_entities (DataFrame): df of entities fro source file with the columns ent_id, label and path.
        entities_in_dwh (DataFrame): rows of DB table dim_entity as pandas dataframe.
        
    Returns:
        delta_dim_entity (DataFrame): ready to insert df with the new rows for the dim_entity table.
        delta_entity (DataFrame): new entity rows but with the entity path for further transformation so it can used to extend the hierarchy map.
    """
    max_pk=max(entities_in_dwh.entity_pk, default=0)
    #first generate delta of dim_entity
    source_entities=source_entities.rename(columns={'ent_id': 'entity_name', 'label': 'entity_label'})
    outer=pd.merge(source_entities, entities_in_dwh, how='outer')[['entity_name', 'entity_label', 'ent_path']]
    delta_entity=pd.concat([outer, entities_in_dwh]).drop_duplicates(keep=False)
    #use only name and label for dim_entity and add consecutive primary key
    delta_dim_entity=delta_entity[['entity_name', 'entity_label']].drop_duplicates()
    delta_dim_entity['entity_pk']=list(range(max_pk+1, max_pk+1+delta_dim_entity.index.size))
    return delta_dim_entity, delta_entity

def transform_delta_entity_hierarchy_map(delta_entities, entitites_in_dwh):
    """Extracts all hierarchies inside the delta paths into the parent-child format as defined in the logical modeling and as recommended by Kimball for hierarchies of variable depth.
    
    Args:
        delta_entities (DataFrame): new entity rows with hierarchy paths.
        entities_in_dwh (DataFrame): the entities present in the dim_entity table, needed to retrieve primary keys.
        
    Returns: 
        DataFrame with new rows for map_entity_hierarchy table.
    """
    hierarchies=[]
    for path in delta_entities.ent_path:
        if path==path:
            path_as_list=path.split('/')
            for idx1, parent_ent in enumerate(path_as_list):
                for idx2, child_ent in enumerate(path_as_list):
                    if idx2-idx1 >-1:
                        if idx1==0:
                            highest_parent_flag=True
                        else:
                            highest_parent_flag=False
                        parent_child_relations=[parent_ent, child_ent, (idx2-idx1), highest_parent_flag]
                        hierarchies.append(parent_child_relations)
    map_entity_hierarchy=pd.DataFrame(hierarchies, columns=['parent', 'child', 'depth_from_parent', 'highest_parent_flag']).drop_duplicates()
    #get list of leaf nodes (those that have no child entities but themselves)
    lowest_children=map_entity_hierarchy.parent.value_counts().loc[lambda x: x==1].index.to_list()
    #assign lowest child flag based on this list
    map_entity_hierarchy['lowest_child_flag']=map_entity_hierarchy['child'].apply(lambda c: True if c in lowest_children else False)
    #get pk for each parent and each child entity
    map_entity_hierarchy=pd.merge(map_entity_hierarchy, entitites_in_dwh, how='left', left_on='parent', right_on='entity_name').drop(columns=['parent', 'entity_label', 'entity_name']).rename(columns={'entity_pk': 'parent_entity_pk'})
    map_entity_hierarchy=pd.merge(map_entity_hierarchy, entitites_in_dwh, how='left', left_on='child', right_on='entity_name').drop(columns=['child', 'entity_label', 'entity_name']).rename(columns={'entity_pk': 'child_entity_pk'})
    #drop rows with missing values
    map_entity_hierarchy.dropna(axis=0, how='any', inplace=True)
    #TODO: here we should check again to only return the delta of what is not yet in the hierarchy table!
    return map_entity_hierarchy
        
