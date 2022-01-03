import pandas as pd
import etl.common_functions as cof
import etl.dim_author as auth
import etl.dim_journal as jour
import etl.database as db
import roman

    
def extract_all_papers():
    """Loads data from papers_final.csv and unique_references.csv sourcefiles.
    
    Returns:
        DataFrame of papers from papers_final sourcefile.
        DataFrame of papers from unique_references sourcefile.
    """
    from_papers=cof.load_sourcefile('papers_final.csv')
    from_references=cof.load_sourcefile('unique_references.csv')
    return from_papers, from_references

def transform_papers(source_papers, engine):
    """Transforms papers from papers_final source: triggers the addition of keyword_pk, author_pk and journal_pk.
    
    Args: 
        source_papers (DataFrame): df of source file from papers_final.
        engine (SQL Alchemy engine): engine to connect to the target DB.
    
    Returns: 
        DataFrame of prepared papers. The group primary keys are not created yet, so papers with e.g. multiple keywords are listed in multiple rows, each with different keyword_pk.
    """
    keywords_df=cof.load_sourcefile('keywords.csv')
    articles_prep=_join_articles_keyword_pk(source_papers, keywords_df, engine)
    #join articles with authors and lookup existing foreign key author_pk
    authors_df=cof.load_sourcefile('authors.csv').rename(columns={'departments': 'department', 'institutions': 'institution', 'countries': 'country'})
    articles_prep=_prepare_article_authors(authors_df, articles_prep)
    articles_prep=_join_papers_author_pk(articles_prep, engine).drop(columns=['surname', 'firstname', 'middlename','email_x', 'department_x', 'institution_x', 'country_x', 'email_y', 'department_y', 'institution_y', 'country_y'], axis=1)
    #join articles with journals and lookup existing foreign key journal_pk
    articles_prep=_prepare_paper_journals(articles_prep)
    articles_prep=_join_papers_journal_pk(articles_prep, engine).drop(columns=['journal_akronym'], axis=1)
    return articles_prep

def transform_references(source_references, engine):
    """Transforms papers from unique_references source: triggers the addition of author_pk and journal_pk.
    As keywords are not present in the source data, the dummy keyword_pk of 0 is added to each reference which will point to MISSING keywords.
    
    Args: 
        source_references (DataFrame):
        engine (SQL Alchemy engine): engine to connect to target DB.
    
    Returns:
        DataFrame of prepared references. As in transform_papers() the group primary keys are not created yet, which is why for each author, a paper has a row in the returned df. 
    """
    # create 'keyword_pk'=0 for papers from references with reference to dummy entry in dim_keyword
    references_prep=source_references.assign(keyword_pk=0)
    #join references with authors and lookup existing foreign key author_pk
    references_prep=_prepare_reference_authors(references_prep)
    references_prep=_join_papers_author_pk(references_prep, engine).drop(columns=['authors', 'surname', 'firstname', 'middlename', 'email', 'department', 'institution', 'country'], axis=1)
    #join articles with journals and lookup existing foreign key journal_pk
    references_prep=_prepare_paper_journals(references_prep)
    references_prep=_join_papers_journal_pk(references_prep, engine).drop(columns=['source_type', 'editor', 'monograph_title', 'note'], axis=1)
    return references_prep

def merge_all_papers(prepared_references, prepared_papers):
    """Merges prepared references and prepared papers to one df.
    
    Args:
        prepared_references (DataFrame): The references df resulting from transform references() with primary keys added.
        prepared_papers (DataFrame): The papers df resulting from transform_papers() with primary keys added.
    
    Returns:
        DataFrame of merged papers with page numbers calculated and filled missing values.
    """
    #split start and end of pages from references into two columns, then transform the page numbers to integers
    prepared_references.pages=prepared_references.pages.apply(lambda x: x.split('-') if x==x else [0, 0])
    prepared_references[['pages_start', 'pages_end']]=pd.DataFrame(prepared_references.pages.tolist(), index=prepared_references.index)
    prepared_references.pages_start=prepared_references.pages_start.apply(lambda p: _page_number_to_int(p))
    prepared_references.pages_end=prepared_references.pages_end.apply(lambda p: _page_number_to_int(p))
    #calculate the difference between start and end as number of pages
    prepared_references['number_of_pages']=prepared_references.pages_end-prepared_references.pages_start

    #merge papers from articles and from references
    all_papers=pd.merge(prepared_papers, prepared_references, how='outer', on='citekey', suffixes=['_art', '_ref'])
    all_papers['year']=all_papers.apply(lambda x: x.year_art if x.year_art==x.year_art else x.year_ref, axis=1)
    all_papers['year']=all_papers.year.apply(lambda y: pd.to_datetime(int(y), format='%Y').normalize() if 1676<y<2263 else pd.to_datetime(1678, format='%Y').normalize()) 
    all_papers['title']=all_papers.apply(lambda x: x.title_art if x.title_art==x.title_art else x.title_ref, axis=1)
    all_papers['author_pk']=all_papers.apply(lambda x: x.author_pk_art if x.author_pk_art==x.author_pk_art else x.author_pk_ref, axis=1)
    all_papers['no_of_pages']=all_papers.apply(lambda x: x.number_of_pages_art if x.number_of_pages_art==x.number_of_pages_art else x.number_of_pages_ref, axis=1)
    all_papers['no_of_pages']=all_papers.no_of_pages.apply(lambda x: x if 0<x<2000000000 else 0)
    all_papers['journal_pk']=all_papers.apply(lambda x: x.journal_pk_art if x.journal_pk_art==x.journal_pk_art else x.journal_pk_ref, axis=1)
    all_papers['keyword_pk']=all_papers['keyword_pk_art']
    all_papers.fillna({'article_id': 0, 'author_position': 0, 'citekey': 'MISSING', 'abstract': 'MISSING', 'year': pd.to_datetime(1678, format='%Y').normalize(), 'title': 'MISSING', 'author_pk': 0, 'no_of_pages': 0, 'journal_pk': 0,'keyword_pk': 0}, inplace=True)

    final_papers=all_papers[['article_id', 'author_position', 'citekey', 'abstract', 'year', 'title', 'author_pk', 'no_of_pages', 'journal_pk', 'keyword_pk']]
    return final_papers

def find_delta_papers(source_papers, papers_in_dwh):
    """Compares merged source papers with data in the DB and finds delta of rows. 
    For this delta_df, a primary key, authorgroup_pk and keywordgroup_pk are added, bridge tables and separate group dimensions are created. 
    
    Args:
        source_papers (DataFrame): The transformed and merged source papers.
        papers_in_dwh (DataFrame): The data currently present in the DB table dim_paper as pandas df.
    Returns:  
        DataFrame of delta papers, ready to insert into dim_paper.
        DataFrame of delta keywordgroup, ready to insert into dim_keywordgroup.
        DataFrame of delta rows ready to insert into bridge_paper_keyword.
        DataFrame of delta authorgroup, ready to insert into dim_authorgroup.
        DataFrame of delta rows ready to insert into bridge_paper_author.
    """
    source_papers=source_papers.rename(columns={'article_id': 'article_source_id'})
    outer=pd.merge(source_papers, papers_in_dwh, how='outer')[['article_source_id', 'author_position', 'citekey', 'abstract', 'year', 'title', 'author_pk', 'no_of_pages', 'journal_pk', 'keyword_pk']]
    delta_papers=pd.concat([outer,papers_in_dwh]).drop_duplicates(keep=False)
    
    #assign group_index for later authorgroup and keywordgroup, starting from 0
    delta_papers['group_index']=delta_papers.groupby(by='citekey').ngroup(ascending=True)
    #which is the highest group_index we have currently in the dwh?
    max_group_pk=max(papers_in_dwh.keywordgroup_pk, default=0)
    delta_papers['keywordgroup_pk']=delta_papers['group_index']+max_group_pk+1
    delta_papers['authorgroup_pk']=delta_papers['group_index']+max_group_pk+1
    delta_keywordbridge=delta_papers[['keywordgroup_pk', 'keyword_pk']].drop_duplicates()
    delta_keywordgroup=pd.DataFrame(delta_keywordbridge['keywordgroup_pk']).drop_duplicates()
    delta_authorbridge=delta_papers[['authorgroup_pk', 'author_pk', 'author_position']].drop_duplicates(subset=['authorgroup_pk', 'author_pk'], keep='first')
    delta_authorgroup=pd.DataFrame(delta_authorbridge['authorgroup_pk']).drop_duplicates(subset=['authorgroup_pk'], keep='first')
    #remove now not needed columns from paper df and drop duplicate rows now
    delta_papers=delta_papers.drop(columns=['author_position', 'author_pk', 'keyword_pk', 'group_index'], axis=1).drop_duplicates()
    #add a consecutive key, starting from max_pk +1
    max_pk=max(papers_in_dwh.paper_pk, default=0)
    delta_papers['paper_pk']=list(range(max_pk+1, max_pk+1+delta_papers.index.size))

    #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
    if max_pk==0:
        delta_papers=delta_papers.append({'paper_pk': 0, 'article_source_id': 0, 'citekey': 'MISSING', 'abstract': 'MISSING', 'year': pd.to_datetime(1678, format='%Y').normalize(), 'title': 'MISSING', 'authorgroup_pk': 0, 'no_of_pages': 0, 'journal_pk': 0,'keywordgroup_pk': 0}, ignore_index=True)
    if max_group_pk==0:
        delta_keywordbridge=delta_keywordbridge.append({'keywordgroup_pk': 0, 'keyword_pk': 0}, ignore_index=True)
        delta_keywordgroup=delta_keywordgroup.append({'keywordgroup_pk': 0}, ignore_index=True)
        delta_authorbridge=delta_authorbridge.append({'authorgroup_pk': 0, 'author_pk': 0, 'author_position': 0}, ignore_index=True)
        delta_authorgroup=delta_authorgroup.append({'authorgroup_pk': 0}, ignore_index=True)
    return delta_papers, delta_keywordgroup, delta_keywordbridge, delta_authorgroup, delta_authorbridge

def _join_articles_keyword_pk(articles_df, keywords_df, engine):
    """Joins papers with keywords so that a keyword_pk is added to each row.
    
    Args:
        articles_df (DataFrame): dataframe of the source file final_papers.
        keywords_df (DataFrame): dataframe of the source file keywords.csv.
        engine (SQLAlchemy engine): engine object to connect to the target DB.
    
    Returns: 
        DataFrame of papers with a keyword_pk instead of keyword itself.
    """
    #join with keywords and lookup exising foreign key 'keyword_pk'
    keywords_df["keyword"]=keywords_df["keyword"].str.lower()
    articles_prep=pd.merge(articles_df, keywords_df, how='outer', on='article_id')
    keywords_in_dwh = db.load_full_table(engine, 'dim_keyword')
    articles_prep=pd.merge(articles_prep, keywords_in_dwh, how='left', left_on='keyword', right_on='keyword_string')
    #insert dummy foreign key 0 if keyword is missing
    articles_prep.keyword_pk=articles_prep.keyword_pk.apply(lambda p: 0 if p!=p else int(p))
    articles_prep.drop(['keywords', 'keyword', 'keyword_string'], axis=1, inplace=True)
    return articles_prep

def _prepare_article_authors(authors_df, articles_df):
    """Prepares the author fullname column from the authors sourcefile as it is done during the transformations for the author dimension. 
    Then the transformed authors are merged to the paper file.
    
    Args: 
        authors_df (DataFrame): df from the source file authors.csv.
        articles_df (DataFrame): prepared df from the source file papers_final.

    Returns: 
        DataFrame of papers with columns for the authors first-, middle- and surname. If a paper has multiple authors it is listed miltiple times, each author gets one row.
    """
    #some cells in the source data still contain numbers, html tags or @ tags, these are removed
    authors_df.fullname=authors_df.fullname.apply(lambda f: auth._remove_numbers_tags_and_signs(f))
    #then split the fullname again into the columns first-, middle- and surname
    authors_df[['surname', 'firstname', 'middlename']]=pd.DataFrame(authors_df.fullname.apply(lambda fn: auth._split_fullname(fn)).to_list(), index=authors_df.index)
    article_authors=pd.merge(authors_df, articles_df, how='outer', on='article_id').drop(columns=['fullname', 'authors'])
    article_authors.fillna({'surname': 'MISSING', 'firstname': 'MISSING', 'middlename': 'MISSING'}, axis=0, inplace=True)
    return article_authors

def _join_papers_author_pk(articles_df, engine):
    """Exchanges author name for a foreign key to author in dim_author.
    
    Args: 
        articles_df (DataFrame): prepared df of papers, must contain columns surname, middlename and firstname.
        engine (SQLAlchemy engine): engine object to connect to the target DB.
    
    Returns:
        DataFrame of papers with author_pk.
    """
    authors_in_dwh=db.load_full_table(engine, 'dim_author')
    joined=pd.merge(articles_df, authors_in_dwh, how= 'left', on=['surname', 'firstname', 'middlename'])
    return joined

def _prepare_paper_journals(paper_df):
    """Prepares the journal-related columns in the papers df like it is done during the transformations for the journal dimension. 
    
    Args: 
        paper_df (DataFrame): df of papers, must contain the columns journal, volume, issue, publisher, place.

    Returns: 
        DataFrame of papers with transformed journal information.
    """
    paper_df.fillna({'journal': 'MISSING', 'volume':0, 'issue': 0, 'publisher': 'MISSING', 'place': 'MISSING'}, inplace=True)
    paper_df.volume=paper_df.volume.apply(lambda v: jour._volume_to_int(v))
    paper_df.issue=paper_df.issue.apply(lambda i: jour._issue_to_int(i))
    return paper_df

def _join_papers_journal_pk(paper_df, engine):
    """Exchanges journal information for a foreign key to journal in dim_journal.
    
    Args: 
        paper_df (DataFrame): prepared df of papers, must contain the transformed columns journal, volume, issue, publisher, place.
        engine (SQLAlchemy engine): engine object to connect to the target DB.
    
    Returns:
        DataFrame of papers with journal_pk.
    """
    journals_in_dwh=db.load_full_table(engine, 'dim_journal').rename({'title': 'journal'})
    joined=pd.merge(paper_df, journals_in_dwh, how='left', left_on=['journal', 'volume', 'issue', 'publisher', 'place'], right_on=['title', 'volume', 'issue', 'publisher', 'place'], suffixes=[None, '_db'])
    joined=joined.drop(columns=['journal', 'volume', 'issue', 'publisher', 'place', 'title_db'], axis=1)
    return joined

def _prepare_reference_authors(references_df):
    """Prepares the author column from the unique_references sourcefile so that each author is in a seperate row and has values for firstname, middlename and surname. 
    
    Args: 
        references_df (DataFrame): df from the source file unique_references.

    Returns: 
        DataFrame of references with columns for the authors first-, middle- and surname. If a paper has multiple authors it is listed miltiple times, each author gets one row.
    """
    references_df.authors=references_df['authors'].str.split('; ').explode(ignore_index=True).str.split(', ')
    references_df.dropna(axis=0, subset=['authors'], inplace=True)
    #remove the 'Van' if existing in strings that are longer tan 2, as checks have shown these are most probably parsing errors
    references_df.authors.apply(lambda l: (l.remove('Van') if 'Van' in l else l) if len(l)>2 else l)
    #keep only the rows with a length of 2 now:
    keep=references_df[references_df.authors.str.len()==2]
    #those that are longer must be changed
    change1=references_df[references_df.authors.str.len()>2]
    #first split every string in list from each other, then split into sublist pairs of two
    change1.authors=change1.authors.apply(lambda l:auth. _split_into_lists_of_two_strings(l)).explode()
    #and append now corrected series again to keep
    keep.append(change1)
    #those that are shorter must be changed
    change2=references_df[references_df.authors.str.len()<2]
    #those that are shorter than 2 letters will be missing
    change3=change2[change2.authors.explode().str.len()>2]
    missing=change2[change2.authors.explode().str.len()<2]
    #many rows contain names that are just not separated by comma and therefore not recognized. lets split them into sublist of two strings each
    change3.authors=change3.authors.apply(lambda l: auth._split_into_lists_of_two_strings(l))
    #unnest lists to new rows
    change3=change3.explode('authors')
    #keep only those that contain two strings, others will be missing
    change4=change3[change3.authors.str.len()==2]
    missing=change3[change3.authors.str.len()!=2]
    #and append the transformed rows to keep
    ref_prep=keep.append(change4)
    #split into firstname and surname
    ref_prep[['surname', 'firstname']]=pd.DataFrame(ref_prep.authors.tolist(), index=ref_prep.index)
    ref_prep.fillna({'surname': 'MISSING', 'firstname': 'MISSING'}, axis=0, inplace=True)
    #assign missing surname and firstname to missing and append it to final keep df
    missing=missing.assign(surname='MISSING', firstname='MISSING')
    ref_prep=ref_prep.append(missing)
    #insert column for missing middlename
    ref_prep=ref_prep.assign(middlename='MISSING')
    return ref_prep

def _page_number_to_int(page):
    """Function to convert a page number, which can be numeric, a roman number or a string, into an integer.
    
    Args:
        page (object): page information of one paper, as it is parsed from the sourcefile.
    
    Returns: 
        Page number of dtype integer.
    """
    try:
        p=int(page) 
    except:
        try:
            p=roman.fromRoman(page.upper())
        except:
            p=0
    return p