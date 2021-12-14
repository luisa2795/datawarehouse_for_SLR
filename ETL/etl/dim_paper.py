import pandas as pd
import etl.common_functions as cof
import etl.dim_author as auth
import etl.dim_journal as jour
import etl.database as db
import roman


    # def __init__(self, sourcepath, connection_params):
    #     super().__init__(sourcepath, connection_params)
    #     self._unique_papers=None
    #     self._papers_in_dwh = self._targetdb.load_full_table('dim_paper')
    #     self._max_pk=max(self._papers_in_dwh.author_pk, default=0)
    
def extract_all_papers():
    """Loads data from papers_final and unique_references sourcefiles, merges them and merges duplicate papers.
    """
    from_papers=cof.load_sourcefile('papers_final.csv')
    #auth_authors=self._clean_authors_from_authors(from_authors)
    from_references=cof.load_sourcefile('unique_references.csv')
    #reference_authors=self._clean_authors_from_references(from_references)
    #self._unique_authors=pd.merge(auth_authors, reference_authors, how='outer', on=['surname', 'firstname', 'middlename'], suffixes=[None, '_ref'])[['surname', 'firstname', 'middlename', 'email', 'department', 'institution', 'country']]
    return from_papers, from_references

def transform_papers(source_papers, engine):
    keywords_df=cof.load_sourcefile('keywords.csv')
    articles_prep=_join_articles_keyword_pk(source_papers, keywords_df, engine)
    #join articles with authors and lookup existing foreign key author_pk
    authors_df=cof.load_sourcefile('authors.csv').rename(columns={'departments': 'department', 'institutions': 'institution', 'countries': 'country'})
    articles_prep=_prepare_article_authors(authors_df, articles_prep)
    articles_prep=_join_papers_author_pk(articles_prep, engine).drop(columns=['surname', 'firstname', 'middlename','email_x', 'department_x', 'institution_x', 'country_x', 'email_y', 'department_y', 'institution_y', 'country_y', 'row_effective_date', 'row_expiration_date', 'current_row_indicator'], axis=1)
    #join articles with journals and lookup existing foreign key journal_pk
    articles_prep=_prepare_paper_journals(articles_prep)
    articles_prep=_join_papers_journal_pk(articles_prep, engine).drop(columns=['journal_akronym'], axis=1)
    return articles_prep

def transform_references(source_references, engine):
    # create 'keyword_pk'=0 for papers from references with reference to dummy entry in dim_keyword
    references_prep=source_references.assign(keyword_pk=0)
    #join references with authors and lookup existing foreign key author_pk
    references_prep=_prepare_reference_authors(references_prep)
    references_prep=_join_papers_author_pk(references_prep, engine).drop(columns=['authors', 'surname', 'firstname', 'middlename', 'email', 'department', 'institution', 'country', 'row_effective_date', 'row_expiration_date', 'current_row_indicator'], axis=1)
    #join articles with journals and lookup existing foreign key journal_pk
    references_prep=_prepare_paper_journals(references_prep)
    references_prep=_join_papers_journal_pk(references_prep, engine).drop(columns=['source_type', 'editor', 'monograph_title', 'note'], axis=1)
    return references_prep

def merge_all_papers(prepared_references, prepared_authors):
    prepared_references.pages=prepared_references.pages.apply(lambda x: x.split('-') if x==x else [0, 0])
    prepared_references[['pages_start', 'pages_end']]=pd.DataFrame(prepared_references.pages.tolist(), index=prepared_references.index)
    prepared_references.pages_start=prepared_references.pages_start.apply(lambda p: _page_number_to_int(p))
    prepared_references.pages_end=prepared_references.pages_end.apply(lambda p: _page_number_to_int(p))
    prepared_references['number_of_pages']=prepared_references.pages_end+prepared_references.pages_start

    #merge papers from articles and from references
    all_papers=pd.merge(prepared_authors, prepared_references, how='outer', on='citekey', suffixes=['_art', '_ref'])
    all_papers['year']=all_papers.apply(lambda x: x.year_art if x.year_art==x.year_art else x.year_ref, axis=1) 
    all_papers['title']=all_papers.apply(lambda x: x.title_art if x.title_art==x.title_art else x.title_ref, axis=1)
    all_papers['author_pk']=all_papers.apply(lambda x: x.author_pk_art if x.author_pk_art==x.author_pk_art else x.author_pk_ref, axis=1)
    all_papers['no_of_pages']=all_papers.apply(lambda x: x.number_of_pages_art if x.number_of_pages_art==x.number_of_pages_art else x.number_of_pages_ref, axis=1)
    all_papers['journal_pk']=all_papers.apply(lambda x: x.journal_pk_art if x.journal_pk_art==x.journal_pk_art else x.journal_pk_ref, axis=1)
    all_papers['keyword_pk']=all_papers['keyword_pk_art']
    all_papers.fillna({'article_id': 0, 'author_position': 0, 'citekey': 'MISSING', 'abstract': 'MISSING', 'year': 0, 'title': 'MISSING', 'author_pk': 0, 'no_of_pages': 0, 'journal_pk': 0,'keyword_pk': 0}, inplace=True)

    final_papers=all_papers[['article_id', 'author_position', 'citekey', 'abstract', 'year', 'title', 'author_pk', 'no_of_pages', 'journal_pk', 'keyword_pk']]
    return final_papers

def find_delta_papers(source_papers, papers_in_dwh):
    outer=pd.merge(source_papers, papers_in_dwh, how='outer')[['article_id', 'author_position', 'citekey', 'abstract', 'year', 'title', 'author_pk', 'no_of_pages', 'journal_pk', 'keyword_pk']]
    delta_papers=pd.concat([outer,papers_in_dwh]).drop_duplicates(keep=False)
    #add a consecutive key, starting from max_pk +1
    max_pk=max(papers_in_dwh.paper_pk, default=0)
    delta_papers['journal_pk']=list(range(max_pk+1, max_pk+1+delta_papers.index.size))
    #insert dummy row with primary key 0 if the table was empty before. Will serve as dummy for linked tables to avoid missing foreign keys in case of missing values
    if max_pk==0:
        delta_papers=delta_papers.append({'paper_pk': 0, 'article_id': 0, 'author_position': 0, 'citekey': 'MISSING', 'abstract': 'MISSING', 'year': 0, 'title': 'MISSING', 'author_pk': 0, 'no_of_pages': 0, 'journal_pk': 0,'keyword_pk': 0}, ignore_index=True)
    return delta_papers

def _join_articles_keyword_pk(articles_df, keywords_df, engine):
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
    #some cells in the source data still contain numbers, html tags or @ tags, these are removed
    authors_df.fullname=authors_df.fullname.apply(lambda f: auth._remove_numbers_amp_and_at_tags(f))
    #then split the fullname again into the columns first-, middle- and surname
    authors_df[['surname', 'firstname', 'middlename']]=pd.DataFrame(authors_df.fullname.apply(lambda fn: auth._split_fullname(fn)).to_list(), index=authors_df.index)
    article_authors=pd.merge(authors_df, articles_df, how='outer', on='article_id').drop(columns=['fullname', 'authors'])
    article_authors.fillna({'surname': 'MISSING', 'firstname': 'MISSING', 'middlename': 'MISSING'}, axis=0, inplace=True)
    return article_authors

def _join_papers_author_pk(articles_df, engine):
    authors_in_dwh=db.load_full_table(engine, 'dim_author')
    joined=pd.merge(articles_df, authors_in_dwh, how= 'left', on=['surname', 'firstname', 'middlename'])
    return joined

def _prepare_paper_journals(paper_df):
    paper_df.fillna({'journal': 'MISSING', 'volume':0, 'issue': 0, 'publisher': 'MISSING', 'place': 'MISSING'}, inplace=True)
    paper_df.volume=paper_df.volume.apply(lambda v: jour._volume_to_int(v))
    paper_df.issue=paper_df.issue.apply(lambda i: jour._issue_to_int(i))
    return paper_df

def _join_papers_journal_pk(paper_df, engine):
    journals_in_dwh=db.load_full_table(engine, 'dim_journal').rename({'title': 'journal'})
    joined=pd.merge(paper_df, journals_in_dwh, how='left', left_on=['journal', 'volume', 'issue', 'publisher', 'place'], right_on=['title', 'volume', 'issue', 'publisher', 'place'], suffixes=[None, '_db'])
    joined=joined.drop(columns=['journal', 'volume', 'issue', 'publisher', 'place', 'title_db'], axis=1)
    return joined

def _prepare_reference_authors(references_df):
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
    try:
        p=int(page) 
    except:
        try:
            p=roman.fromRoman(page.upper())
        except:
            p=0
    return p