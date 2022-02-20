
CREATE TABLE public.dim_citationgroup (
                citationgroup_pk INTEGER NOT NULL,
                CONSTRAINT dim_citationgroup_pk PRIMARY KEY (citationgroup_pk)
);


CREATE TABLE public.dim_authorgroup (
                authorgroup_pk INTEGER NOT NULL,
                CONSTRAINT dim_authorgroup_pk PRIMARY KEY (authorgroup_pk)
);


CREATE TABLE public.dim_keywordgroup (
                keywordgroup_pk INTEGER NOT NULL,
                CONSTRAINT dim_keywordgroup_pk PRIMARY KEY (keywordgroup_pk)
);


CREATE TABLE public.dim_journal (
                journal_pk INTEGER NOT NULL,
                title VARCHAR NOT NULL,
                volume INTEGER NOT NULL,
                issue INTEGER NOT NULL,
                publisher VARCHAR NOT NULL,
                place VARCHAR NOT NULL,
                CONSTRAINT dim_journal_pk PRIMARY KEY (journal_pk)
);


CREATE TABLE public.aggregation_paper (
                paper_pk INTEGER NOT NULL,
                authorgroup_pk INTEGER NOT NULL,
                keywordgroup_pk INTEGER NOT NULL,
                journal_pk INTEGER NOT NULL,
                year DATE NOT NULL,
                title VARCHAR NOT NULL,
                citekey VARCHAR NOT NULL,
                abstract TEXT NOT NULL,
                no_of_pages INTEGER NOT NULL,
                article_source_id INTEGER NOT NULL,
                model_element VARCHAR NOT NULL,
                level VARCHAR NOT NULL,
                partcipants VARCHAR NOT NULL,
                no_of_participants INTEGER NOT NULL,
                collection_method VARCHAR NOT NULL,
                sampling VARCHAR NOT NULL,
                analysis_method VARCHAR NOT NULL,
                sector VARCHAR NOT NULL,
                region VARCHAR NOT NULL,
                metric VARCHAR NOT NULL,
                metric_value REAL NOT NULL,
                conceptual_method VARCHAR NOT NULL,
                topic VARCHAR NOT NULL,
                technology VARCHAR NOT NULL,
                theory VARCHAR NOT NULL,
                paradigm VARCHAR NOT NULL,
                company_type VARCHAR NOT NULL,
                validity VARCHAR NOT NULL,
                CONSTRAINT paper_pk PRIMARY KEY (paper_pk)
);


CREATE TABLE public.dim_keyword (
                keyword_pk INTEGER NOT NULL,
                keyword_string VARCHAR NOT NULL,
                CONSTRAINT dim_keyword_pk PRIMARY KEY (keyword_pk)
);


CREATE TABLE public.bridge_paper_keyword (
                keyword_pk INTEGER NOT NULL,
                keywordgroup_pk INTEGER NOT NULL,
                CONSTRAINT bridge_paper_keyword_pk PRIMARY KEY (keyword_pk, keywordgroup_pk)
);


CREATE TABLE public.dim_author (
                author_pk INTEGER NOT NULL,
                surname VARCHAR NOT NULL,
                firstname VARCHAR NOT NULL,
                middlename VARCHAR NOT NULL,
                email VARCHAR NOT NULL,
                department VARCHAR NOT NULL,
                institution VARCHAR NOT NULL,
                country VARCHAR NOT NULL,
                CONSTRAINT dim_author_pk PRIMARY KEY (author_pk)
);


CREATE TABLE public.bridge_paper_author (
                author_pk INTEGER NOT NULL,
                authorgroup_pk INTEGER NOT NULL,
                author_position INTEGER NOT NULL,
                CONSTRAINT bridge_paper_author_pk PRIMARY KEY (author_pk, authorgroup_pk)
);


CREATE TABLE public.dim_paper (
                paper_pk INTEGER NOT NULL,
                keywordgroup_pk INTEGER NOT NULL,
                authorgroup_pk INTEGER NOT NULL,
                journal_pk INTEGER NOT NULL,
                year DATE NOT NULL,
                title VARCHAR NOT NULL,
                citekey VARCHAR NOT NULL,
                abstract TEXT NOT NULL,
                no_of_pages INTEGER NOT NULL,
                article_source_id INTEGER NOT NULL,
                CONSTRAINT dim_paper_pk PRIMARY KEY (paper_pk)
);


CREATE TABLE public.bridge_sentence_citation (
                citationgroup_pk INTEGER NOT NULL,
                paper_pk INTEGER NOT NULL,
                CONSTRAINT bridge_sentence_citation_pk PRIMARY KEY (citationgroup_pk, paper_pk)
);


CREATE TABLE public.dim_paragraph (
                paragraph_pk INTEGER NOT NULL,
                paper_pk INTEGER NOT NULL,
                subheading VARCHAR NOT NULL,
                heading VARCHAR NOT NULL,
                paragraph_type VARCHAR NOT NULL,
                para_source_id VARCHAR NOT NULL,
                CONSTRAINT dim_paragraph_pk PRIMARY KEY (paragraph_pk)
);


CREATE TABLE public.dim_entity (
                entity_pk INTEGER NOT NULL,
                entity_label VARCHAR NOT NULL,
                entity_name VARCHAR NOT NULL,
                CONSTRAINT entity_pk PRIMARY KEY (entity_pk)
);


CREATE TABLE public.map_entity_hierarchy (
                child_entity_pk INTEGER NOT NULL,
                parent_entity_pk INTEGER NOT NULL,
                depth_from_parent INTEGER NOT NULL,
                highest_parent_flag BOOLEAN NOT NULL,
                lowest_child_flag BOOLEAN NOT NULL,
                CONSTRAINT map_entity_hierarchy_pk PRIMARY KEY (child_entity_pk, parent_entity_pk)
);


CREATE TABLE public.dim_sentence (
                sentence_pk INTEGER NOT NULL,
                citationgroup_pk INTEGER NOT NULL,
                paragraph_pk INTEGER NOT NULL,
                sentence_type VARCHAR NOT NULL,
                sentence_string VARCHAR NOT NULL,
                sentence_source_id VARCHAR NOT NULL,
                CONSTRAINT sentence_pk PRIMARY KEY (sentence_pk)
);


CREATE TABLE public.fact_entity_detection (
                entity_pk INTEGER NOT NULL,
                sentence_pk INTEGER NOT NULL,
                entity_count INTEGER NOT NULL,
                CONSTRAINT fact_id PRIMARY KEY (entity_pk, sentence_pk)
);


ALTER TABLE public.dim_sentence ADD CONSTRAINT dim_citationgroup_dim_sentence_fk
FOREIGN KEY (citationgroup_pk)
REFERENCES public.dim_citationgroup (citationgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.bridge_sentence_citation ADD CONSTRAINT dim_citationgroup_bridge_sentence_citation_fk
FOREIGN KEY (citationgroup_pk)
REFERENCES public.dim_citationgroup (citationgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.dim_paper ADD CONSTRAINT dim_authorgroup_dim_paper_fk
FOREIGN KEY (authorgroup_pk)
REFERENCES public.dim_authorgroup (authorgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.bridge_paper_author ADD CONSTRAINT dim_authorgroup_bridge_paper_author_fk
FOREIGN KEY (authorgroup_pk)
REFERENCES public.dim_authorgroup (authorgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.aggregation_paper ADD CONSTRAINT dim_authorgroup_aggregation_paper_fk
FOREIGN KEY (authorgroup_pk)
REFERENCES public.dim_authorgroup (authorgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.dim_paper ADD CONSTRAINT dim_keywordgroup_dim_paper_fk
FOREIGN KEY (keywordgroup_pk)
REFERENCES public.dim_keywordgroup (keywordgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.bridge_paper_keyword ADD CONSTRAINT dim_keywordgroup_bridge_paper_keyword_fk
FOREIGN KEY (keywordgroup_pk)
REFERENCES public.dim_keywordgroup (keywordgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.aggregation_paper ADD CONSTRAINT dim_keywordgroup_aggregation_paper_fk
FOREIGN KEY (keywordgroup_pk)
REFERENCES public.dim_keywordgroup (keywordgroup_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.dim_paper ADD CONSTRAINT dim_journal_dim_paper_fk
FOREIGN KEY (journal_pk)
REFERENCES public.dim_journal (journal_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.aggregation_paper ADD CONSTRAINT dim_journal_aggregation_paper_fk
FOREIGN KEY (journal_pk)
REFERENCES public.dim_journal (journal_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.bridge_paper_keyword ADD CONSTRAINT dim_keyword_paper_keyword_bridge_fk
FOREIGN KEY (keyword_pk)
REFERENCES public.dim_keyword (keyword_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.bridge_paper_author ADD CONSTRAINT dim_author_paper_author_bridge_fk
FOREIGN KEY (author_pk)
REFERENCES public.dim_author (author_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.dim_paragraph ADD CONSTRAINT dim_paper_dim_paragraph_fk
FOREIGN KEY (paper_pk)
REFERENCES public.dim_paper (paper_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.bridge_sentence_citation ADD CONSTRAINT dim_paper_bridge_sentence_citation_fk
FOREIGN KEY (paper_pk)
REFERENCES public.dim_paper (paper_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.dim_sentence ADD CONSTRAINT dim_paragraph_dim_sentence_fk
FOREIGN KEY (paragraph_pk)
REFERENCES public.dim_paragraph (paragraph_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.fact_entity_detection ADD CONSTRAINT dim_entity_fact_entity_detection_fk
FOREIGN KEY (entity_pk)
REFERENCES public.dim_entity (entity_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.map_entity_hierarchy ADD CONSTRAINT dim_entity_entity_hierarchy_map_fk
FOREIGN KEY (child_entity_pk)
REFERENCES public.dim_entity (entity_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.map_entity_hierarchy ADD CONSTRAINT dim_entity_entity_hierarchy_map_fk1
FOREIGN KEY (parent_entity_pk)
REFERENCES public.dim_entity (entity_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE public.fact_entity_detection ADD CONSTRAINT dim_sentence_fact_entity_detection_fk
FOREIGN KEY (sentence_pk)
REFERENCES public.dim_sentence (sentence_pk)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;