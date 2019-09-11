CREATE TABLE public.indeed_search_set
(
    iss_pk serial NOT NULL,
    search_keyword_list character varying(63),
    search_zip_code character varying(63),
    creation_date  timestamp,
    search_completed  boolean,
    search_run_date timestamp,
    PRIMARY KEY (isr_pk)
)
WITH (
    OIDS = FALSE
);