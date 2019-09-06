CREATE TABLE public.indeed_search_results
(
    isr_pk serial NOT NULL,
    guid character(10),
    job_title_row character varying(255),
    real_job_title character varying(255),
    company character varying(127),
    in_location_zip character varying(63),
    source character varying(127),
    publish_date character varying(255),
    short_description text,
    lat real,
    longitude real,
    extracted_url character varying(128),
    scraped boolean,
    PRIMARY KEY (isr_pk)
)
WITH (
    OIDS = FALSE
);