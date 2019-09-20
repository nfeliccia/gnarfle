CREATE TABLE public.indeed_search_results
(
    isr_pk integer NOT NULL DEFAULT nextval('indeed_search_results_isr_pk_seq1'::regclass),
    company character varying(255) COLLATE pg_catalog."default",
    extracted_url character varying(255) COLLATE pg_catalog."default",
    guid character varying(48) COLLATE pg_catalog."default",
    job_title_row text COLLATE pg_catalog."default",
    latitude real,
    longitude real,
    publish_date timestamp without time zone,
    job_text_raw text COLLATE pg_catalog."default",
    job_title character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT indeed_search_results_pkey1 PRIMARY KEY (isr_pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.indeed_search_results
    OWNER to "Beldar";
