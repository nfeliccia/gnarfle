
CREATE TABLE public.indeed_search_queue
(
    search_keyword_list text COLLATE pg_catalog."default",
    creation_date timestamp without time zone,
    search_completed boolean,
    search_zip_code character(5) COLLATE pg_catalog."default",
    isq_pk integer NOT NULL DEFAULT nextval('indeed_search_queue_isq_pk_seq'::regclass),
    search_run_date timestamp without time zone,
    CONSTRAINT indeed_search_queue_pkey PRIMARY KEY (isq_pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.indeed_search_queue
    OWNER to "Beldar";
