create TABLE public.indeed_search_set
(
    iss_pk integer NOT NULL DEFAULT nextval('indeed_search_set_iss_pk_seq'::regclass),
    search_keyword_list character varying(63) COLLATE pg_catalog."default",
    search_zip_code character varying(31)
    creation_date date,
    search_completed boolean,
    search_run_date date,
    CONSTRAINT indeed_search_set_pkey PRIMARY KEY (iss_pk)
)
with (OIDS = FALSE)
TABLESPACE pg_default;

alter table public.indeed_search_set
    OWNER to "Beldar";