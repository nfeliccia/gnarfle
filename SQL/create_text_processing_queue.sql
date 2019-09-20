CREATE TABLE public.text_processing_queue
(
    tpq_pk integer NOT NULL DEFAULT nextval('text_processing_queue_tpq_pk_seq'::regclass),
    isr_pk integer,
    job_description text COLLATE pg_catalog."default",
    jd_processed boolean,
    CONSTRAINT text_processing_queue_pkey PRIMARY KEY (tpq_pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.text_processing_queue
    OWNER to "Beldar";
