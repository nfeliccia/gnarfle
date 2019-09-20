CREATE TABLE public.ref_zip_codes
(
    zipcodes_pk smallint,
    zip_code text COLLATE pg_catalog."default",
    metro_area_name text COLLATE pg_catalog."default",
    metro_area_rank real
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ref_zip_codes
    OWNER to "Beldar";

-- Index: ix_ref_zip_codes_zipcodes_pk

-- DROP INDEX public.ix_ref_zip_codes_zipcodes_pk;

CREATE INDEX ix_ref_zip_codes_zipcodes_pk
    ON public.ref_zip_codes USING btree
    (zipcodes_pk)
    TABLESPACE pg_default;
