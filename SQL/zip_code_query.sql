EXEC SQL BEGIN DECLARE SECTION;
CHAR zip_code[5]

EXEC SQL END DECLARE SECTION;


EXEC SQL SELECT ref_zip_codes.zipcodes_pk AS ref_zip_codes_zipcodes_pk,
  ref_zip_codes.zip_code AS ref_zip_codes_zip_code,
  ref_zip_codes.metro_area_name AS ref_zip_codes_metro_area_name,
  ref_zip_codes.metro_area_rank AS ref_zip_codes_metro_area_rank
FROM ref_zip_codes
WHERE ref_zip_codes.metro_area_rank < :zip_code[5]
