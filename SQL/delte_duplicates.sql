WITH singled_out as
	(select distinct ON (guid) guid,isr_pk from indeed_search_results)
DELETE FROM indeed_search_results
WHERE indeed_search_results.isr_pk NOT IN (SELECT singled_out.isr_pk FROM singled_out);

SELECT COUNT(*) FROM indeed_search_results;