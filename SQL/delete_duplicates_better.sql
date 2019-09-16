DELETE FROM indeed_search_results AS a USING (
      SELECT MIN(isr_pk) as isr_pk, guid
        FROM indeed_search_results
        GROUP BY guid HAVING COUNT(*) > 1
      ) AS b
      WHERE a.guid = b.guid
      AND a.isr_pk <> b.isr_pk
