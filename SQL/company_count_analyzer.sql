WITH  company_count AS (
  SELECT indeed_search_results.company as over_ten_co,
    COUNT(indeed_search_results.company) as co_count
  FROM indeed_search_results
  HAVING COUNT(indeed_search_results.company) > 10)

SELECT  *
FROM indeed_search_results
WHERE company_count.over_ten_co IN (SELECT company_count.over_ten_co)
ORDER BY publish_date;
