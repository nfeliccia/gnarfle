SELECT indeed_search_queue.isr_pk,indeed_search_queue.job_text_raw as job_text
FROM  indeed_search_queue
INNER JOIN indeed_search_results
ON indeed_search_queue.isr_pk = indeed_search_results.isr_pk
ORDER BY indeed_search_results.publish_date DESC;
