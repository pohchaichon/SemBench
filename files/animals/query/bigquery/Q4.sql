-- Q4: What is the city for which we have most recordings of elephants?
-- Ground truth: select city from AudioData where Animal = 'Elephant' group by city order by count(*) desc limit 1;
-- SELECT City AS city
-- FROM animals_dataset.audio_data_mm 
-- WHERE AI.IF(('Does this audio contain an elephant sound?', audio), 
--     connection_id => '<<connection>>', 
--     model_params => JSON '{"labels":{"query_uuid": "<<query_id>>"}, "generation_config":{"thinking_config": {"thinking_budget": <<thinking_budget>>}}}' <<other_params>>)
-- GROUP BY City 
-- ORDER BY COUNT(*) DESC 
-- LIMIT 1;

SELECT City AS city
FROM animals_dataset.audio_data_mm 
WHERE IF(AI.GENERATE(
    prompt => ("Does this audio contain an elephant sound? Answer only TRUE or FALSE, nothing more.", audio), 
    connection_id => '<<connection>>', 
    model_params => JSON '{"labels":{"query_uuid": "<<query_id>>"}, "generation_config":{"thinking_config": {"thinking_budget": <<thinking_budget>>}}}' <<other_params>>).result = "TRUE", 1, 0) = 1
GROUP BY City 
ORDER BY COUNT(*) DESC 
LIMIT 1;