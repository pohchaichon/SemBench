-- Q2: How many sound recordings of elephants do we have in our database?
-- Ground truth: select count(*) from AudioData where Animal = 'Elephant';
-- SELECT COUNT(*) AS count
-- FROM animals_dataset.audio_data_mm 
-- WHERE AI.IF(('Does this audio contain an elephant sound?', audio), 
--     connection_id => '<<connection>>', 
--     model_params => JSON '{"labels":{"query_uuid": "<<query_id>>"}, "generation_config":{"thinking_config": {"thinking_budget": <<thinking_budget>>}}}' <<other_params>>);

SELECT COUNT(*) AS count
FROM animals_dataset.audio_data_mm 
WHERE IF(AI.GENERATE(
    prompt => ("Does this audio contain an elephant sound? Answer only TRUE or FALSE, nothing more.", audio), 
    connection_id => '<<connection>>', 
    model_params => JSON '{"labels":{"query_uuid": "<<query_id>>"}, "generation_config":{"thinking_config": {"thinking_budget": <<thinking_budget>>}}}' <<other_params>>).result = "TRUE", 1, 0) = 1;