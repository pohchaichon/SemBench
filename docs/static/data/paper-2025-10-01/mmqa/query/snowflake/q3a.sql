SELECT title AS "title"
FROM lizzy_caplan_text_data
WHERE AI_FILTER(
    CONCAT('The movie is a comedy movie given their description: ', text)
);
