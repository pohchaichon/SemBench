SELECT title AS "title"
FROM lizzy_caplan_text_data
WHERE AI_FILTER(
    CONCAT('The movie is a romantic comedy given their description: ', text)
);
