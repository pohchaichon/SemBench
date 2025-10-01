WITH movie_with_director AS (
    SELECT title AS "title", AI_COMPLETE(
        'openai-gpt-5-mini',
        CONCAT('Extract the director name from the following movie description: ', text)
    ) AS "director"
    FROM ben_piazza_text_data
)

SELECT t2."director"
FROM ben_piazza t1
JOIN movie_with_director t2
ON t1.Title = t2."title"
WHERE t1.Role = 'Bob Whitewood';
