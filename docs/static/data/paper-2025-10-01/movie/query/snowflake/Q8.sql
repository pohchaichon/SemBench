SELECT 
    sentiment_label AS "scoreSentiment",
    COUNT(*) AS count
FROM (
    SELECT 
        CASE WHEN AI_FILTER(CONCAT('The review is clearly positive: ', "reviewText")) 
            THEN 'POSITIVE' 
            ELSE 'NEGATIVE' 
        END AS sentiment_label
    FROM reviews
    WHERE "id" = 'taken_3'
) AS sentiment_results
GROUP BY sentiment_label;