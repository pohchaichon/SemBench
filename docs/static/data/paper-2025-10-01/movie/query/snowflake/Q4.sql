SELECT CAST(SUM(CASE WHEN AI_FILTER(CONCAT('The review is clearly positive: ', "reviewText")) THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) AS positivity_ratio
FROM reviews
WHERE "id" = 'taken_3';
