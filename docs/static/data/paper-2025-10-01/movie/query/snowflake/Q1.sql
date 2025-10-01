SELECT "reviewId"
FROM reviews
WHERE AI_FILTER(CONCAT('The review is clearly positive: ', "reviewText"))
LIMIT 5;
