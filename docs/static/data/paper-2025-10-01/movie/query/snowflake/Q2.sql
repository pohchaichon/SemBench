SELECT "reviewId"
FROM reviews
WHERE "id" = 'taken_3' AND AI_FILTER(CONCAT('The review is clearly positive: ', "reviewText"))
LIMIT 5;
