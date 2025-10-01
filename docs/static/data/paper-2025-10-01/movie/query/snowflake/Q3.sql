SELECT COUNT(*) AS positive_review_cnt
FROM reviews
WHERE "id" = 'taken_3' AND AI_FILTER(CONCAT('The review is clearly positive: ', "reviewText"));
