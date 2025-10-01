SELECT r1."id", r1."reviewId", r2."reviewId"
FROM reviews AS r1
JOIN reviews AS r2
ON r1."id" = r2."id" AND r1."reviewId" <> r2."reviewId"
WHERE r1."id" = 'ant_man_and_the_wasp_quantumania' AND AI_FILTER(PROMPT('These two movie reviews express opposite sentiments - one is positive and the other is negative. Review 1: {0} Review 2: {1}', r1."reviewText", r2."reviewText"));