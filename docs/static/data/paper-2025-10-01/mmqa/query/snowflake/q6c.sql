SELECT airlines AS "Airlines"
FROM tampa_international_airport
WHERE AI_FILTER(
    'Given destinations ''' || destinations || ''' of ' || airlines || ', the airline has flights to Europe.'
);
