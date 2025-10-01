SELECT t.Airlines AS "Airlines", i.file_url AS "image_id"
FROM tampa_international_airport t, DIRECTORY(@mmqa_images) as i
WHERE AI_FILTER(
    PROMPT(
        'You will be provided with an airline name and an image. Determine if the image shows the logo of the airline. Airline: {0}, Image: {1}', t.Airlines, TO_FILE(i.file_url)
    )
);
