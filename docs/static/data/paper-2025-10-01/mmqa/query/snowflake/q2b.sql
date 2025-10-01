WITH ap_warrior_joined AS (
    SELECT t.ID, i.file_url AS "image_id"
    FROM ap_warrior t, DIRECTORY(@mmqa_images) as i
    WHERE AI_FILTER(
        PROMPT(
            'You will be provided with a horse racetrack name and an image. Determine if the image shows the logo of the racetrack. Racetrack: {0}, Image: {1}', t.track, TO_FILE(i.file_url)
        )
    )
)

SELECT ID, "image_id" AS "image_id", AI_COMPLETE(
    'openai-gpt-4.1',
    PROMPT(
        'What is the color of the logo in the image if available: {0} Simply give the color name', TO_FILE("image_id")
    )
) AS "color"
FROM ap_warrior_joined;
