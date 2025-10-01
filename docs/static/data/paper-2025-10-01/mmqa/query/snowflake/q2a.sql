SELECT t.ID, i.file_url AS "image_id"
FROM ap_warrior t, DIRECTORY(@mmqa_images) as i
WHERE AI_FILTER(
    PROMPT(
        'You will be provided with a horse racetrack name and an image. Determine if the image shows the logo of the racetrack. Racetrack: {0}, Image: {1}', t.track, TO_FILE(i.file_url)
    )
);
