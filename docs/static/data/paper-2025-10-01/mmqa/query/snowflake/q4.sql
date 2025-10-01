WITH movie_genres AS (
  SELECT
    t1.title,
    PARSE_JSON(
      AI_COMPLETE(
        'openai-gpt-5-mini',
        CONCAT(
          'Extract all applicable genres for the movie based on its description: ',
          t1.text,
          '. Respond with only a JSON array of strings, for example: ["Action", "Comedy"].'
        )
      )
    ) AS genres
  FROM
    lizzy_caplan_text_data AS t1
    WHERE t1.title IN (
        "Orange County",
        "Mean Girls",
        "Love Is the Drug",
        "Crashing",
        "Cloverfield",
        "My Best Friend's Girl",
        "Crossing Over",
        "Hot Tub Time Machine",
        "The Last Rites of Ransom Pride",
        "127 Hours",
        "High Road",
        "Save the Date",
        "Bachelorette",
        "3, 2, 1... Frankie Go Boom",
        "Queens of Country",
        "Item 47",
        "The Interview",
        "The Night Before",
        "Now You See Me 2",
        "Allied",
        "The Disaster Artist",
        "Extinction",
        "The People We Hate at the Wedding",
        "Cobweb"
    )
)

SELECT
  f.value::STRING AS "genre",
  LISTAGG(t2.title, ', ') AS "movies_in_genre"
FROM
  movie_genres AS t2,
  LATERAL FLATTEN(INPUT => t2.genres) AS f
GROUP BY
  "genre"
ORDER BY
  "genre";
