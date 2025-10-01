
WITH product_selection AS (
  SELECT images.*
  FROM styles_details
  JOIN image_mapping
    ON styles_details.styleImages:default:imageURL = image_mapping.link
  JOIN DIRECTORY(@images) as images
    ON images.relative_path = image_mapping.filename
  WHERE true
    -- Limit to simple colors:
    AND baseColour IN ('Black', 'Blue', 'Red', 'White', 'Orange', 'Green')
)
SELECT
  SPLIT_PART(relative_path, '.', 1) as "id",
  AI_COMPLETE('llama4-maverick', PROMPT(
    'Extract the primary color of the product in the image. Only return the capitalized name of the base color, nothing else: {0}',
    TO_FILE(images.file_url))
  ) AS "category"
FROM product_selection images
;
