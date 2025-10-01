-- Note: Currently not supported in Snowflake with the following error:
-- Unsupported prompt input modality type: multiImages for function: AI_FILTER

WITH product_selection AS (
  SELECT images.*
  FROM styles_details
  JOIN image_mapping
    ON styles_details.styleImages:default:imageURL = image_mapping.link
  JOIN DIRECTORY(@images) as images
    ON images.relative_path = image_mapping.filename
  WHERE true
    AND styles_details.baseColour IN ('Black', 'Blue', 'Red', 'White', 'Orange', 'Green')
    AND styles_details.colour1 = ''
    AND styles_details.colour2 = ''
    AND price < 800
)
SELECT
  SPLIT_PART(p1.relative_path, '.', 1) || '-' ||
  SPLIT_PART(p2.relative_path, '.', 1) AS "id"
FROM product_selection p1
JOIN product_selection p2
  ON p1.relative_path <> p2.relative_path
  AND AI_FILTER(PROMPT('''
     Determine whether both images display objects of the same category
     (e.g., both are shoes, both are bags, etc.) and whether these objects
     share the same dominant surface color. Disregard any logos, text, or
     printed graphics on the objects. There might be other objects in the
     images. Only focus on the main object. Base your comparison solely on
     object type and overall surface color. {0} {1}''',
     TO_FILE(p1.file_url),
     TO_FILE(p2.file_url)
     )
  )
;
