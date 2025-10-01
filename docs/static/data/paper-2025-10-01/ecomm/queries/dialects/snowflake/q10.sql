-- Note: Currently not supported in Snowflake with the following error:
-- Unsupported prompt input modality type: multiImages for function: AI_FILTER

WITH images AS (
  SELECT
    images.*,
    styles_details.productDisplayName AS title,
    styles_details.productDescriptors:description:value AS descr
  FROM styles_details
  JOIN image_mapping
    ON styles_details.styleImages:default:imageURL = image_mapping.link
  JOIN DIRECTORY(@images) as images
    ON images.relative_path = image_mapping.filename
  WHERE true
    -- Pre-filtering for simple colors
    AND styles_details.baseColour IN ('Black', 'Blue', 'Red', 'White')
    AND price <= 1000
)
SELECT
  SPLIT_PART(images1.relative_path, '.', 1) || '-' ||
  SPLIT_PART(images2.relative_path, '.', 1) || '-' ||
  SPLIT_PART(images3.relative_path, '.', 1) AS "id"
FROM
  images as images1,
  images as images2,
  images as images3
WHERE true
  -- Filter categories
  AND AI_FILTER(PROMPT('The image depicts a (pair of) shoe(s), sandal(s), flip-flop(s). If there are multiple products in the picture, always refer to the most promiment one. {0}', TO_FILE(images1.file_url)))
  AND AI_FILTER(PROMPT('The image depicts a piece of apparel that can be worn on the lower part of the body, like pants, shorts, skirts, ... If there are multiple products in the picture, always refer to the most promiment one. {0}', TO_FILE(images2.file_url)))
  AND AI_FILTER(PROMPT('The image depicts a piece of apparel that can be worn on the upper part of the body, like t-shirts, shirts, pullovers, hoodies, but still require some sort of clothing on the lower body, which means, e.g., not a dress. If there are multiple products in the picture, always refer to the most promiment one. {0}', TO_FILE(images3.file_url)))
  -- Join conditions
  AND AI_FILTER(PROMPT('The images depict products with the same primary base color, e.g., both are black, both are white, and both products are from the same brand. The description of the first product is {0} {1} and the image of the first product is {3}. The description of the second product is {4} {5} and the image of the second product is {6}', images1.title, images1.descr, TO_FILE(images1.file_url), images2.title, images2.descr, TO_FILE(images2.file_url)))
  AND AI_FILTER(PROMPT('The images depict products with the same primary base color, e.g., both are black, both are white, and both products are from the same brand. The description of the first product is {0} {1} and the image of the first product is {3}. The description of the second product is {4} {5} and the image of the second product is {6}', images2.title, images2.descr, TO_FILE(images2.file_url), images3.title, images3.descr, TO_FILE(images3.file_url)))
;