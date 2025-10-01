WITH product_selection AS (
  SELECT *
  FROM styles_details
  WHERE true
    AND LENGTH(styles_details.productDescriptors:description:value) >= 3000
)
SELECT
  SPLIT_PART(images.relative_path, '.', 1) || '-' ||
  SPLIT_PART(images.relative_path, '.', 1) as "id"
FROM product_selection as styles_details
JOIN DIRECTORY(@images) as images
  ON AI_FILTER(PROMPT('''
     The image {0} fits the description {1} {2}''',
     TO_FILE(images.file_url),
     styles_details.productDisplayName,
     styles_details.productDescriptors:description:value
    )
  )
;
