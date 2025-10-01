SELECT
  SPLIT_PART(relative_path, '.', 1) as "id"
FROM DIRECTORY(@images) as images
WHERE true
  AND AI_FILTER(PROMPT('The image shows a (pair of) sports shoe(s) that feature the colors yellow and silver. {0}', TO_FILE(images.file_url)))
;
