SELECT
  id as "id", AI_COMPLETE('llama4-maverick', PROMPT(
    'Extract the brand name from the following product description. Only return the brand name, nothing else: {0} {1}',
    styles_details.productDisplayName,
    styles_details.productDescriptors:description:value)
  ) AS "category"
FROM styles_details;
