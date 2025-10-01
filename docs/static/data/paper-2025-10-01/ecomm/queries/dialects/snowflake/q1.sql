SELECT
  id as "id"
FROM styles_details
WHERE true
  AND AI_FILTER(PROMPT('The product is a backpack from Reebok: {0} {1}',
    styles_details.productDisplayName,
    styles_details.productDescriptors:description:value));
