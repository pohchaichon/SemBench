WITH product_selection AS (
  SELECT *
  FROM styles_details
  WHERE true
    AND price <= 500
)
SELECT
  p1.id || '-' || p2.id AS "id"
FROM product_selection p1
JOIN product_selection p2
  ON AI_FILTER(PROMPT('''
     You will be given two product descriptions. Do both product descriptions describe products of the same category from the same brand, e.g., both are t-shirts from Adidas?
     
     The first product description is:
     {0} - {1}
     The second product description is:
     {2} - {3}
     ''',
     p1.productDisplayName,
     p1.productDescriptors:description:value,
     p2.productDisplayName,
     p2.productDescriptors:description:value
     )
  )
;
