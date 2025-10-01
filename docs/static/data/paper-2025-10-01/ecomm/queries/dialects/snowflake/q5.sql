WITH product_selection AS (
  SELECT *
  FROM styles_details
  WHERE true
    AND masterCategory:typeName = 'Apparel'
    -- Exclude difficult categories:
    AND subCategory:typeName NOT IN ('Saree', 'Apparel Set', 'Loungewear and Nightwear')
)
SELECT
  id as "id",
  AI_CLASSIFY(PROMPT('{0}, {1}', styles_details.productDisplayName, styles_details.productDescriptors:description:value),
    [
        {'label': 'Dress', 'description': 'A dress is a one-piece outer garment that is worn on the torso, hangs down over the legs, and often consist of a bodice attached to a skirt.'},
        {'label': 'Bottomwear', 'description': 'Bottomwear refers to clothing worn on the lower part of the body, such as trousers, jeans, skirts, shorts, and leggings.'},
        {'label': 'Socks', 'description': 'Socks are a type of clothing worn on the feet, typically made of soft fabric, designed to provide comfort and warmth.'},
        {'label': 'Topwear', 'description': 'Topwear refers to clothing worn on the upper part of the body, such as shirts, blouses, t-shirts, and jackets'},
        {'label': 'Innerwear', 'description': 'Innerwear refers to clothing worn beneath outer garments, typically close to the skin, such as underwear, bras, and undershirts.'}
    ],
    {
        'task_description': 'You are given a description of a product. Your task is to classify the product into the given categories.',
        'output_mode': 'single'
    }
  ):labels[0]::string AS "category"
FROM product_selection styles_details
;
