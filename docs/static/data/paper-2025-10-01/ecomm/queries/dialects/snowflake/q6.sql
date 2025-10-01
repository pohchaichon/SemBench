WITH product_selection AS (
  SELECT images.*
  FROM styles_details
  JOIN image_mapping
    ON styles_details.styleImages:default:imageURL = image_mapping.link
  JOIN DIRECTORY(@images) as images
    ON images.relative_path = image_mapping.filename
  WHERE true
    AND masterCategory:typeName = 'Apparel'
    -- Exclude difficult categories:
    AND subCategory:typeName NOT IN ('Saree', 'Apparel Set', 'Loungewear and Nightwear')
)
SELECT
  SPLIT_PART(relative_path, '.', 1) as "id",
  AI_CLASSIFY(TO_FILE(images.file_url),
    [
        {'label': 'Dress', 'description': 'A dress is a one-piece outer garment that is worn on the torso, hangs down over the legs, and often consist of a bodice attached to a skirt.'},
        {'label': 'Bottomwear', 'description': 'Bottomwear refers to clothing worn on the lower part of the body, such as trousers, jeans, skirts, shorts, and leggings.'},
        {'label': 'Socks', 'description': 'Socks are a type of clothing worn on the feet, typically made of soft fabric, designed to provide comfort and warmth.'},
        {'label': 'Topwear', 'description': 'Topwear refers to clothing worn on the upper part of the body, such as shirts, blouses, t-shirts, and jackets.'},
        {'label': 'Innerwear', 'description': 'Innerwear refers to clothing worn beneath outer garments, typically close to the skin, such as underwear, bras, and undershirts.'}
    ],
    {
        'task_description': 'You are given an image of a product. Your task is to classify the product into the given categories.',
        'output_mode': 'single'
    }
  ):labels[0]::string AS "category"
FROM product_selection images
;
