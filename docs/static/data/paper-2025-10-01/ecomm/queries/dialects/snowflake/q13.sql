SELECT styles_details.id as "id"
FROM styles_details
JOIN image_mapping
  ON image_mapping.link = styles_details.styleImages:default:imageURL
JOIN DIRECTORY(@images) as images
  ON images.relative_path = image_mapping.filename
WHERE true
  AND AI_FILTER(PROMPT('
    You will receive a description of what a customer is looking for together with an image and a textual description of the product.
    Determine if they both match.
    
    I am looking for a running shirt for men with a round neck and short sleeves,
    preferably in blue or black, but not bright colors like white.
    Also definitely not green.
    It should be suitable for outdoor running in warm weather.
    There should be a catchy or funny slogan on the shirt.
    If the t-shirt is not green, it should at least feature a striped design.

    The product has the following image {0} and textual description {1} {2}',
    TO_FILE(images.file_url),
    styles_details.productDisplayName,
    styles_details.productDescriptors:description:value
  ))
;