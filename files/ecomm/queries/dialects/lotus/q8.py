from operator import itemgetter
import os
import pandas as pd
from lotus.dtype_extensions import ImageArray

def run(data_dir: str):
    # Load data
    styles_details = pd.read_parquet(os.path.join(data_dir, 'styles_details.parquet'))
    image_mapping = pd.read_parquet(os.path.join(data_dir, 'image_mapping.parquet'))

    # Pre-filter data
    styles_details = styles_details[styles_details.apply(lambda row: len(row['productDescriptors']['description']['value']) >= 3000, axis=1)]
    image_mapping = image_mapping[image_mapping['id'].astype('int').isin(styles_details['id'])]
    image_mapping['images']  = ImageArray(image_mapping.filename.apply(lambda s: os.path.join(data_dir, 'images', s)))

    # Perform joins
    processed = styles_details.sem_join(image_mapping, '''
     The image {images} fits the description: {productDisplayName} {productDescriptors}
    ''')

    processed['id'] = processed['id:left'].astype('str') + '-' + processed['id:right'].astype('str')
    return processed['id']
