from operator import itemgetter
import os
import pandas as pd

def run(data_dir: str):
    # Load data
    styles_details = pd.read_parquet(os.path.join(data_dir, 'styles_details.parquet'))

    # Pre-filter data
    styles_details = styles_details[styles_details['price'] <= 500]
    
    # Self-join
    processed = styles_details.sem_join(styles_details, '''
     You will be given two product descriptions. Do both product descriptions describe
     products of the same category from the same brand, e.g., both are t-shirts from Adidas?
     
     The first product description is:
     {productDisplayName:left} - {productDescriptors:left}
     
     The second product description is:
     {productDisplayName:right} - {productDescriptors:right}
    ''')

    processed['id'] = processed['id:left'].astype('str') + '-' + processed['id:right'].astype('str')
    return processed['id']
