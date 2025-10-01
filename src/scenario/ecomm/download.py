#!/usr/bin/env python3

"""
Standalone script for downloading and processing the Fashion Product Images dataset.
"""

import os
from dotenv import load_dotenv
import kagglehub
import duckdb
import pyarrow.parquet as pq


def _download_fashion_product_images():
    """
    Downloads the Fashion Product Images dataset from Kaggle and returns the path to the downloaded dataset in the Kaggle system cache.
    """
    dataset_path = kagglehub.dataset_download(
        "paramaggarwal/fashion-product-images-dataset"
    )
    print(f"Download complete. Dataset available at: {dataset_path}")
    return dataset_path


def _create_sample(target_dir: str, scale_factor: int, seed: int) -> str:
    """
    Creates a deterministic sample of the Fashion Product Images dataset based on the specified sampling factor.

    Args:
        target_dir: The same target dir that was used for downloading the dataset. Will be used as input for the sample.
        scale_factor: The number of rows that will be included in the dataset.

    Returns:
        The path to the directory containing the sampled dataset.
    """
    input_dir = os.path.join(target_dir, "fashion_product_images")
    out_dir = os.path.abspath(
        os.path.join(target_dir, f"fashion_product_images_{str(scale_factor)}")
    )
    if os.path.exists(out_dir):
        print(
            f"Sample directory {out_dir} already exists. Skipping sample creation."
        )
        return out_dir

    os.makedirs(out_dir, exist_ok=True)
    print(f"Creating sample with size: {scale_factor} at: {out_dir}")

    num_extra_rows = 43  # Number of rows that are definitely included and bypass the random sampling such that certain queries have a solution
    if scale_factor <= num_extra_rows:
        raise ValueError(
            f"Scale factor must be greater than {num_extra_rows} to ensure query solutions."
        )

    # Create a sample from one table; the others can then be joined to it.
    duckdb.sql(
        f"""
        COPY (
            SELECT *
            FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}')
            USING SAMPLE {scale_factor - num_extra_rows} (reservoir, {seed})
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (5299, 5300, 5301, 1623, 1624, 5303, 5314)  -- Ensure solution for Q1
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (10037, 10102, 3312, 41825, 3462) -- Ensure solution for Q2
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (3351, 30292, 10689, 8419) -- Ensure solution for Q7
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (12799, 2048, 2606, 2607, 3479, 4038, 4800, 4805, 4817, 2045, 43047, 4811) -- Ensure solution for Q8
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (6241, 1891, 53126, 1563, 15779, 47525) -- Ensure solution for Q9
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (6100, 7935, 10579) -- Ensure solution for Q10
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (8103, 13112, 8402, 3470) -- Ensure solution for Q11
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (43047, 12799, 4811) -- Ensure solution for Q13
            UNION
            SELECT * FROM read_parquet('{os.path.join(input_dir, 'styles.parquet')}') WHERE id IN (18345, 29202) -- Ensure solution for Q14
        )
        TO '{os.path.join(out_dir, 'styles.parquet')}' (FORMAT PARQUET)
    """
    )

    duckdb.sql(
        f"""
        COPY (
            SELECT styles_details.*
            FROM read_parquet('{os.path.join(out_dir, 'styles.parquet')}') AS styles
            JOIN read_parquet('{os.path.join(input_dir, 'styles_details.parquet')}') AS styles_details
            ON styles_details.id = styles.id
        )
        TO '{os.path.join(out_dir, 'styles_details.parquet')}' (FORMAT PARQUET)
    """
    )

    duckdb.sql(
        f"""
        COPY (
            SELECT image_mapping.*
            FROM read_parquet('{os.path.join(out_dir, 'styles.parquet')}') AS styles
            JOIN read_parquet('{os.path.join(input_dir, 'image_mapping.parquet')}') AS image_mapping
            ON image_mapping.id = styles.id
        )
        TO '{os.path.join(out_dir, 'image_mapping.parquet')}' (FORMAT PARQUET)
    """
    )

    # Symlink necessary image files
    os.makedirs(os.path.join(out_dir, "images"), exist_ok=True)
    table = pq.read_table(os.path.join(out_dir, "image_mapping.parquet"))
    for filename in table["filename"]:
        src = os.path.join(input_dir, "images", filename.as_py())
        dst = os.path.join(out_dir, "images", filename.as_py())
        if not os.path.islink(dst) and not os.path.exists(dst):
            os.symlink(os.path.realpath(src), dst)

    return out_dir


def download_and_postprocess_data(
    target_dir: str, scale_factor: int = None
) -> str:
    """
    Downloads the Fashion Product Images dataset into the specified target directory and processes it into Parquet files.
    Optionally creates a sample of the dataset based on the provided sampling factor.

    Args:
        target_dir: The directory where the dataset will be downloaded to.
        scale_factor: Number of rows that will be included in the dataset or None if the maximum dataset size should be used.

    Returns:
        The path to the directory containing the processed dataset.
    """
    kaggle_cache_path = _download_fashion_product_images()

    out_dir = os.path.join(target_dir, "fashion_product_images")
    os.makedirs(out_dir, exist_ok=True)

    # Process styles.csv
    output_path = os.path.join(out_dir, "styles.parquet")
    print(f"Creating {output_path}")
    if not os.path.exists(output_path):
        duckdb.sql(
            f"""
            COPY (
                SELECT *
                FROM read_csv('{os.path.join(kaggle_cache_path, 'fashion-dataset', 'styles.csv')}', header=true, delim=',', quote='"', ignore_errors=true)
            )
            TO '{output_path}' (FORMAT PARQUET)
        """
        )

    # Process images.csv
    output_path = os.path.join(out_dir, "image_mapping.parquet")
    if not os.path.exists(output_path):
        print(f"Creating {output_path}")
        duckdb.sql(
            f"""
            COPY (
                SELECT split_part(filename, '.', 1) as id, filename, link
                FROM read_csv('{os.path.join(kaggle_cache_path, 'fashion-dataset', 'images.csv')}', header=true)
            )
            TO '{output_path}' (FORMAT PARQUET)
        """
        )

    # Process styles/*.json files
    output_path = os.path.join(out_dir, "styles_details.parquet")
    if not os.path.exists(output_path):
        print(f"Creating {output_path}")
        with duckdb.connect() as con:
            con.execute("SET threads = 2;")
            con.execute("SET preserve_insertion_order=false;")
            con.execute(
                f"""
            COPY (
                SELECT unnest(data) 
                FROM read_json('{os.path.join(kaggle_cache_path, 'fashion-dataset', 'styles', '*.json')}', union_by_name=true)
            )
            TO '{output_path}' (FORMAT PARQUET)
            """
            )
        print(f"Parquet file created at: {output_path}")

    # Process images/*.jpg files
    output_path = os.path.join(out_dir, "images")
    if not os.path.exists(output_path):
        print(f"Linking images to {output_path}")
        os.symlink(
            os.path.join(kaggle_cache_path, "fashion-dataset", "images"),
            output_path,
            target_is_directory=True,
        )

    if scale_factor is not None:
        sample_dir = _create_sample(target_dir, scale_factor, 12345600)
        return sample_dir
    else:
        return out_dir


if __name__ == "__main__":
    load_dotenv()
    target_dir = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "files",
            "ecomm",
            "data",
        )
    )
    download_and_postprocess_data(target_dir=target_dir, scale_factor=0.1)

    # TODO:
    # maybe filter some data, e.g.:
    # * baseColour should not be 'NA' or ''
    # * description should have at least x characters
