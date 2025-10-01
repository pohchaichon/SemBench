'''
Created on Jun 10, 2025

@author: immanueltrummer

Creates test database from two Kaggle datasets:
- https://www.kaggle.com/datasets/rushibalajiputthewad/sound-classification-of-animal-voice
- https://www.kaggle.com/datasets/hypnotu/dsail-porini

@improved by: Jiale Lao
different cities have different numbers of images for different animal species
'''
import argparse
import pandas as pd
import random
import os

from pathlib import Path




def _add_strategic_locations(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """ Adds strategic locations to ensure deterministic query results.
    
    Args:
        df: DataFrame to which locations are added.
        data_type: Either 'audio' or 'image' to determine distribution strategy.
    
    Returns:
        DataFrame with additional columns for city and station ID.
    """
    cities = ['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru', 'Eldoret']
    stations = ['Station_A', 'Station_B', 'Station_C', 'Station_D']
    
    # Define strategic distribution based on species and data type
    distribution_strategy = {
        # For images: ensure Nairobi has most zebras, Mombasa most elephants
        'image': {
            'zebra': {'Nairobi': 0.4, 'Mombasa': 0.2, 'Kisumu': 0.15, 'Nakuru': 0.15, 'Eldoret': 0.1},
            'elephant': {'Mombasa': 0.4, 'Nairobi': 0.2, 'Kisumu': 0.15, 'Nakuru': 0.15, 'Eldoret': 0.1},
            'impala': {'Kisumu': 0.3, 'Nairobi': 0.25, 'Mombasa': 0.2, 'Nakuru': 0.15, 'Eldoret': 0.1},
            'monkey': {'Nakuru': 0.4, 'Nairobi': 0.3, 'Mombasa': 0.15, 'Kisumu': 0.1, 'Eldoret': 0.05},
            # Add distributions for new species
            'warthog': {'Eldoret': 0.3, 'Nakuru': 0.25, 'Kisumu': 0.2, 'Nairobi': 0.15, 'Mombasa': 0.1},
            'waterbuck': {'Kisumu': 0.35, 'Mombasa': 0.25, 'Nairobi': 0.2, 'Nakuru': 0.15, 'Eldoret': 0.05},
            'bushbuck': {'Nakuru': 0.3, 'Eldoret': 0.25, 'Nairobi': 0.2, 'Kisumu': 0.15, 'Mombasa': 0.1},
            'default': {'Nairobi': 0.28, 'Mombasa': 0.22, 'Kisumu': 0.2, 'Nakuru': 0.18, 'Eldoret': 0.12}
        },
        # For audio: ensure Kisumu has most elephants, Nakuru most monkeys
        'audio': {
            'elephant': {'Kisumu': 0.4, 'Mombasa': 0.2, 'Nairobi': 0.15, 'Nakuru': 0.15, 'Eldoret': 0.1},
            'monkey': {'Nakuru': 0.45, 'Nairobi': 0.25, 'Mombasa': 0.2, 'Kisumu': 0.05, 'Eldoret': 0.05},
            'default': {'Nairobi': 0.28, 'Mombasa': 0.22, 'Kisumu': 0.2, 'Nakuru': 0.18, 'Eldoret': 0.12}
        }
    }
    
    city_assignments = []
    station_assignments = []
    
    for idx, row in df.iterrows():
        # Determine species from the row
        if data_type == 'audio':
            species_key = row['Animal'].lower() if 'Animal' in row else 'default'
        else:  # image
            species_list = str(row['Species']).lower() if 'Species' in row else ''
            # Handle multiple species in single entry (e.g., "ZEBRA, IMPALA")
            if 'zebra' in species_list:
                species_key = 'zebra'
            elif 'elephant' in species_list:
                species_key = 'elephant'
            elif 'impala' in species_list:
                species_key = 'impala'
            elif 'monkey' in species_list:
                species_key = 'monkey'
            # Add handling for other species found in the dataset
            elif 'warthog' in species_list:
                species_key = 'warthog'
            elif 'waterbuck' in species_list:
                species_key = 'waterbuck'
            elif 'bushbuck' in species_list:
                species_key = 'bushbuck'
            else:
                species_key = 'default'
        
        # Get distribution weights for this species
        weights = distribution_strategy[data_type].get(species_key, distribution_strategy[data_type]['default'])
        
        # Choose city based on weights
        city = random.choices(cities, weights=list(weights.values()))[0]
        
        # Station distribution varies by city to ensure unique (city, station) combinations
        station_weights = {
            'Nairobi': [0.4, 0.3, 0.2, 0.1],    # Favor Station_A
            'Mombasa': [0.3, 0.4, 0.2, 0.1],    # Favor Station_B
            'Kisumu': [0.2, 0.3, 0.4, 0.1],     # Favor Station_C
            'Nakuru': [0.1, 0.2, 0.3, 0.4],     # Favor Station_D
            'Eldoret': [0.25, 0.25, 0.25, 0.25] # Even distribution
        }
        
        station = random.choices(stations, weights=station_weights[city])[0]
        
        city_assignments.append(city)
        station_assignments.append(station)
    
    df['City'] = city_assignments
    df['StationID'] = station_assignments
    return df


def _ensure_q6_pattern(audio_df: pd.DataFrame, image_df: pd.DataFrame) -> tuple:
    """ Ensures Q6 pattern: cities with monkey images but no monkey audio.
    
    Args:
        audio_df: Audio DataFrame
        image_df: Image DataFrame
    
    Returns:
        Tuple of modified (audio_df, image_df)
    """
    # Q6: Ensure Kisumu, Eldoret, and Mombasa have monkey images but no monkey audio
    target_cities = ['Kisumu', 'Eldoret', 'Mombasa']
    
    # First, ensure these cities have monkey images
    monkey_images = image_df[image_df['Species'].str.contains('monkey', case=False, na=False)]
    
    for city in target_cities:
        city_monkey_images = image_df[
            (image_df['Species'].str.contains('monkey', case=False, na=False)) & 
            (image_df['City'] == city)
        ]
        
        if len(city_monkey_images) == 0:
            # Find a monkey image and assign it to this city
            if len(monkey_images) > 0:
                # Get the first available monkey image
                idx = monkey_images.index[0]
                image_df.loc[idx, 'City'] = city
                station_map = {'Kisumu': 'Station_C', 'Eldoret': 'Station_D', 'Mombasa': 'Station_B'}
                image_df.loc[idx, 'StationID'] = station_map[city]
                # Remove this image from available monkey images for next city
                monkey_images = monkey_images.drop(idx)
    
    # Second, ensure these cities have NO monkey audio recordings
    for city in target_cities:
        # Find all monkey audio in these cities and move them to other cities
        city_monkey_audio = audio_df[
            (audio_df['Animal'].str.contains('monkey', case=False, na=False)) & 
            (audio_df['City'] == city)
        ]
        
        if len(city_monkey_audio) > 0:
            # Move these audio recordings to Nakuru or Nairobi (which should have monkey audio)
            target_audio_cities = ['Nakuru', 'Nairobi']
            for i, idx in enumerate(city_monkey_audio.index):
                target_city = target_audio_cities[i % len(target_audio_cities)]
                audio_df.loc[idx, 'City'] = target_city
                audio_df.loc[idx, 'StationID'] = 'Station_A' if target_city == 'Nairobi' else 'Station_D'
    
    return audio_df, image_df


def _ensure_q9_pattern(audio_df: pd.DataFrame, image_df: pd.DataFrame) -> tuple:
    """ Ensures Q9 pattern: cities with both monkey images and monkey audio.
    
    Args:
        audio_df: Audio DataFrame
        image_df: Image DataFrame
    
    Returns:
        Tuple of modified (audio_df, image_df)
    """
    # Q9: Ensure Nakuru and Nairobi have both monkey images and audio
    target_cities = ['Nakuru', 'Nairobi']
    
    # Ensure each target city has both monkey images and audio
    for city in target_cities:
        # Ensure monkey images in this city
        city_monkey_images = image_df[
            (image_df['Species'].str.contains('monkey', case=False, na=False)) & 
            (image_df['City'] == city)
        ]
        
        if len(city_monkey_images) == 0:
            # Find any monkey image not already assigned to Q6 cities and assign it to this city
            q6_cities = ['Kisumu', 'Eldoret', 'Mombasa']
            available_monkey_images = image_df[
                (image_df['Species'].str.contains('monkey', case=False, na=False)) &
                (~image_df['City'].isin(q6_cities))
            ]
            if len(available_monkey_images) == 0:
                # If no available images outside Q6 cities, take from any city
                available_monkey_images = image_df[
                    image_df['Species'].str.contains('monkey', case=False, na=False)
                ]
            
            if len(available_monkey_images) > 0:
                idx = available_monkey_images.index[0]
                image_df.loc[idx, 'City'] = city
                image_df.loc[idx, 'StationID'] = 'Station_A' if city == 'Nairobi' else 'Station_D'
        
        # Ensure monkey audio in this city  
        city_monkey_audio = audio_df[
            (audio_df['Animal'].str.contains('monkey', case=False, na=False)) & 
            (audio_df['City'] == city)
        ]
        
        if len(city_monkey_audio) == 0:
            # Find any monkey audio and assign it to this city
            available_monkey_audio = audio_df[
                audio_df['Animal'].str.contains('monkey', case=False, na=False)
            ]
            if len(available_monkey_audio) > 0:
                # Find one not already in this city
                for idx in available_monkey_audio.index:
                    if audio_df.loc[idx, 'City'] != city:
                        audio_df.loc[idx, 'City'] = city
                        audio_df.loc[idx, 'StationID'] = 'Station_A' if city == 'Nairobi' else 'Station_D'
                        break
    
    return audio_df, image_df




def _ensure_cooccurrence_patterns(audio_df: pd.DataFrame, image_df: pd.DataFrame) -> tuple:
    """ Ensures specific co-occurrence patterns needed for queries.
    
    Args:
        audio_df: Audio DataFrame
        image_df: Image DataFrame
    
    Returns:
        Tuple of modified (audio_df, image_df)
    """
    # Query 7: Ensure zebra and impala co-occur in at least Nairobi and Kisumu
    cooccurrence_cities = ['Nairobi', 'Kisumu']
    
    for city in cooccurrence_cities:
        # Ensure zebra images in this city
        zebra_rows = image_df[image_df['Species'].str.contains('zebra', case=False, na=False)]
        if len(zebra_rows[zebra_rows['City'] == city]) == 0:
            # Find a zebra row and assign it to this city
            if len(zebra_rows) > 0:
                idx = zebra_rows.index[0]
                image_df.loc[idx, 'City'] = city
                image_df.loc[idx, 'StationID'] = 'Station_A'
        
        # Ensure impala images in this city
        impala_rows = image_df[image_df['Species'].str.contains('impala', case=False, na=False)]
        if len(impala_rows[impala_rows['City'] == city]) == 0:
            # Find an impala row and assign it to this city
            if len(impala_rows) > 0:
                idx = impala_rows.index[0]
                image_df.loc[idx, 'City'] = city
                image_df.loc[idx, 'StationID'] = 'Station_B'
    
    # Query 8: Ensure elephant and monkey co-occur in both image and audio in Nairobi
    target_city = 'Nairobi'
    target_station = 'Station_A'
    
    # Ensure elephant presence in both modalities
    elephant_audio = audio_df[audio_df['Animal'].str.contains('elephant', case=False, na=False)]
    if len(elephant_audio[elephant_audio['City'] == target_city]) == 0 and len(elephant_audio) > 0:
        idx = elephant_audio.index[0]
        audio_df.loc[idx, 'City'] = target_city
        audio_df.loc[idx, 'StationID'] = target_station
    
    elephant_image = image_df[image_df['Species'].str.contains('elephant', case=False, na=False)]
    if len(elephant_image[elephant_image['City'] == target_city]) == 0 and len(elephant_image) > 0:
        idx = elephant_image.index[0]
        image_df.loc[idx, 'City'] = target_city
        image_df.loc[idx, 'StationID'] = target_station
    
    # Ensure monkey presence in both modalities
    monkey_audio = audio_df[audio_df['Animal'].str.contains('monkey', case=False, na=False)]
    if len(monkey_audio[monkey_audio['City'] == target_city]) == 0 and len(monkey_audio) > 0:
        idx = monkey_audio.index[0]
        audio_df.loc[idx, 'City'] = target_city
        audio_df.loc[idx, 'StationID'] = target_station
    
    monkey_image = image_df[image_df['Species'].str.contains('monkey', case=False, na=False)]
    if len(monkey_image[monkey_image['City'] == target_city]) == 0 and len(monkey_image) > 0:
        idx = monkey_image.index[0]
        image_df.loc[idx, 'City'] = target_city
        image_df.loc[idx, 'StationID'] = target_station
    
    return audio_df, image_df


def _generate_audio_table(
        audio_path: str, scaling_factor: int) -> pd.DataFrame:
    """ Generates a table referencing audio data from Kaggle.
    
    Args:
        audio_path: Path to Kaggle data with audio files recording animals.
        scaling_factor: The number of rows in the generated table.
    
    Returns:
        DataFrame with audio data.
    """
    # Get all directories in the audio path
    audio_path = Path(audio_path) / 'Animal-Soundprepros'
    directories = list(audio_path.glob('*/'))
    # Generate list with pairs of directory names and full file paths
    animal_recording = []
    for directory in directories:
        files = list(directory.glob('*.wav'))
        for file in files:
            file_path = str(file.resolve())
            animal_recording.append(
                (directory.name, file_path))
    # Shuffle the list of animal recordings
    random.shuffle(animal_recording)
    # Generate DataFrame containing requested number of rows
    df = pd.DataFrame(
        animal_recording[:scaling_factor],
        columns=['Animal', 'AudioPath'])
    # Add column with strategically chosen city and station ID
    return _add_strategic_locations(df, 'audio')


def _find_image_path(image_path: Path, filename: str, device: str) -> str:
    """ Finds the full path for an image file based on device type.
    
    Args:
        image_path: Base path to the image dataset.
        filename: Name of the image file.
        device: Device type that captured the image.
    
    Returns:
        Full path to the image file.
    """
    if 'OpenMV' in device:
        return str(image_path / 'OpenMV_images' / filename)
    elif 'Raspberry Pi' in device:
        # Search for the file in all RaspberryPi deployment folders
        raspberry_dir = image_path / 'RaspberryPi_images'
        for deployment_folder in raspberry_dir.glob('*-deployment'):
            potential_path = deployment_folder / filename
            if potential_path.exists():
                return str(potential_path)
        # If not found, return expected path (might be missing)
        return str(raspberry_dir / filename)
    else:
        # Unknown device, return a generic path
        return str(image_path / filename)


def _generate_image_table(
        image_path: str, scaling_factor: int) -> pd.DataFrame:
    """ Generates a table referencing image data from Kaggle.
    
    Args:
        image_path: Path to Kaggle data with images (camera traps to record animals).
        scaling_factor: The number of rows in the generated table.
    
    Returns:
        DataFrame with image data.
    """
    # Extract annotations from .xls file
    image_path = Path(image_path) / "DSAIL-Porini Annotated camera trap images of wildlife species from a conservancy in Kenya"
    xls_path = image_path / 'camera_trap_dataset_annotation.xlsx'
    df = pd.read_excel(xls_path, sheet_name='Sheet1')
    
    # Include images from all devices, not just OpenMV Cam H7
    print(f"Total images in dataset: {len(df)}")
    print("Device distribution:")
    print(df['Device'].value_counts())
    
    # Add column with full image path based on device type
    df['ImagePath'] = df.apply(
        lambda row: _find_image_path(image_path, row['Filename'], row['Device']), 
        axis=1)
    
    # Filter out rows where image files don't exist (optional - can be removed if needed)
    existing_images = []
    for idx, row in df.iterrows():
        if Path(row['ImagePath']).exists():
            existing_images.append(idx)
    
    print(f"Images with existing files: {len(existing_images)} out of {len(df)}")
    df_existing = df.loc[existing_images].copy()
    
    # If we don't have enough existing images, use all entries anyway
    if len(df_existing) < scaling_factor:
        print(f"Warning: Only {len(df_existing)} images exist, but {scaling_factor} requested. Using all available data.")
        df_filtered = df.copy()
    else:
        df_filtered = df_existing.copy()
    
    # Drop all columns except "ImagePath" and "Species"
    df_filtered = df_filtered[['ImagePath', 'Species']]
    
    # Ensure sufficient monkey images for Q6 and Q9 (need at least 10 total)
    monkey_rows = df_filtered[df_filtered['Species'].str.contains('monkey', case=False, na=False)]
    non_monkey_rows = df_filtered[~df_filtered['Species'].str.contains('monkey', case=False, na=False)]
    
    min_monkeys_needed = 10  # Enough for Q6 (3 cities) and Q9 (2 cities) requirements
    
    if len(monkey_rows) > 0:
        # Take enough monkeys and sample the rest
        monkeys_to_include = min(len(monkey_rows), min_monkeys_needed)
        selected_monkeys = monkey_rows.sample(n=monkeys_to_include, random_state=42)
        remaining_slots = scaling_factor - len(selected_monkeys)
        
        if remaining_slots > 0 and len(non_monkey_rows) > 0:
            selected_non_monkeys = non_monkey_rows.sample(n=min(remaining_slots, len(non_monkey_rows)), random_state=42)
            df_filtered = pd.concat([selected_monkeys, selected_non_monkeys]).sample(frac=1, random_state=42).reset_index(drop=True)
        else:
            df_filtered = selected_monkeys.copy()
    else:
        # No monkeys found, proceed with normal sampling
        df_filtered = df_filtered.sample(n=min(scaling_factor, len(df_filtered)), random_state=42).reset_index(drop=True)
    
    # Add column with strategically chosen city and station ID
    df_filtered = _add_strategic_locations(df_filtered, 'image')
    
    # Return DataFrame with image data
    return df_filtered
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'audio_path', type=str, 
        help='Path to Kaggle data with audio files recording animal sounds'
        )
    parser.add_argument(
        'image_path', type=str,
        help='Path to Kaggle data with images (camera traps to record animals'
        )
    parser.add_argument(
        'scaling_factor', type=int,
        help='The number of rows in the image table (audio table will be min(scaling_factor/3, 650))'
        )
    parser.add_argument(
        '--seed', type=int, default=42,
        help='Random seed for reproducible results'
        )
    args = parser.parse_args()

    # Set random seed for reproducibility
    random.seed(args.seed)
    
    # Calculate table sizes based on dataset constraints
    # Audio: min(scaling_factor/3, 650) due to limited audio files
    # Image: scaling_factor (up to 8718 available images)
    max_audio_files = 650
    max_image_files = 8718
    
    audio_size = min(args.scaling_factor // 3, max_audio_files)
    image_size = min(args.scaling_factor, max_image_files)
    
    print(f"Generating tables: Audio={audio_size}, Image={image_size}")
    
    # Validate scaling factor bounds
    if args.scaling_factor > max_image_files:
        print(f"Warning: scaling_factor ({args.scaling_factor}) exceeds available images ({max_image_files})")
        print(f"Using maximum available images: {max_image_files}")
        image_size = max_image_files
    
    audio_table = _generate_audio_table(args.audio_path, audio_size)
    image_table = _generate_image_table(args.image_path, image_size)
    
    
    # Ensure co-occurrence patterns for complex queries
    audio_table, image_table = _ensure_cooccurrence_patterns(audio_table, image_table)
    
    # Ensure Q9 pattern: cities with both monkey images and monkey audio
    audio_table, image_table = _ensure_q9_pattern(audio_table, image_table)
    
    # Ensure Q6 pattern: cities with monkey images but no monkey audio (must come after Q9)
    audio_table, image_table = _ensure_q6_pattern(audio_table, image_table)
    
    # Validation: Print Q6 and Q9 results
    print("\nValidation Results:")
    
    # Q6: Cities with monkey images but no monkey audio
    cities_with_monkey_images = set(image_table[image_table['Species'].str.contains('monkey', case=False, na=False)]['City'].unique())
    cities_with_monkey_audio = set(audio_table[audio_table['Animal'].str.contains('monkey', case=False, na=False)]['City'].unique())
    q6_cities = cities_with_monkey_images - cities_with_monkey_audio
    print(f"Q6 - Cities with monkey images but no audio: {sorted(q6_cities)} (count: {len(q6_cities)})")
    
    # Q9: Cities with both monkey images and audio
    q9_cities = cities_with_monkey_images & cities_with_monkey_audio  
    print(f"Q9 - Cities with both monkey images and audio: {sorted(q9_cities)} (count: {len(q9_cities)})")
    
    folder = Path(__file__).resolve().parents[4] / "files" / "animals" / "data"
    os.makedirs(folder, exist_ok=True)
    audio_table.to_csv(f'{folder}/audio_data.csv', index=False)
    image_table.to_csv(f'{folder}/image_data.csv', index=False)
    
    print(f"\n=== Tables saved to {folder} ===")
    print(f"\n=== Generated Tables Summary ===")
    print(f"audio_data.csv: {len(audio_table)} rows")
    print(f"image_data.csv: {len(image_table)} rows")
    print(f"Maximum table size: {max(len(audio_table), len(image_table))} rows")