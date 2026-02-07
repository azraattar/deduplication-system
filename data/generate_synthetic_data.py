import pandas as pd
import numpy as np
from faker import Faker
import random
from pathlib import Path

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def introduce_typos(text, probability=0.3):
    """Introduce realistic typos in text"""
    if not text or random.random() > probability:
        return text
    
    text = list(text)
    typo_type = random.choice(['swap', 'delete', 'replace'])
    
    if typo_type == 'swap' and len(text) > 1:
        idx = random.randint(0, len(text) - 2)
        text[idx], text[idx + 1] = text[idx + 1], text[idx]
    elif typo_type == 'delete' and len(text) > 1:
        del text[random.randint(0, len(text) - 1)]
    elif typo_type == 'replace' and len(text) > 0:
        idx = random.randint(0, len(text) - 1)
        text[idx] = random.choice('abcdefghijklmnopqrstuvwxyz')
    
    return ''.join(text)

def generate_clean_records(n_records=1000000):
    """Generate clean base customer records"""
    print(f"Generating {n_records} clean records...")
    
    records = []
    for i in range(n_records):
        if i % 100000 == 0:
            print(f"  Progress: {i}/{n_records}")
        
        record = {
            'record_id': f'REC_{i:08d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number()[:15],
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'dob': fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),
            'is_duplicate': False,
            'original_id': None
        }
        records.append(record)
    
    return pd.DataFrame(records)

def create_duplicates(df, duplicate_rate=0.15):
    """Create various types of duplicates"""
    print(f"\nCreating duplicates (rate: {duplicate_rate*100}%)...")
    
    n_duplicates = int(len(df) * duplicate_rate)
    duplicate_records = []
    
    # Select random records to duplicate
    source_indices = np.random.choice(df.index, size=n_duplicates, replace=True)
    
    for idx, source_idx in enumerate(source_indices):
        if idx % 50000 == 0:
            print(f"  Progress: {idx}/{n_duplicates}")
        
        source = df.iloc[source_idx].copy()
        duplicate = source.copy()
        
        # Assign new record_id
        duplicate['record_id'] = f'DUP_{idx:08d}'
        duplicate['is_duplicate'] = True
        duplicate['original_id'] = source['record_id']
        
        # Determine duplicate type
        dup_type = random.choices(
            ['exact', 'typo', 'partial', 'transposed'],
            weights=[0.05, 0.40, 0.35, 0.20]
        )[0]
        
        if dup_type == 'exact':
            # Exact duplicate (5%)
            pass
        
        elif dup_type == 'typo':
            # Typos in name/address (40%)
            if random.random() < 0.6:
                duplicate['first_name'] = introduce_typos(duplicate['first_name'])
            if random.random() < 0.6:
                duplicate['last_name'] = introduce_typos(duplicate['last_name'])
            if random.random() < 0.4:
                duplicate['address'] = introduce_typos(duplicate['address'], 0.2)
        
        elif dup_type == 'partial':
            # Missing fields (35%)
            fields_to_null = random.sample(['email', 'phone', 'address'], k=random.randint(1, 2))
            for field in fields_to_null:
                duplicate[field] = None
        
        elif dup_type == 'transposed':
            # Transposed first/last name (20%)
            duplicate['first_name'], duplicate['last_name'] = duplicate['last_name'], duplicate['first_name']
            if random.random() < 0.5:
                duplicate['email'] = fake.email()
        
        duplicate_records.append(duplicate)
    
    duplicates_df = pd.DataFrame(duplicate_records)
    combined_df = pd.concat([df, duplicates_df], ignore_index=True)
    
    # Shuffle the dataset
    combined_df = combined_df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    print(f"\nDataset created:")
    print(f"  Total records: {len(combined_df)}")
    print(f"  Clean records: {len(df)}")
    print(f"  Duplicate records: {len(duplicates_df)}")
    
    return combined_df

def main():
    # Create data directory
    Path('data/raw').mkdir(parents=True, exist_ok=True)
    
    # Generate datasets of different sizes
    sizes = {
        'small': 10000,
        'medium': 100000,
        'large': 1000000
    }
    
    for size_name, n_records in sizes.items():
        print(f"\n{'='*60}")
        print(f"Generating {size_name} dataset ({n_records} records)")
        print('='*60)
        
        # Generate clean records
        df_clean = generate_clean_records(n_records)
        
        # Add duplicates
        df_final = create_duplicates(df_clean, duplicate_rate=0.15)
        
        # Save to CSV and Parquet
        csv_path = f'data/raw/customers_{size_name}.csv'
        parquet_path = f'data/raw/customers_{size_name}.parquet'
        
        df_final.to_csv(csv_path, index=False)
        df_final.to_parquet(parquet_path, index=False)
        
        print(f"\n✅ Saved to:")
        print(f"  - {csv_path}")
        print(f"  - {parquet_path}")

if __name__ == '__main__':
    main()
