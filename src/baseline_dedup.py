import pandas as pd
import time
import psutil
import os
from pathlib import Path

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def baseline_exact_dedup(df):
    """Naive exact duplicate removal using pandas"""
    print("\n" + "="*60)
    print("BASELINE: Exact Deduplication with Pandas")
    print("="*60)
    
    start_time = time.time()
    start_memory = get_memory_usage()
    
    # Drop exact duplicates (excluding record_id)
    comparison_cols = [col for col in df.columns if col not in ['record_id', 'is_duplicate', 'original_id']]
    
    df_deduped = df.drop_duplicates(subset=comparison_cols, keep='first')
    
    end_time = time.time()
    end_memory = get_memory_usage()
    
    # Calculate metrics
    duplicates_found = len(df) - len(df_deduped)
    execution_time = end_time - start_time
    memory_used = end_memory - start_memory
    
    # Calculate ground truth (how many we should have found)
    actual_duplicates = df['is_duplicate'].sum() if 'is_duplicate' in df.columns else 0
    
    print(f"\n📊 Results:")
    print(f"  Input records: {len(df):,}")
    print(f"  Duplicates found: {duplicates_found:,}")
    print(f"  Actual duplicates: {actual_duplicates:,}")
    print(f"  Detection rate: {(duplicates_found/actual_duplicates*100 if actual_duplicates > 0 else 0):.2f}%")
    print(f"  Execution time: {execution_time:.2f}s")
    print(f"  Memory used: {memory_used:.2f} MB")
    
    return {
        'method': 'Baseline (Exact Match)',
        'input_records': len(df),
        'duplicates_found': duplicates_found,
        'actual_duplicates': actual_duplicates,
        'detection_rate': duplicates_found/actual_duplicates*100 if actual_duplicates > 0 else 0,
        'execution_time': execution_time,
        'memory_mb': memory_used
    }

def baseline_fuzzy_simple(df, threshold=0.8):
    """Simple fuzzy matching (too slow for large datasets)"""
    print("\n" + "="*60)
    print("BASELINE: Simple Fuzzy Matching (Limited)")
    print("="*60)
    print("⚠️  WARNING: This will be VERY slow on large datasets")
    
    # Only run on small subset
    if len(df) > 10000:
        print(f"⚠️  Dataset too large ({len(df)} records). Using sample of 10,000.")
        df = df.sample(n=10000, random_state=42)
    
    start_time = time.time()
    start_memory = get_memory_usage()
    
    from difflib import SequenceMatcher
    
    duplicates = []
    processed = set()
    
    for i in range(len(df)):
        if i in processed:
            continue
        
        if i % 1000 == 0:
            print(f"  Processing: {i}/{len(df)}")
        
        row1 = df.iloc[i]
        name1 = f"{row1['first_name']} {row1['last_name']}".lower()
        
        for j in range(i+1, len(df)):
            if j in processed:
                continue
            
            row2 = df.iloc[j]
            name2 = f"{row2['first_name']} {row2['last_name']}".lower()
            
            # Simple string similarity
            similarity = SequenceMatcher(None, name1, name2).ratio()
            
            if similarity >= threshold:
                duplicates.append((i, j, similarity))
                processed.add(j)
                break
    
    end_time = time.time()
    end_memory = get_memory_usage()
    
    execution_time = end_time - start_time
    memory_used = end_memory - start_memory
    
    print(f"\n📊 Results:")
    print(f"  Input records: {len(df):,}")
    print(f"  Duplicate pairs found: {len(duplicates):,}")
    print(f"  Execution time: {execution_time:.2f}s")
    print(f"  Memory used: {memory_used:.2f} MB")
    print(f"  ⚠️  Projected time for 1M records: ~{execution_time * 100:.0f}s ({execution_time * 100 / 3600:.1f} hours)")
    
    return {
        'method': 'Baseline (Simple Fuzzy)',
        'input_records': len(df),
        'duplicates_found': len(duplicates),
        'execution_time': execution_time,
        'memory_mb': memory_used,
        'note': 'Limited sample only'
    }

def run_baseline_benchmarks(data_path):
    """Run all baseline benchmarks"""
    print(f"\n🔍 Loading data from: {data_path}")
    
    if data_path.endswith('.parquet'):
        df = pd.read_parquet(data_path)
    else:
        df = pd.read_csv(data_path)
    
    print(f"✅ Loaded {len(df):,} records")
    
    results = []
    
    # Test 1: Exact deduplication
    result1 = baseline_exact_dedup(df)
    results.append(result1)
    
    # Test 2: Fuzzy matching (only on small datasets)
    if len(df) <= 100000:
        result2 = baseline_fuzzy_simple(df)
        results.append(result2)
    
    # Save results
    results_df = pd.DataFrame(results)
    Path('benchmarks').mkdir(exist_ok=True)
    results_df.to_csv('benchmarks/baseline_results.csv', index=False)
    print(f"\n✅ Results saved to benchmarks/baseline_results.csv")
    
    return results_df

if __name__ == '__main__':
    # Test on small dataset
    run_baseline_benchmarks('data/raw/customers_small.csv')
