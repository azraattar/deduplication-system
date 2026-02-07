import duckdb
import pandas as pd
import time
from pathlib import Path

def optimize_with_duckdb(data_path):
    """Demonstrate DuckDB optimization for blocking"""
    print("\n" + "="*60)
    print("DUCKDB: Optimized Blocking Analysis")
    print("="*60)
    
    print(f"\n📂 Loading data into DuckDB...")
    start_time = time.time()
    
    # Create DuckDB connection
    con = duckdb.connect(database=':memory:')
    
    # Load data directly into DuckDB
    if data_path.endswith('.parquet'):
        con.execute(f"CREATE TABLE customers AS SELECT * FROM read_parquet('{data_path}')")
    else:
        con.execute(f"CREATE TABLE customers AS SELECT * FROM read_csv_auto('{data_path}')")
    
    load_time = time.time() - start_time
    
    # Get record count
    n_records = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    print(f"✅ Loaded {n_records:,} records in {load_time:.2f}s")
    
    # Analyze blocking effectiveness
    print("\n📊 Analyzing blocking rule effectiveness...")
    
    blocking_rules = {
        "Block 1: Last name (3 chars) + City": """
            SELECT 
                substr(last_name, 1, 3) || '_' || city as block_key,
                COUNT(*) as records_in_block
            FROM customers
            GROUP BY block_key
            HAVING COUNT(*) > 1
        """,
        "Block 2: Email domain + Birth year": """
            SELECT 
                substr(email, instr(email, '@')) || '_' || substr(dob, 1, 4) as block_key,
                COUNT(*) as records_in_block
            FROM customers
            GROUP BY block_key
            HAVING COUNT(*) > 1
        """,
        "Block 3: Phone prefix + State": """
            SELECT 
                substr(phone, 1, 3) || '_' || state as block_key,
                COUNT(*) as records_in_block
            FROM customers
            GROUP BY block_key
            HAVING COUNT(*) > 1
        """
    }
    
    blocking_stats = []
    
    for rule_name, query in blocking_rules.items():
        start = time.time()
        result = con.execute(query).fetchdf()
        exec_time = time.time() - start
        
        n_blocks = len(result)
        total_comparisons = (result['records_in_block'] * (result['records_in_block'] - 1) / 2).sum()
        avg_block_size = result['records_in_block'].mean() if len(result) > 0 else 0
        max_block_size = result['records_in_block'].max() if len(result) > 0 else 0
        
        print(f"\n{rule_name}:")
        print(f"  Blocks created: {n_blocks:,}")
        print(f"  Total comparisons: {total_comparisons:,.0f}")
        print(f"  Avg block size: {avg_block_size:.1f}")
        print(f"  Max block size: {max_block_size:.0f}")
        print(f"  Execution time: {exec_time:.3f}s")
        
        blocking_stats.append({
            'rule': rule_name,
            'n_blocks': n_blocks,
            'total_comparisons': int(total_comparisons),
            'avg_block_size': avg_block_size,
            'max_block_size': max_block_size,
            'execution_time': exec_time
        })
    
    # Calculate naive comparison count
    naive_comparisons = n_records * (n_records - 1) / 2
    total_blocked_comparisons = sum([s['total_comparisons'] for s in blocking_stats])
    reduction = (1 - total_blocked_comparisons / naive_comparisons) * 100
    
    print(f"\n{'='*60}")
    print(f"📈 Blocking Efficiency:")
    print(f"  Naive comparisons (no blocking): {naive_comparisons:,.0f}")
    print(f"  Comparisons with blocking: {total_blocked_comparisons:,.0f}")
    print(f"  Reduction: {reduction:.2f}%")
    print(f"{'='*60}")
    
    # Save stats
    Path('benchmarks').mkdir(exist_ok=True)
    stats_df = pd.DataFrame(blocking_stats)
    stats_df.to_csv('benchmarks/blocking_analysis.csv', index=False)
    print(f"\n✅ Blocking analysis saved to benchmarks/blocking_analysis.csv")
    
    con.close()
    
    return blocking_stats

if __name__ == '__main__':
    optimize_with_duckdb('data/raw/customers_medium.parquet')
