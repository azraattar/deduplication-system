import pandas as pd
import numpy as np
from pathlib import Path

def evaluate_deduplication_accuracy(predictions_path, ground_truth_path):
    """Evaluate deduplication accuracy against ground truth"""
    print("\n" + "="*60)
    print("EVALUATION: Accuracy Metrics")
    print("="*60)
    
    # Load predictions
    print(f"\n📂 Loading predictions from: {predictions_path}")
    df_pred = pd.read_csv(predictions_path)
    
    # Load ground truth
    print(f"📂 Loading ground truth from: {ground_truth_path}")
    if ground_truth_path.endswith('.parquet'):
        df_truth = pd.read_parquet(ground_truth_path)
    else:
        df_truth = pd.read_csv(ground_truth_path)
    
    # Build ground truth duplicate pairs
    true_duplicates = set()
    duplicate_map = {}
    
    for _, row in df_truth[df_truth['is_duplicate'] == True].iterrows():
        dup_id = row['record_id']
        orig_id = row['original_id']
        if orig_id:
            pair = tuple(sorted([dup_id, orig_id]))
            true_duplicates.add(pair)
            duplicate_map[dup_id] = orig_id
    
    # Build predicted duplicate pairs
    predicted_duplicates = set()
    for _, row in df_pred.iterrows():
        pair = tuple(sorted([row['record_id_l'], row['record_id_r']]))
        predicted_duplicates.add(pair)
    
    # Calculate metrics
    true_positives = len(true_duplicates & predicted_duplicates)
    false_positives = len(predicted_duplicates - true_duplicates)
    false_negatives = len(true_duplicates - predicted_duplicates)
    
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    print(f"\n📊 Results:")
    print(f"  True duplicate pairs (ground truth): {len(true_duplicates):,}")
    print(f"  Predicted duplicate pairs: {len(predicted_duplicates):,}")
    print(f"  True Positives: {true_positives:,}")
    print(f"  False Positives: {false_positives:,}")
    print(f"  False Negatives: {false_negatives:,}")
    print(f"\n  Precision: {precision:.2%}")
    print(f"  Recall: {recall:.2%}")
    print(f"  F1 Score: {f1_score:.2%}")
    
    metrics = {
        'true_duplicates': len(true_duplicates),
        'predicted_duplicates': len(predicted_duplicates),
        'true_positives': true_positives,
        'false_positives': false_positives,
        'false_negatives': false_negatives,
        'precision': precision,
        'recall': recall,
        'f1_score': f1_score
    }
    
    # Save metrics
    Path('benchmarks').mkdir(exist_ok=True)
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv('benchmarks/accuracy_metrics.csv', index=False)
    print(f"\n✅ Metrics saved to benchmarks/accuracy_metrics.csv")
    
    return metrics

if __name__ == '__main__':
    evaluate_deduplication_accuracy(
        'data/raw/splink_predictions.csv',
        'data/raw/customers_small.parquet'
    )
