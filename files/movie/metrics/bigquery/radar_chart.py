import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, List, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

plt.style.use('default')
plt.rcParams['figure.dpi'] = 300
plt.rcParams['font.size'] = 10

def load_json_data() -> Dict[str, Dict[str, Any]]:
    """Load all JSON files from the current directory and extract model data.
    
    Returns:
        Dict mapping model names to their query results
    """
    current_dir = Path(__file__).parent
    data = {}
    
    json_files = list(current_dir.glob("bigquery_*.json"))
    if not json_files:
        raise FileNotFoundError("No bigquery_*.json files found in current directory")
    
    for json_file in json_files:
        model_name = json_file.stem.replace("bigquery_", "")
        try:
            with open(json_file, 'r') as f:
                data[model_name] = json.load(f)
            print(f"✓ Loaded {len(data[model_name])} queries for model: {model_name}")
        except (json.JSONDecodeError, IOError) as e:
            print(f"✗ Error loading {json_file}: {e}")
    
    return data

def extract_quality_metric(query_data: Dict[str, Any]) -> float:
    """Extract and normalize quality metric from query data.
    
    Args:
        query_data: Dictionary containing query results
        
    Returns:
        Normalized quality score between 0 and 1 (higher = better)
    """
    if 'f1_score' in query_data:
        return min(1.0, max(0.0, query_data['f1_score']))
    elif 'spearman_correlation' in query_data:
        # Convert from [-1, 1] to [0, 1] range
        corr = query_data['spearman_correlation']
        return (corr + 1) / 2
    elif 'relative_error' in query_data:
        # Transform relative error to quality score (lower error = higher quality)
        error = query_data['relative_error']
        # Cap error at 1.0 to prevent negative scores
        return max(0.0, 1.0 - min(1.0, error))
    else:
        print(f"Warning: No quality metric found in query data, using 0.0")
        return 0.0

def calculate_model_metrics(model_data: Dict[str, Any]) -> Dict[str, float]:
    """Calculate average metrics for a model across all successful queries.
    
    Args:
        model_data: Dictionary containing all queries for a model
        
    Returns:
        Dictionary with averaged metrics
    """
    execution_times = []
    quality_scores = []
    costs = []
    successful_queries = 0
    
    for query_id, query_data in model_data.items():
        if query_data.get('status') == 'success':
            execution_times.append(query_data['execution_time'])
            quality_scores.append(extract_quality_metric(query_data))
            costs.append(query_data['money_cost'])
            successful_queries += 1
    
    if successful_queries == 0:
        raise ValueError(f"No successful queries found in model data")
    
    print(f"  - Processed {successful_queries} successful queries")
    
    return {
        'avg_execution_time': np.mean(execution_times),
        'avg_quality': np.mean(quality_scores),
        'avg_cost': np.mean(costs),
        'query_count': successful_queries
    }

def normalize_and_transform_metrics(all_metrics: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
    """Transform metrics so that larger values = better performance.
    
    Args:
        all_metrics: Raw metrics for all models
        
    Returns:
        Tuple of (transformed_metrics, raw_stats) for display
    """
    execution_times = [m['avg_execution_time'] for m in all_metrics.values()]
    costs = [m['avg_cost'] for m in all_metrics.values()]
    qualities = [m['avg_quality'] for m in all_metrics.values()]
    
    # Calculate ranges and statistics
    time_stats = {'min': min(execution_times), 'max': max(execution_times), 'range': max(execution_times) - min(execution_times)}
    cost_stats = {'min': min(costs), 'max': max(costs), 'range': max(costs) - min(costs)}
    quality_stats = {'min': min(qualities), 'max': max(qualities), 'range': max(qualities) - min(qualities)}
    
    transformed = {}
    raw_stats = {}
    
    for model, metrics in all_metrics.items():
        # Transform latency: lower time = better (inverted)
        if time_stats['range'] > 0:
            latency_score = (time_stats['max'] - metrics['avg_execution_time']) / time_stats['range']
        else:
            latency_score = 1.0
        
        # Transform cost: lower cost = better (inverted)
        if cost_stats['range'] > 0:
            cost_score = (cost_stats['max'] - metrics['avg_cost']) / cost_stats['range']
        else:
            cost_score = 1.0
        
        # Quality: already normalized, higher = better
        quality_score = metrics['avg_quality']
        
        transformed[model] = {
            'Latency': latency_score,  # Lower execution time = better (inverted)
            'Quality': quality_score,  # Higher quality = better
            'Cost': cost_score  # Lower cost = better (inverted)
        }
        
        raw_stats[model] = {
            'Latency (s)': f"{metrics['avg_execution_time']:.1f}",
            'Quality Score': f"{metrics['avg_quality']:.3f}",
            'Cost ($)': f"{metrics['avg_cost']:.4f}",
            'Queries': metrics['query_count']
        }
    
    return transformed, raw_stats

def create_radar_chart(transformed_metrics: Dict[str, Dict[str, float]], raw_stats: Dict[str, Dict[str, str]]) -> None:
    """Create and save a professional radar chart.
    
    Args:
        transformed_metrics: Normalized performance metrics
        raw_stats: Raw statistics for display
    """
    metrics = list(next(iter(transformed_metrics.values())).keys())
    models = list(transformed_metrics.keys())
    
    # Create figure with subplots - wider figure for better spacing
    fig = plt.figure(figsize=(22, 14))
    
    # Radar chart subplot - adjust position and size to prevent overlap
    ax1 = plt.subplot2grid((3, 4), (0, 0), rowspan=3, colspan=2, projection='polar')
    
    # Compute angles for radar chart
    N = len(metrics)
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]
    
    # Professional color palette
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    markers = ['o', 's', '^', 'D', 'v']
    
    # Plot each model
    for i, model in enumerate(models):
        values = [transformed_metrics[model][metric] for metric in metrics]
        values += values[:1]
        
        color = colors[i % len(colors)]
        marker = markers[i % len(markers)]
        
        # Plot line and fill
        ax1.plot(angles, values, marker=marker, linewidth=3, 
                label=f'{model}', color=color, markersize=8)
        ax1.fill(angles, values, alpha=0.15, color=color)
    
    # Customize radar chart with better spacing for metric labels
    ax1.set_xticks(angles[:-1])
    ax1.set_xticklabels(metrics, fontsize=14, fontweight='bold')
    ax1.tick_params(axis='x', pad=20)  # Add padding to x-tick labels
    ax1.set_ylim(0, 1.2)  # Extend limit to give more space for labels
    ax1.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax1.set_yticklabels(['0.2', '0.4', '0.6', '0.8', '1.0'], fontsize=12, alpha=0.7)
    ax1.grid(True, alpha=0.3)
    ax1.set_facecolor('#f8f9fa')
    
    # Add extra margin around the radar chart
    ax1.margins(0.1)
    
    # Add title with more padding to avoid legend overlap
    ax1.set_title('BigQuery Model Performance Comparison\n(Larger Area = Better Overall Performance)', 
                  fontsize=16, fontweight='bold', pad=50)
    
    # Legend for radar chart - positioned below the chart to avoid all overlaps
    ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3, fontsize=12, 
               frameon=True, fancybox=True, shadow=True)
    
    # Statistics table subplot - adjust position for better layout
    ax2 = plt.subplot2grid((3, 4), (0, 2), rowspan=2, colspan=2)
    ax2.axis('off')
    
    # Create statistics table
    table_data = []
    headers = ['Model'] + list(next(iter(raw_stats.values())).keys())
    table_data.append(headers)
    
    for model in models:
        row = [model] + list(raw_stats[model].values())
        table_data.append(row)
    
    # Create and style table with better positioning
    table = ax2.table(cellText=table_data[1:], colLabels=table_data[0],
                      cellLoc='center', loc='center', bbox=[0, 0.4, 1, 0.5])
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2)
    
    # Style table
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    for i in range(1, len(table_data)):
        color = colors[(i-1) % len(colors)]
        for j in range(len(headers)):
            table[(i, j)].set_facecolor(color if j == 0 else '#f0f0f0')
            if j == 0:
                table[(i, j)].set_text_props(weight='bold', color='white')
    
    ax2.set_title('Detailed Statistics', fontsize=16, fontweight='bold', pad=30)
    
    # Add mathematical equations in separate subplot to avoid crowding
    ax3 = plt.subplot2grid((3, 4), (2, 2), colspan=2)
    ax3.axis('off')
    
    equations = (
        "Mathematical Formulations:\n\n"
        "Latency Score = (max_time - execution_time) / (max_time - min_time)\n"
        "   → Higher score = faster execution (better performance)\n\n"
        "Quality Score = Average of normalized metrics:\n"
        "   • F1-Score: Direct value [0,1]\n"
        "   • Spearman Correlation: (corr + 1) / 2\n"
        "   • Relative Error: max(0, 1 - error)\n\n"
        "Cost Score = (max_cost - cost) / (max_cost - min_cost)\n"
        "   → Higher score = lower cost (better efficiency)"
    )
    ax3.text(0.0, 1.0, equations, fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round,pad=0.8', facecolor='lightyellow', alpha=0.8),
             fontfamily='monospace', transform=ax3.transAxes)
    
    # Adjust spacing between subplots to prevent overlaps
    plt.tight_layout(pad=3.0)
    plt.subplots_adjust(hspace=0.3, wspace=0.3)
    
    plt.savefig('model_performance_radar.png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    print(f"✓ Radar chart saved as 'model_performance_radar.png'")
    plt.show()

def print_summary(all_metrics: Dict[str, Dict[str, float]], transformed: Dict[str, Dict[str, float]]) -> None:
    """Print a summary of the analysis."""
    print("\n" + "="*60)
    print("📊 BIGQUERY MODEL PERFORMANCE ANALYSIS")
    print("="*60)
    
    print(f"\n📈 Raw Performance Metrics:")
    for model, metrics in all_metrics.items():
        print(f"\n🤖 {model.upper()}:")
        print(f"  ⚡ Average Latency: {metrics['avg_execution_time']:.1f}s")
        print(f"  🎯 Average Quality Score: {metrics['avg_quality']:.3f}")
        print(f"  💰 Average Cost: ${metrics['avg_cost']:.4f}")
        print(f"  📋 Successful Queries: {metrics['query_count']}")
    
    print(f"\n🏆 Normalized Performance Scores (0-1, higher = better):")
    for model, metrics in transformed.items():
        avg_score = np.mean(list(metrics.values()))
        print(f"\n🤖 {model.upper()} (Overall: {avg_score:.3f}):")
        for metric, score in metrics.items():
            emoji = "⚡" if metric == "Latency" else "🎯" if metric == "Quality" else "💰"
            print(f"  {emoji} {metric}: {score:.3f}")
    
    # Find best performer
    overall_scores = {model: np.mean(list(metrics.values())) for model, metrics in transformed.items()}
    best_model = max(overall_scores.keys(), key=lambda k: overall_scores[k])
    print(f"\n🏅 Best Overall Performer: {best_model.upper()} (Score: {overall_scores[best_model]:.3f})")
    print("="*60)

def main() -> None:
    """Main function to generate the radar chart."""
    print("🔍 Loading JSON data...")
    try:
        data = load_json_data()
        print(f"✅ Found {len(data)} models: {list(data.keys())}")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return
    
    print("\n⚙️  Calculating metrics...")
    all_metrics = {}
    for model_name, model_data in data.items():
        print(f"Processing {model_name}...")
        try:
            all_metrics[model_name] = calculate_model_metrics(model_data)
        except Exception as e:
            print(f"❌ Error processing {model_name}: {e}")
            continue
    
    if not all_metrics:
        print("❌ No valid metrics calculated. Exiting.")
        return
    
    print("\n🔄 Transforming metrics...")
    transformed, raw_stats = normalize_and_transform_metrics(all_metrics)
    
    print_summary(all_metrics, transformed)
    
    print("\n📈 Creating radar chart...")
    try:
        create_radar_chart(transformed, raw_stats)
    except Exception as e:
        print(f"❌ Error creating chart: {e}")
        return

if __name__ == "__main__":
    main()