"""
Five visualizations for NBA player performance before vs after sunset
Imports data from the analysis module
"""

import matplotlib.pyplot as plt
import numpy as np
from analysis import analyze_player_performance

def create_visualizations():
    """Create 5 simple visualizations from the sunset analysis data."""
    results = analyze_player_performance()
    
    if not results:
        print("No data available for visualization.")
        return
    
    # Create figure with 5 subplots
    fig = plt.figure(figsize=(16, 10))
    
    # ============ VISUALIZATION 1: Top 10 Point Differences ============
    ax1 = plt.subplot(2, 3, 1)
    
    # Get top 10 players by point difference
    sorted_by_pts = sorted(results.items(), 
                          key=lambda x: x[1]['differences']['pts_diff'], 
                          reverse=True)[:10]
    
    names = [data['name'].split()[-1] for _, data in sorted_by_pts]
    pts_diffs = [data['differences']['pts_diff'] for _, data in sorted_by_pts]
    
    colors = ['green' if x > 0 else 'red' for x in pts_diffs]
    ax1.barh(names, pts_diffs, color=colors, alpha=0.7)
    ax1.axvline(x=0, color='black', linestyle='-', linewidth=1)
    ax1.set_xlabel('Point Difference')
    ax1.set_title('Top 10: Better After Sunset?', fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # ============ VISUALIZATION 2: Before vs After Points ============
    ax2 = plt.subplot(2, 3, 2)
    
    before_pts = [data['before_sunset']['avg_pts'] for data in results.values()]
    after_pts = [data['after_sunset']['avg_pts'] for data in results.values()]
    
    ax2.scatter(before_pts, after_pts, alpha=0.6, s=60, c='steelblue')
    
    # Add diagonal line
    max_val = max(max(before_pts), max(after_pts))
    ax2.plot([0, max_val], [0, max_val], 'r--', linewidth=2, alpha=0.5)
    
    ax2.set_xlabel('Points Before Sunset')
    ax2.set_ylabel('Points After Sunset')
    ax2.set_title('Do Players Score More After Sunset?', fontweight='bold')
    ax2.grid(alpha=0.3)
    
    # ============ VISUALIZATION 3: Average Stats Comparison ============
    ax3 = plt.subplot(2, 3, 3)
    
    # Calculate overall averages
    avg_before_pts = np.mean([d['before_sunset']['avg_pts'] for d in results.values()])
    avg_after_pts = np.mean([d['after_sunset']['avg_pts'] for d in results.values()])
    avg_before_reb = np.mean([d['before_sunset']['avg_reb'] for d in results.values()])
    avg_after_reb = np.mean([d['after_sunset']['avg_reb'] for d in results.values()])
    avg_before_ast = np.mean([d['before_sunset']['avg_ast'] for d in results.values()])
    avg_after_ast = np.mean([d['after_sunset']['avg_ast'] for d in results.values()])
    
    categories = ['Points', 'Rebounds', 'Assists']
    before_avgs = [avg_before_pts, avg_before_reb, avg_before_ast]
    after_avgs = [avg_after_pts, avg_after_reb, avg_after_ast]
    
    x = np.arange(len(categories))
    width = 0.35
    
    ax3.bar(x - width/2, before_avgs, width, label='Before Sunset', color='orange', alpha=0.8)
    ax3.bar(x + width/2, after_avgs, width, label='After Sunset', color='darkblue', alpha=0.8)
    
    ax3.set_ylabel('Average Value')
    ax3.set_title('Overall: Before vs After Sunset', fontweight='bold')
    ax3.set_xticks(x)
    ax3.set_xticklabels(categories)
    ax3.legend()
    ax3.grid(axis='y', alpha=0.3)
    
    # ============ VISUALIZATION 4: How Many Players Improve? ============
    ax4 = plt.subplot(2, 3, 4)
    
    all_pts_diffs = [data['differences']['pts_diff'] for data in results.values()]
    
    better_after = sum(1 for diff in all_pts_diffs if diff > 0)
    worse_after = sum(1 for diff in all_pts_diffs if diff < 0)
    no_change = sum(1 for diff in all_pts_diffs if diff == 0)
    
    labels = ['Better After\nSunset', 'Worse After\nSunset', 'No Change']
    sizes = [better_after, worse_after, no_change]
    colors = ['lightgreen', 'lightcoral', 'lightgray']
    
    ax4.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
    ax4.set_title('Players: Better or Worse After Sunset?', fontweight='bold')
    
    # ============ VISUALIZATION 5: Rebounds & Assists Changes ============
    ax5 = plt.subplot(2, 3, 5)
    
    # Get top 8 by rebounds difference
    sorted_by_reb = sorted(results.items(), 
                          key=lambda x: abs(x[1]['differences']['reb_diff']), 
                          reverse=True)[:8]
    
    names_reb = [data['name'].split()[-1] for _, data in sorted_by_reb]
    reb_diffs = [data['differences']['reb_diff'] for _, data in sorted_by_reb]
    ast_diffs = [data['differences']['ast_diff'] for _, data in sorted_by_reb]
    
    x = np.arange(len(names_reb))
    width = 0.35
    
    ax5.bar(x - width/2, reb_diffs, width, label='Rebounds', color='coral', alpha=0.8)
    ax5.bar(x + width/2, ast_diffs, width, label='Assists', color='skyblue', alpha=0.8)
    
    ax5.set_ylabel('Difference (After - Before)')
    ax5.set_title('Rebounds & Assists Changes', fontweight='bold')
    ax5.set_xticks(x)
    ax5.set_xticklabels(names_reb, rotation=45, ha='right')
    ax5.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax5.legend()
    ax5.grid(axis='y', alpha=0.3)
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig('sunset_performance_visualizations.png', dpi=300, bbox_inches='tight')
    print("Visualizations saved as 'sunset_performance_visualizations.png'")
    plt.show()

if __name__ == "__main__":
    create_visualizations()