import os
import pandas as pd
import matplotlib.pyplot as plt

# Constants
RESULTS_DIR = os.path.join(os.getcwd(), "results")
STATS_PATH = os.path.join(RESULTS_DIR, "layer_statistics.csv")
WKT_PATH = os.path.join(RESULTS_DIR, "wkt_intersection_results.csv")

expected_stats_cols = ['layer', 'tile_count',
                       'min_area', 'max_area', 'mean_area', 'stddev_area']
expected_wkt_cols = ['tile_id', 'layer', 'area_km', 'centroid']

# Function to safely read CSV with fallback to default headers


def safe_read_csv(path, expected_cols):
    try:
        df = pd.read_csv(path)
        if not all(col in df.columns for col in expected_cols):
            df = pd.read_csv(path, header=None)
            if list(df.columns) == list(range(len(expected_cols))):
                df.columns = expected_cols
    except Exception as e:
        print(f"❌ Failed to read {path}: {e}")
        df = pd.DataFrame(columns=expected_cols)
    return df


# Load CSVs
layer_stats = safe_read_csv(STATS_PATH, expected_stats_cols)
wkt_tiles = safe_read_csv(WKT_PATH, expected_wkt_cols)

print(f"✅ layer_statistics.csv columns: {list(layer_stats.columns)}")
print(f"✅ wkt_intersection_results.csv columns: {list(wkt_tiles.columns)}")

# Plot 1: Mean Area per Layer
plt.figure(figsize=(12, 6))
ax = plt.gca()
bar = ax.bar(layer_stats['layer'], layer_stats['mean_area'], color='steelblue')
ax.set_xlabel("Layer")
ax.set_ylabel("Mean Area (km²)")
ax.set_title("Mean Area per Layer")
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(os.path.join(RESULTS_DIR, "mean_area_per_layer.png"))
print("📊 Saved: mean_area_per_layer.png")

# Plot 2: Top Intersected Tiles by Area
wkt_tiles_sorted = wkt_tiles.sort_values(
    by="area_km", ascending=False).head(20)
plt.figure(figsize=(10, 8))
ax = plt.gca()
bar = ax.barh(wkt_tiles_sorted["tile_id"],
              wkt_tiles_sorted["area_km"], color="darkorange")
ax.set_xlabel("Area (km²)")
ax.set_ylabel("Tile ID")
ax.set_title("Top 20 Intersected Tiles by Area")
plt.tight_layout()
plt.savefig(os.path.join(RESULTS_DIR, "top_intersected_tiles.png"))
print("📊 Saved: top_intersected_tiles.png")
