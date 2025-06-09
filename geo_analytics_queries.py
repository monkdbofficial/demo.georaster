import configparser
import pandas as pd
from monkdb import client
from shapely import wkt
from shapely.geometry import mapping
from shapely.ops import unary_union
import os
import json

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")

# Database config
DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_PASSWORD = config['database']['DB_PASSWORD']
DB_SCHEMA = config['database']['DB_SCHEMA']
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE']

# Path resolution (same as insert script)
tile_dir = config['paths']['tile_dir']
output_filename = config['paths']['output_csv']
tile_index_dir = os.path.join(tile_dir, "tile_index")
TILE_INDEX_CSV = os.path.join(tile_index_dir, output_filename)

# Output folder
output_dir = os.getcwd()
os.makedirs(output_dir, exist_ok=True)

# Connect to MonkDB
conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()

# 1. Layer-wise Statistics
print("🔍 Running layer-wise descriptive stats...")
cursor.execute(f"""
    SELECT
        layer,
        COUNT(*) AS tile_count,
        MIN(area_km) AS min_area,
        MAX(area_km) AS max_area,
        ROUND(AVG(area_km), 2) AS mean_area,
        ROUND(stddev(area_km), 2) AS stddev_area
    FROM {DB_SCHEMA}.{RASTER_TABLE}
    GROUP BY layer
    ORDER BY layer
""")
stats_df = pd.DataFrame(cursor.fetchall())
stats_df.to_csv(os.path.join(output_dir, "layer_statistics.csv"), index=False)
print("✅ Saved: layer_statistics.csv")

# 2. Percentile distribution
print("🔍 Running percentile distribution...")
cursor.execute(f"""
    SELECT
        layer,
        percentile(area_km, 0.25) AS p25,
        percentile(area_km, 0.5) AS median,
        percentile(area_km, 0.75) AS p75,
        percentile(area_km, 0.95) AS p95
    FROM {DB_SCHEMA}.{RASTER_TABLE}
    GROUP BY layer
    ORDER BY layer
""")
percentile_df = pd.DataFrame(cursor.fetchall())
percentile_df.to_csv(os.path.join(
    output_dir, "layer_percentiles.csv"), index=False)
print("✅ Saved: layer_percentiles.csv")

# 3. Tiles Intersecting with a Given WKT (from raster_tile_index.csv)
print("📍 Querying for a sample WKT polygon intersection...")
try:
    index_df = pd.read_csv(TILE_INDEX_CSV)
except FileNotFoundError:
    print(f"❌ Could not find file: {TILE_INDEX_CSV}")
    cursor.close()
    conn.close()
    exit(1)

sample_wkt = index_df["bbox"].iloc[0]

# Convert WKT to GeoJSON for MonkDB
geom = wkt.loads(sample_wkt)
geojson_obj = mapping(geom)  # returns a dict in GeoJSON format

# Pass as JSON string
cursor.execute(f"""
    SELECT tile_id, layer, area_km, centroid
    FROM {DB_SCHEMA}.{RASTER_TABLE}
    WHERE intersects(area, cast(? as geo_shape))
    ORDER BY area_km DESC
    LIMIT 100
""", (json.dumps(geojson_obj),))
wkt_query_df = pd.DataFrame(cursor.fetchall())
wkt_query_df.to_csv(os.path.join(
    output_dir, "wkt_intersection_results.csv"), index=False)
print("✅ Saved: wkt_intersection_results.csv")

# 4. Client-side Boundary Extraction
print("🧩 Computing union and bounding box on client side...")
geoms = [wkt.loads(w) for w in index_df["bbox"]]
union_geom = unary_union(geoms)
bbox = union_geom.bounds  # (minx, miny, maxx, maxy)

with open(os.path.join(output_dir, "boundary_summary.txt"), "w", encoding="utf-8") as f:
    f.write("BOUNDING BOX (minx, miny, maxx, maxy):\n")
    f.write(f"{bbox}\n\n")
    f.write("WKT of unified geometry:\n")
    f.write(union_geom.wkt)
print("✅ Saved: boundary_summary.txt")

# Clean up
cursor.close()
conn.close()
print("🎯 All analytics completed successfully. Outputs saved to working directory.")
