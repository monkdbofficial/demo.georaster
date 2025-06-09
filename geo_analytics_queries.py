import configparser
import pandas as pd
from monkdb import client
from shapely import wkt
from shapely.ops import unary_union
import os

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")

DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_PASSWORD = config['database']['DB_PASSWORD']
DB_SCHEMA = config['database']['DB_SCHEMA']
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE']

# Connect to MonkDB
conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()

output_dir = os.getcwd()
os.makedirs(output_dir, exist_ok=True)

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

# Percentile distribution
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

# 2. Tiles Intersecting with a Given WKT (from CSV)
print("📍 Querying for a sample WKT polygon intersection...")
index_df = pd.read_csv("raster_tile_index.csv")
sample_wkt = index_df["bbox"].iloc[0]

cursor.execute(f"""
    SELECT tile_id, layer, area_km, centroid
    FROM {DB_SCHEMA}.{RASTER_TABLE}
    WHERE intersects(area, ?)
    ORDER BY area_km DESC
    LIMIT 100
""", (sample_wkt,))
wkt_query_df = pd.DataFrame(cursor.fetchall())
wkt_query_df.to_csv(os.path.join(
    output_dir, "wkt_intersection_results.csv"), index=False)

# 3. Client-side Boundary Extraction
print("🧩 Computing union and bounding box on client side...")
geoms = [wkt.loads(w) for w in index_df["bbox"]]
union_geom = unary_union(geoms)
bbox = union_geom.bounds  # (minx, miny, maxx, maxy)

with open(os.path.join(output_dir, "boundary_summary.txt"), "w", encoding="utf-8") as f:
    f.write("BOUNDING BOX (minx, miny, maxx, maxy):\n")
    f.write(f"{bbox}\n\n")
    f.write("WKT of unified geometry:\n")
    f.write(union_geom.wkt)

cursor.close()
conn.close()
print("✅ All tasks completed. Output saved to current directory.")
