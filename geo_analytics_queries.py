import configparser
import pandas as pd
from monkdb import client
from shapely.geometry import shape
from shapely import wkt
from shapely.geometry import Polygon
from shapely.ops import unary_union
import os


def swap_wkt_coords(wkt_str):
    """
    Swap coordinates in a WKT POLYGON from (lat lon) to (lon lat).
    """
    geom = wkt.loads(wkt_str)
    if not isinstance(geom, Polygon):
        raise ValueError("Only POLYGON WKT supported in this helper.")
    # Swap x and y for each point
    new_exterior = [(y, x) for x, y in geom.exterior.coords]
    new_interiors = [
        [(y, x) for x, y in interior.coords]
        for interior in geom.interiors
    ]
    new_geom = Polygon(new_exterior, new_interiors)
    return new_geom.wkt


# Load configuration
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")

# Database config
DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_PASSWORD = config['database']['DB_PASSWORD']
DB_SCHEMA = config['database']['DB_SCHEMA']
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE_V2']

# Path resolution (same as insert script)
tile_dir = config['sentinel']['sentinel_data_dir_v2']
output_filename = config['paths']['output_csv_v3']
tile_index_dir = os.path.join(tile_dir, "tile_index")
TILE_INDEX_CSV = os.path.join(tile_index_dir, output_filename)

# Output folder: use 'results' directory in current working directory
output_dir = os.path.join(os.getcwd(), "results", "v3")
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
print("✅ Saved: results/layer_statistics.csv")

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
print("✅ Saved: results/layer_percentiles.csv")

# 3. Tiles Intersecting with a Given WKT (from DB)
print("📍 Querying for a sample WKT polygon intersection...")
cursor.execute(f"SELECT area FROM {DB_SCHEMA}.{RASTER_TABLE} LIMIT 1")
sample_area_row = cursor.fetchone()
if not sample_area_row:
    print("❌ No geometries found in the database.")
    cursor.close()
    conn.close()
    exit(1)
sample_geom = shape(sample_area_row[0])
sample_wkt = sample_geom.wkt

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
print("✅ Saved: results/wkt_intersection_results.csv")

# 4. Server-side Boundary Extraction (from DB)
print("🧩 Computing union and bounding box from database polygons...")
cursor.execute(f"SELECT area FROM {DB_SCHEMA}.{RASTER_TABLE}")
areas = cursor.fetchall()
geoms = [shape(row[0]) for row in areas]
union_geom = unary_union(geoms)
bbox = union_geom.bounds  # (minx, miny, maxx, maxy)

with open(os.path.join(output_dir, "boundary_summary.txt"), "w", encoding="utf-8") as f:
    f.write("BOUNDING BOX (minx, miny, maxx, maxy):\n")
    f.write(f"{bbox}\n\n")
    f.write("WKT of unified geometry:\n")
    f.write(union_geom.wkt)
print("✅ Saved: results/boundary_summary.txt")

# Clean up
cursor.close()
conn.close()
print("🎯 All analytics completed successfully. Outputs saved to 'results' directory.")
