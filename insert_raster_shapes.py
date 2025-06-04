import csv
import configparser
import os
from shapely import wkt
from shapely.geometry import shape
from shapely.validation import explain_validity
from monkdb import client

# Load config
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")

DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_PASSWORD = config['database']['DB_PASSWORD']
DB_SCHEMA = config['database']['DB_SCHEMA']
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE']

tile_dir = config['paths']['tile_dir']
output_filename = config['paths']['output_csv']

# Construct full CSV path: tile_dir/tile_index/output_filename
output_dir = os.path.join(tile_dir, "tile_index")
os.makedirs(output_dir, exist_ok=True)
TILE_INDEX_CSV = os.path.join(output_dir, output_filename)

# Connect to MonkDB
try:
    conn = client.connect(
        f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
        username=DB_USER
    )
    cursor = conn.cursor()
    print("✅ Connected to MonkDB.")
except Exception as e:
    print(f"❌ Database connection error: {e}")
    exit(1)

# Drop existing table
cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{RASTER_TABLE}")
print(f"🧹 Dropped table {DB_SCHEMA}.{RASTER_TABLE} (if existed).")

# Create new table
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{RASTER_TABLE} (
    tile_id TEXT PRIMARY KEY,
    area GEO_SHAPE,
    path TEXT,
    layer TEXT,
    centroid GEO_POINT,
    area_km DOUBLE
) WITH (number_of_replicas = 0);
""")
print(f"🛠️  Created table {DB_SCHEMA}.{RASTER_TABLE}.")

# Read CSV and insert records
with open(TILE_INDEX_CSV, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) != 4:
            continue

        tile_id, polygon_wkt, file_path, layer = row

        try:
            geom = wkt.loads(polygon_wkt)

            if not geom.is_valid:
                reason = explain_validity(geom)
                print(f"❌ Invalid polygon for tile {tile_id}: {reason}")
                continue

            # Get centroid as [lon, lat]
            centroid_coords = list(geom.centroid.coords)[0]
            centroid = [round(centroid_coords[0], 6),
                        round(centroid_coords[1], 6)]

            # Estimate area in square kilometers (approximate for large WGS84 bounds)
            area_km = round(geom.area / 1e6, 3)

            cursor.execute(f"""
                INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
                (tile_id, area, path, layer, centroid, area_km)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                tile_id,
                polygon_wkt,
                file_path,
                layer,
                centroid,
                area_km
            ))

            print(f"✅ Inserted: {tile_id}")

        except Exception as e:
            print(f"⚠️  Failed to insert {tile_id}: {e}")

# Final check
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{RASTER_TABLE}")
print(f"\n📊 Total rows inserted: {cursor.fetchone()[0]}")

cursor.close()
conn.close()
print("🔌 Disconnected from MonkDB.")
