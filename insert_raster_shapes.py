import csv
import configparser
import os
from shapely import wkt
from shapely.geometry import Polygon
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

# Construct full path to CSV
output_dir = os.path.join(tile_dir, "tile_index")
os.makedirs(output_dir, exist_ok=True)
TILE_INDEX_CSV = os.path.join(output_dir, output_filename)


def safe_wkt(polygon: Polygon) -> str:
    """Ensure polygon coordinates are within MonkDB limits (-180 to <180, -90 to <90)."""
    coords = list(polygon.exterior.coords)
    adjusted = []
    for lon, lat in coords:
        lon = max(min(lon, 179.999999), -179.999999)
        lat = max(min(lat, 89.999999), -89.999999)
        adjusted.append((lon, lat))
    return f"POLYGON (({', '.join([f'{x[0]} {x[1]}' for x in adjusted])}))"


# Connect to MonkDB
try:
    conn = client.connect(
        f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
        username=DB_USER
    )
    cursor = conn.cursor()
    print("Connected to MonkDB.")
except Exception as e:
    print(f"Database connection error: {e}")
    exit(1)

# Drop and Create table
cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{RASTER_TABLE}")
print(f"Dropped table {DB_SCHEMA}.{RASTER_TABLE} (if existed).")

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
print(f"Created table {DB_SCHEMA}.{RASTER_TABLE}.")

# Insert records from CSV
inserted_count = 0
skipped_count = 0

with open(TILE_INDEX_CSV, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) != 4:
            continue

        tile_id, polygon_wkt, file_path, layer = row

        try:
            geom = wkt.loads(polygon_wkt)

            if not geom.is_valid:
                print(f"Invalid polygon for tile {tile_id}, skipping.")
                skipped_count += 1
                continue

            adjusted_wkt = safe_wkt(geom)

            centroid_coords = list(geom.centroid.coords)[0]
            centroid = [round(centroid_coords[0], 6),
                        round(centroid_coords[1], 6)]

            area_km = round(geom.area / 1e6, 3)

            cursor.execute(f"""
                INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
                (tile_id, area, path, layer, centroid, area_km)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                tile_id,
                adjusted_wkt,
                file_path,
                layer,
                centroid,
                area_km
            ))

            print(f"Inserted: {tile_id}")
            inserted_count += 1

        except Exception as e:
            if "duplicate key" in str(e).lower():
                print(f"Duplicate tile_id {tile_id}, skipping.")
            else:
                print(f"Failed to insert {tile_id}: {e}")
            skipped_count += 1

# Final reporting
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{RASTER_TABLE}")
row_count = cursor.fetchone()[0]

print("\nSummary:")
print(f"Total rows in table: {row_count}")
print(f"Successful inserts: {inserted_count}")
print(f"Skipped or failed inserts: {skipped_count}")

cursor.close()
conn.close()
print("Disconnected from MonkDB.")
