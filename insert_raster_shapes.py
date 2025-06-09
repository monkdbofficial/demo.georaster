import csv
import configparser
import os
from shapely import wkt
from shapely.geometry import Polygon
from pyproj import Geod
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

# Initialize geodetic calculator
geod = Geod(ellps="WGS84")

# --- 75 Layers Generation ---
SIMULATED_LAYERS = []
for i in range(75):
    if i < 25:
        SIMULATED_LAYERS.append((f"layer_{i+1}", "high", 0.0))
    elif i < 50:
        SIMULATED_LAYERS.append((f"layer_{i+1}", "medium", 0.1))
    else:
        SIMULATED_LAYERS.append((f"layer_{i+1}", "low", 0.5))
# ---

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

# Drop and recreate table with resolution
cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{RASTER_TABLE}")
print(f"Dropped table {DB_SCHEMA}.{RASTER_TABLE} (if existed).")

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{RASTER_TABLE} (
    tile_id TEXT PRIMARY KEY,
    area GEO_SHAPE,
    path TEXT,
    layer TEXT,
    resolution TEXT,
    centroid GEO_POINT,
    area_km DOUBLE
) WITH (number_of_replicas = 0);
""")
print(f"Created table {DB_SCHEMA}.{RASTER_TABLE}.")

# Utility
def safe_wkt(polygon: Polygon) -> str:
    coords = list(polygon.exterior.coords)
    adjusted = []
    for lon, lat in coords:
        lon = max(min(lon, 179.999999), -179.999999)
        lat = max(min(lat, 89.999999), -89.999999)
        adjusted.append((lon, lat))
    return f"POLYGON (({', '.join([f'{x[0]} {x[1]}' for x in adjusted])}))"

# Insert records across layers
inserted_count = 0
skipped_count = 0

with open(TILE_INDEX_CSV, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) != 4:
            skipped_count += 1
            continue

        original_tile_id, polygon_wkt, file_path, base_layer = row

        try:
            geom = wkt.loads(polygon_wkt)
            if not geom.is_valid:
                print(f"Invalid polygon for tile {original_tile_id}, skipping.")
                skipped_count += 1
                continue

            for layer, resolution, tolerance in SIMULATED_LAYERS:
                # Simplify if needed
                sim_geom = geom.simplify(tolerance) if tolerance > 0 else geom
                adjusted_wkt = safe_wkt(sim_geom)

                # Compute centroid
                centroid_coords = list(sim_geom.centroid.coords)[0]
                centroid = [round(centroid_coords[0], 6), round(centroid_coords[1], 6)]

                # Geodesic area in km²
                area_m2, _ = geod.geometry_area_perimeter(sim_geom)
                area_km = round(abs(area_m2) / 1e6, 3)

                # Unique tile ID per layer
                tile_id = f"{original_tile_id}__{layer}"

                cursor.execute(f"""
                    INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
                    (tile_id, area, path, layer, resolution, centroid, area_km)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    tile_id,
                    adjusted_wkt,
                    file_path,
                    layer,
                    resolution,
                    centroid,
                    area_km
                ))

                print(f"Inserted: {tile_id} [res={resolution}] (area_km={area_km})")
                inserted_count += 1

        except Exception as e:
            print(f"Failed to insert {original_tile_id}: {e}")
            skipped_count += 1

# Summary
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{RASTER_TABLE}")
total_rows = cursor.fetchone()[0]

print("\nSummary:")
print(f"Total rows in table: {total_rows}")
print(f"Successful inserts: {inserted_count}")
print(f"Skipped or failed inserts: {skipped_count}")

cursor.close()
conn.close()
print("Disconnected from MonkDB.")
