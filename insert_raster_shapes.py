import csv
import configparser
import os
from shapely import wkt
from shapely.geometry import Polygon
from shapely.affinity import translate
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
output_dir = os.path.join(tile_dir, "tile_index")
os.makedirs(output_dir, exist_ok=True)
TILE_INDEX_CSV = os.path.join(output_dir, output_filename)

geod = Geod(ellps="WGS84")

# Simulate 75 layers with resolution, tolerance, and translation offset
SIMULATED_LAYERS = []
for i in range(75):
    if i < 25:
        SIMULATED_LAYERS.append(
            (f"layer_{i+1}", "high", 0.0, i * 0.001, i * 0.001))
    elif i < 50:
        SIMULATED_LAYERS.append(
            (f"layer_{i+1}", "medium", 0.05, i * 0.002, i * 0.002))
    else:
        SIMULATED_LAYERS.append(
            (f"layer_{i+1}", "low", 0.1, i * 0.003, i * 0.003))


def safe_wkt(polygon: Polygon) -> str:
    coords = list(polygon.exterior.coords)
    adjusted = []
    for lon, lat in coords:
        lon = max(min(lon, 179.999999), -179.999999)
        lat = max(min(lat, 89.999999), -89.999999)
        adjusted.append((lon, lat))
    return f"POLYGON (({', '.join([f'{x[0]} {x[1]}' for x in adjusted])}))"


# Connect to MonkDB
conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()
print("Connected to MonkDB.")

# Drop and recreate table
cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{RASTER_TABLE}")
print(f"Dropped table {DB_SCHEMA}.{RASTER_TABLE} (if existed).")

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{RASTER_TABLE} (
    tile_id TEXT,
    area GEO_SHAPE,
    path TEXT,
    layer TEXT,
    resolution TEXT,
    centroid GEO_POINT,
    area_km DOUBLE,
    geohash3 TEXT GENERATED ALWAYS AS substr(geohash(centroid), 1, 3)
)
CLUSTERED BY (layer) INTO 12 SHARDS
WITH (number_of_replicas = 0);
""")
print(f"Created table {DB_SCHEMA}.{RASTER_TABLE} clustered by layer.")

# Ingest
inserted_count = 0
skipped_count = 0
batch = []
BATCH_SIZE = 500

with open(TILE_INDEX_CSV, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)  # skip header
    for row in reader:
        if len(row) != 4:
            skipped_count += 1
            continue

        original_tile_id, polygon_wkt, file_path, base_layer = row

        try:
            geom = wkt.loads(polygon_wkt)
            if not geom.is_valid:
                skipped_count += 1
                continue

            for layer, resolution, tolerance, x_offset, y_offset in SIMULATED_LAYERS:
                # Add variation: simplify + shift
                sim_geom = geom.simplify(tolerance) if tolerance > 0 else geom
                translated_geom = translate(
                    sim_geom, xoff=x_offset, yoff=y_offset)

                adjusted_wkt = safe_wkt(translated_geom)
                centroid_coords = list(translated_geom.centroid.coords)[0]
                centroid = [round(centroid_coords[0], 6),
                            round(centroid_coords[1], 6)]
                area_m2, _ = geod.geometry_area_perimeter(translated_geom)
                area_km = round(abs(area_m2) / 1e6, 3)
                tile_id = f"{original_tile_id}__{layer}"

                batch.append((
                    tile_id,
                    adjusted_wkt,
                    file_path,
                    layer,
                    resolution,
                    centroid,
                    area_km
                ))

                if len(batch) >= BATCH_SIZE:
                    cursor.executemany(
                        f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
                        (tile_id, area, path, layer, resolution, centroid, area_km)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        batch
                    )
                    inserted_count += len(batch)
                    batch.clear()

        except Exception as e:
            print(f"❌ Failed on tile {original_tile_id}: {e}")
            skipped_count += 1

# Final batch insert
if batch:
    cursor.executemany(
        f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
        (tile_id, area, path, layer, resolution, centroid, area_km)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        batch
    )
    inserted_count += len(batch)

cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{RASTER_TABLE}")
total_rows = cursor.fetchone()[0]

# Summary
print("\n📊 Summary:")
print(f"✅ Total rows in table: {total_rows}")
print(f"✅ Successful inserts: {inserted_count}")
print(f"⚠️ Skipped or failed inserts: {skipped_count}")

cursor.close()
conn.close()
print("🔌 Disconnected from MonkDB.")
