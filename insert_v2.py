import csv
import configparser
import os
import random
from shapely import wkt
from shapely.geometry import Polygon
from shapely.affinity import translate
from shapely.ops import transform as shapely_transform
from pyproj import Geod, Transformer
from datetime import datetime, timedelta
from monkdb import client

# --- Config ---
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")

DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_PASSWORD = config['database']['DB_PASSWORD']
DB_SCHEMA = config['database']['DB_SCHEMA']
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE_V2']

tile_dir = config['sentinel']['sentinel_data_dir_v2']
output_filename = config['paths']['output_csv_v3']
output_dir = os.path.join(tile_dir, "tile_index")
TILE_INDEX_CSV = os.path.join(output_dir, output_filename)

# --- Geo & DB Setup ---
geod = Geod(ellps="WGS84")
transformer = Transformer.from_crs("EPSG:32630", "EPSG:4326", always_xy=True)

conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()
print("Connected to MonkDB.")

# --- Drop and Recreate Table ---
cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{RASTER_TABLE}")
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
print(f"Created table {DB_SCHEMA}.{RASTER_TABLE}.")

# --- Insert Function ---


def insert_batch(batch):
    cursor.executemany(
        f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
            (tile_id, area, path, layer, resolution, centroid, area_km)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
        batch
    )


# --- Load Real Tiles ---
real_tiles = []
with open(TILE_INDEX_CSV, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        if len(row) != 7:
            continue
        tile_id, utm_tile, timestamp, layer, resolution, bbox, path = row
        try:
            geom_utm = wkt.loads(bbox)
            if geom_utm.is_valid:
                real_tiles.append({
                    "tile_id": tile_id,
                    "timestamp": timestamp,
                    "layer": layer,
                    "resolution": resolution,
                    "bbox": geom_utm,
                    "path": path
                })
        except Exception:
            continue

if not real_tiles:
    print("No valid tiles found. Aborting.")
    exit()

# --- Generate and Insert Records ---
TOTAL_MIN_ROWS = 10_000
batch = []
inserted_count = 0
skipped_count = 0
BATCH_SIZE = 500
synth_index = 0


def generate_variants(base_tile, num_variants):
    variants = []
    base_ts = datetime.strptime(base_tile["timestamp"], "%Y%m%dT%H%M%S")
    for i in range(num_variants):
        try:
            new_tile_id = f"{base_tile['tile_id']}_synth_{i+1}"
            new_ts = (base_ts + timedelta(days=i)).strftime("%Y%m%dT%H%M%S")
            offset_x = random.uniform(50, 500)
            offset_y = random.uniform(50, 500)
            shifted_geom = translate(
                base_tile["bbox"], xoff=offset_x, yoff=offset_y)
            geom_wgs84 = shapely_transform(transformer.transform, shifted_geom)

            if not geom_wgs84.is_valid:
                continue

            centroid_coords = list(geom_wgs84.centroid.coords)[0]
            centroid = [round(centroid_coords[0], 6),
                        round(centroid_coords[1], 6)]
            area_m2, _ = geod.geometry_area_perimeter(geom_wgs84)
            area_km = round(abs(area_m2) / 1e6, 3)

            variants.append((
                new_tile_id,
                geom_wgs84.wkt,
                base_tile["path"],
                base_tile["layer"],
                base_tile["resolution"],
                centroid,
                area_km
            ))
        except Exception:
            continue
    return variants


print(f"Generating synthetic data to reach at least {TOTAL_MIN_ROWS} rows...")

while inserted_count < TOTAL_MIN_ROWS:
    for base_tile in real_tiles:
        real_id = f"{base_tile['tile_id']}_real"
        geom_wgs84 = shapely_transform(
            transformer.transform, base_tile["bbox"])
        if not geom_wgs84.is_valid:
            continue

        centroid_coords = list(geom_wgs84.centroid.coords)[0]
        centroid = [round(centroid_coords[0], 6), round(centroid_coords[1], 6)]
        area_m2, _ = geod.geometry_area_perimeter(geom_wgs84)
        area_km = round(abs(area_m2) / 1e6, 3)

        batch.append((
            real_id,
            geom_wgs84.wkt,
            base_tile["path"],
            base_tile["layer"],
            base_tile["resolution"],
            centroid,
            area_km
        ))

        # Create ~10 variants per real row (adjustable)
        synth = generate_variants(base_tile, num_variants=10)
        batch.extend(synth)

        if len(batch) >= BATCH_SIZE:
            insert_batch(batch)
            inserted_count += len(batch)
            print(f"Inserted: {inserted_count}")
            batch.clear()

# Final insert
if batch:
    insert_batch(batch)
    inserted_count += len(batch)

# --- Summary ---
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{RASTER_TABLE}")
total_rows = cursor.fetchone()[0]

print("\n📊 Summary:")
print(f"✅ Total rows in table: {total_rows}")
print(f"✅ Successful inserts: {inserted_count}")
print(f"⚠️ Skipped or failed inserts: {skipped_count}")

cursor.close()
conn.close()
print("🔌 Disconnected from MonkDB.")
