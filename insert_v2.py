import csv
import configparser
import os
from shapely import wkt
from shapely.geometry import Polygon
from shapely.ops import transform as shapely_transform
from pyproj import Geod, Transformer
from monkdb import client

# Load config
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

# Geodetic calculator and CRS transformer
geod = Geod(ellps="WGS84")
transformer = Transformer.from_crs("EPSG:32630", "EPSG:4326", always_xy=True)

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

# Ingest CSV
inserted_count = 0
skipped_count = 0
batch = []
BATCH_SIZE = 500

with open(TILE_INDEX_CSV, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)

    for row in reader:
        if len(row) != 7:
            skipped_count += 1
            continue

        tile_id, utm_tile, timestamp, layer, resolution, bbox, path = row

        try:
            geom_utm = wkt.loads(bbox)
            if not geom_utm.is_valid:
                skipped_count += 1
                continue

            # Transform to WGS84
            geom_wgs84 = shapely_transform(transformer.transform, geom_utm)
            if not geom_wgs84.is_valid:
                skipped_count += 1
                continue

            # Compute centroid and area
            centroid_coords = list(geom_wgs84.centroid.coords)[0]
            centroid = [round(centroid_coords[0], 6),
                        round(centroid_coords[1], 6)]
            area_m2, _ = geod.geometry_area_perimeter(geom_wgs84)
            area_km = round(abs(area_m2) / 1e6, 3)

            batch.append((
                tile_id,
                geom_wgs84.wkt,
                path,
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
            print(f"❌ Error on tile {tile_id}: {e}")
            skipped_count += 1

# Final flush
if batch:
    cursor.executemany(
        f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
            (tile_id, area, path, layer, resolution, centroid, area_km)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
        batch
    )
    inserted_count += len(batch)

# Summary
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{RASTER_TABLE}")
total_rows = cursor.fetchone()[0]

print("\n📊 Summary:")
print(f"✅ Total rows in table: {total_rows}")
print(f"✅ Successful inserts: {inserted_count}")
print(f"⚠️ Skipped or failed inserts: {skipped_count}")

cursor.close()
conn.close()
print("🔌 Disconnected from MonkDB.")
