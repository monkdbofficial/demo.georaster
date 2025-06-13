import csv
import configparser
import os
from shapely import wkt
from shapely.geometry import mapping
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

sentinel_data_dir = config['sentinel']['sentinel_data_dir']
output_filename = config['paths']['output_csv_v2']
output_dir = os.path.join(sentinel_data_dir, "tile_index")
os.makedirs(output_dir, exist_ok=True)
TILE_INDEX_CSV = os.path.join(output_dir, output_filename)

# Connect to MonkDB
conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()
print("Connected to MonkDB.")

# Drop and recreate table (only 4 columns)
cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{RASTER_TABLE}")
print(f"Dropped table {DB_SCHEMA}.{RASTER_TABLE} (if existed).")

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{RASTER_TABLE} (
    tile_id TEXT,
    area GEO_SHAPE,
    path TEXT,
    layer TEXT
)
CLUSTERED BY (layer) INTO 4 SHARDS
WITH (number_of_replicas = 0);
""")
print(f"Created table {DB_SCHEMA}.{RASTER_TABLE} with 4 columns.")

# Ingest
inserted_count = 0
skipped_count = 0
batch = []
BATCH_SIZE = 500

with open(TILE_INDEX_CSV, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)  # ['tile_id', 'bbox', 'path', 'layer']

    for row in reader:
        if len(row) != 4:
            skipped_count += 1
            continue

        tile_id, polygon_wkt, file_path, layer = row

        try:
            geom = wkt.loads(polygon_wkt)
            if not geom.is_valid:
                skipped_count += 1
                continue

            geojson = mapping(geom)  # Convert to GeoJSON dict

            batch.append((tile_id, geojson, file_path, layer))

            if len(batch) >= BATCH_SIZE:
                cursor.executemany(
                    f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
                    (tile_id, area, path, layer)
                    VALUES (%s, %s, %s, %s)""",
                    batch
                )
                inserted_count += len(batch)
                batch.clear()

        except Exception as e:
            print(f"❌ Error on {tile_id}: {e}")
            skipped_count += 1

# Final insert
if batch:
    cursor.executemany(
        f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
        (tile_id, area, path, layer)
        VALUES (%s, %s, %s, %s)""",
        batch
    )
    inserted_count += len(batch)

# Refresh for visibility
# cursor.execute(f"REFRESH TABLE {DB_SCHEMA}.{RASTER_TABLE}")
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
