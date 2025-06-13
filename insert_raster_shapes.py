import csv
import configparser
import os
import random
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
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE_V2']

sentinel_data_dir = config['paths']['sentinel_data_dir']
output_filename = config['paths']['output_csv_v2']
output_dir = os.path.join(sentinel_data_dir, "tile_index")
os.makedirs(output_dir, exist_ok=True)
TILE_INDEX_CSV = os.path.join(output_dir, output_filename)

geod = Geod(ellps="WGS84")

# --- Realistic, contextual simulation for 75 synthetic layers ---
SIMULATED_LAYERS = []
random.seed(42)

contextual_names = [
    "ndvi_diff", "urban_growth", "flood_extent", "water_logged", "soil_moisture_dip",
    "veg_stress", "thermal_variation", "infra_expansion", "mining_impact", "cloud_shadow",
    "canopy_loss", "glacier_melt", "landslide_risk", "fire_burned", "drought_impact",
    "salinity_patch", "crop_health", "siltation_zone", "river_shift", "wetland_drain",
    "temp_anomaly", "hazard_zone", "forest_regrowth", "construction_zone", "groundwater_decline",
    "airstrip_build", "coastal_erosion", "railway_new", "solar_farm", "reservoir_growth",
    "new_road_cut", "landfill_expansion", "pipeline_trace", "airport_buffer", "biodiversity_loss",
    "habitat_corridor", "powergrid_overlay", "barren_land_rise", "permafrost_shift", "climate_hotspot",
    "illegal_encroachment", "deforestation_path", "moisture_retreat", "disease_spread_zone", "hail_impact",
    "canal_construction", "drainage_path", "snowline_change", "thermal_island", "agri_intensification",
    "forest_patch_loss", "quarry_expansion", "wind_turbine_zone", "desertification_front", "heatwave_foci",
    "water_stress_core", "clay_extraction", "sewage_spread", "stormwater_path", "paddy_to_cash_crop",
    "pond_fillup", "irrigation_pattern", "urban_greening", "embankment_zone", "pasture_shrink",
    "marshland_fill", "hydro_construction", "eco_zone_shift", "dryland_push", "coral_bleach_risk",
    "sediment_discharge", "rainfall_zone_shift", "borewell_intensity", "wasteland_conversion", "thermal_plume"
]

for i in range(75):
    name = contextual_names[i] if i < len(contextual_names) else f"layer_{i+1}"

    if i < 25:
        resolution = "high"
        tolerance = random.uniform(0.0001, 0.005)
    elif i < 50:
        resolution = "medium"
        tolerance = random.uniform(0.01, 0.05)
    else:
        resolution = "low"
        tolerance = random.uniform(0.06, 0.15)

    x_offset = round(random.uniform(-0.003, 0.003), 6)
    y_offset = round(random.uniform(-0.003, 0.003), 6)

    SIMULATED_LAYERS.append((name, resolution, tolerance, x_offset, y_offset))


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
    base_tile_id TEXT,
    area GEO_SHAPE,
    original_area GEO_SHAPE,
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
    header = next(reader)
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

            original_wkt = safe_wkt(geom)

            for layer, resolution, tolerance, x_offset, y_offset in SIMULATED_LAYERS:
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
                base_tile_id = original_tile_id

                batch.append((
                    tile_id,
                    base_tile_id,
                    adjusted_wkt,
                    original_wkt,
                    file_path,
                    layer,
                    resolution,
                    centroid,
                    area_km
                ))

                if len(batch) >= BATCH_SIZE:
                    cursor.executemany(
                        f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
                        (tile_id, base_tile_id, area, original_area, path, layer, resolution, centroid, area_km)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        batch
                    )
                    inserted_count += len(batch)
                    batch.clear()

        except Exception as e:
            print(f"❌ Failed on tile {original_tile_id}: {e}")
            skipped_count += 1

# Final insert
if batch:
    cursor.executemany(
        f"""INSERT INTO {DB_SCHEMA}.{RASTER_TABLE}
        (tile_id, base_tile_id, area, original_area, path, layer, resolution, centroid, area_km)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
