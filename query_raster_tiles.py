import configparser
import pandas as pd
from monkdb import client
import os
import time

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")

DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_PASSWORD = config['database']['DB_PASSWORD']
DB_SCHEMA = config['database']['DB_SCHEMA']
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE']

# Output file path
output_path = os.path.join(os.getcwd(), "query_results_with_layers.txt")

# Establish MonkDB connection
conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()

# Define queries
queries = {
    "Centroids within bounding box (Lat -10 to 10, Lon 100 to 120)": f"""
        SELECT tile_id, centroid
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE within(centroid, 'POLYGON ((100 -10, 120 -10, 120 10, 100 10, 100 -10))');
    """,

    "Tiles with zero or near-zero area": f"""
        SELECT tile_id, area_km
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE area_km < 0.01;
    """,

    "Top 10 largest tiles by area": f"""
        SELECT tile_id, area_km
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE area_km IS NOT NULL
        GROUP BY area_km, tile_id
        ORDER BY area_km DESC
        LIMIT 10;
    """,

    "Tiles in layer = hypso_relief": f"""
        SELECT tile_id, path
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE layer = 'hypso_relief';
    """,

    "Centroids within 1000km of [85, 20]": f"""
        SELECT tile_id, centroid
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE distance(centroid, [85.0, 20.0]) < 1000000;
    """,

    "Count of tiles per geohash region (precision ~3)": f"""
        SELECT substr(geohash(centroid), 1, 3) AS region, COUNT(*) AS tiles
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY region
        ORDER BY tiles DESC;
    """,

    "Southern & Eastern Hemisphere Centroids": f"""
        SELECT tile_id, centroid
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE latitude(centroid) < 0 AND longitude(centroid) > 0;
    """,

    "Total Area Coverage (km²)": f"""
        SELECT SUM(area_km) AS total_area_covered_km2
        FROM {DB_SCHEMA}.{RASTER_TABLE};
    """
}

# Run and log queries
with open(output_path, "w", encoding="utf-8") as output_file:
    for name, sql in queries.items():
        output_file.write(f"\n\n### {name}\n")
        start = time.perf_counter()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            df = pd.DataFrame(results)
            output_file.write(df.to_string(index=False))
        except Exception as e:
            output_file.write(f"Query failed: {e}\n")
        end = time.perf_counter()
        output_file.write(f"\n⏱️ Query Time: {round(end - start, 3)} sec\n")

cursor.close()
conn.close()
print(f"\n✅ Finished all queries. Results saved to {output_path}")
