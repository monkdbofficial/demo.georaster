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

output_path = os.path.join(os.getcwd(), "advanced_query_results.txt")

# Connect to MonkDB
conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()

# Optimized queries for large datasets
queries = {
    "Tiles with multiple layer versions (duplicate tile_id)": f"""
        SELECT tile_id, COUNT(*) AS layer_versions
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY tile_id
        HAVING COUNT(*) > 1
        ORDER BY layer_versions DESC
        LIMIT 100
    """,
    "Compare area_km across different layers for same tile_id": f"""
        SELECT t.tile_id, t.layer, t.area_km
        FROM {DB_SCHEMA}.{RASTER_TABLE} t
        WHERE t.tile_id IN (
            SELECT tile_id
            FROM {DB_SCHEMA}.{RASTER_TABLE}
            GROUP BY tile_id
            HAVING COUNT(*) > 1
        )
        ORDER BY t.tile_id, t.layer
        LIMIT 500
    """,
    "Tiles per layer distribution": f"""
        SELECT layer, COUNT(*) AS tile_count
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY layer
        ORDER BY tile_count DESC
    """,
    "Average area_km per layer": f"""
        SELECT layer, ROUND(AVG(area_km), 2) AS avg_area_km
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY layer
        ORDER BY avg_area_km DESC
    """,
    "Top 5 tiles by area in each layer": f"""
        SELECT layer, tile_id, area_km
        FROM (
            SELECT layer, tile_id, area_km,
                   ROW_NUMBER() OVER (PARTITION BY layer ORDER BY area_km DESC) AS rnk
            FROM {DB_SCHEMA}.{RASTER_TABLE}
        ) ranked
        WHERE rnk <= 5
        ORDER BY layer, rnk
    """,
    "Resolution-wise tile count per layer": f"""
        SELECT layer, resolution, COUNT(*) AS count
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY layer, resolution
        ORDER BY layer, resolution
    """,
    "Geohash region diversity per layer (precision ~3)": f"""
        SELECT layer, COUNT(DISTINCT substr(geohash(centroid), 1, 3)) AS region_diversity
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY layer
        ORDER BY region_diversity DESC
    """,
    "Tiles near [85, 20] with area > 1000 km2": f"""
        SELECT tile_id, layer, area_km, distance(centroid, [85.0, 20.0]) AS dist_m
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE area_km > 1000
        ORDER BY dist_m ASC
        LIMIT 10
    """
}

with open(output_path, "w", encoding="utf-8") as output_file:
    for name, sql in queries.items():
        output_file.write(f"\n\n### {name}\n")
        start = time.perf_counter()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            if results:
                df = pd.DataFrame(results)
                output_file.write(df.to_string(index=False))
                output_file.write(f"\nRows returned: {len(df)}\n")
            else:
                output_file.write("No results.\n")
            print(f"✅ Completed: {name}")
        except Exception as e:
            output_file.write(f"Query failed: {e}\n")
            print(f"❌ Failed: {name} — {e}")
        end = time.perf_counter()
        output_file.write(f"\n⏱️ Query Time: {round(end - start, 3)} sec\n")

cursor.close()
conn.close()
print(f"\n🎯 All advanced queries completed. Results saved to {output_path}")
