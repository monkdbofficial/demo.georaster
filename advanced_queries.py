import configparser
import pandas as pd
from monkdb import client
import os
import time
import re

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")

DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_PASSWORD = config['database']['DB_PASSWORD']
DB_SCHEMA = config['database']['DB_SCHEMA']
RASTER_TABLE = config['database']['RASTER_GEO_SHAPE_TABLE']

results_dir = os.path.join(os.getcwd(), "results")
os.makedirs(results_dir, exist_ok=True)
summary_path = os.path.join(results_dir, "advanced_query_summary.txt")

conn = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = conn.cursor()


def safe_filename(title: str) -> str:
    return re.sub(r'\W+', '_', title.lower()).strip('_') + ".csv"


# Optimized queries
queries = {
    "Tiles with multiple layer versions (duplicate tile_id)": f"""
        SELECT tile_id, COUNT(DISTINCT layer) AS layer_versions
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY tile_id
        HAVING COUNT(DISTINCT layer) > 1
        ORDER BY layer_versions DESC
        LIMIT 100
    """,

    # "Compare area_km across different layers for same tile_id": f"""
    #     SELECT tile_id, layer, area_km
    #     FROM {DB_SCHEMA}.{RASTER_TABLE}
    #     WHERE tile_id IN (
    #         SELECT tile_id
    #         FROM {DB_SCHEMA}.{RASTER_TABLE}
    #         GROUP BY tile_id
    #         HAVING COUNT(DISTINCT layer) > 1
    #     )
    #     ORDER BY tile_id, layer
    #     LIMIT 500
    # """,

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
        SELECT layer, resolution, COUNT(*) AS tile_count
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY layer, resolution
        ORDER BY layer, resolution
    """,

    "Geohash region diversity per layer (precision ~3)": f"""
        SELECT layer, COUNT(DISTINCT geohash3) AS region_diversity
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        GROUP BY layer
        ORDER BY region_diversity DESC
    """,

    "Tiles near [85, 20] with area > 1000 km2": f"""
        SELECT tile_id, layer, area_km, distance(centroid, [85.0, 20.0]) AS dist_m
        FROM {DB_SCHEMA}.{RASTER_TABLE}
        WHERE area_km > 1000
          AND geohash3 IN ('t1d', 't1e', 't1f', 't1g', 't1h')  -- Example geohash3 prefixes for region
        ORDER BY dist_m ASC
        LIMIT 10
    """
}

with open(summary_path, "w", encoding="utf-8") as summary:
    for name, sql in queries.items():
        summary.write(f"\n\n### {name}\n")
        start = time.perf_counter()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            duration = round(time.perf_counter() - start, 3)

            if rows:
                df = pd.DataFrame(rows)
                file_name = safe_filename(name)
                csv_path = os.path.join(results_dir, file_name)
                df.to_csv(csv_path, index=False)
                summary.write(f"✅ Query succeeded. Rows: {len(df)}\n")
                summary.write(f"⏱️ Duration: {duration} sec\n")
                summary.write(f"📁 Saved to: {csv_path}\n")
            else:
                summary.write("⚠️ No results returned.\n")
                summary.write(f"⏱️ Duration: {duration} sec\n")

            print(f"✅ Completed: {name}")

        except Exception as e:
            duration = round(time.perf_counter() - start, 3)
            summary.write(f"❌ Query failed: {e}\n")
            summary.write(f"⏱️ Duration: {duration} sec\n")
            print(f"❌ Failed: {name} — {e}")

cursor.close()
conn.close()
print(f"\n🎯 All queries completed. Results saved to: {results_dir}")
