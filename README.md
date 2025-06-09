# 🌍 Raster Tile Geospatial Indexing Demo with MonkDB

> **Author:** MonkDB Engineering  
> **Last Updated:** 2025-06-09

---

## 📦 Dataset Overview

We have used Natural Earth's Cross Blended Hypsometric Tints with Relief, Water, Drainages, and Ocean Bottom dataset (specifically, the [**HYP_HR_SR_OB_DR.zip**](https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/raster/HYP_HR_SR_OB_DR.zip) archive). This high-resolution global raster (.tif) file provides visually rich, elevation-based shading with additional features such as ocean floor bathymetry, river drainages, and land relief. Sourced from Natural Earth Data, this dataset is suitable for large-scale geospatial visualizations, terrain analysis, and vector-based enrichment, making it ideal for geospatial indexing and geoanalytics with MonkDB.

### 🔹 Why This Dataset?

- **High-resolution geospatial coverage** of the Earth’s surface
- Well-suited for raster tile indexing and tiling workflows
- Ideal for testing geospatial database capabilities such as `GEO_SHAPE`, `GEO_POINT`, and spatial functions like `intersects`, `distance`, `area`

---

## 🧰 Toolchain and Architecture

### 🛠 Tools Used

| Tool        | Purpose                                      |
|-------------|----------------------------------------------|
| `GDAL`      | To extract tile metadata and geometries      |
| `Shapely`   | Geometry parsing and validation               |
| `PyProj`    | Accurate area calculation on curved surfaces |
| `MonkDB`    | AI-Native database for geospatial storage    |
| `Dask`      | Parallel processing of large sets |
| `Python`    | Scripting, data transformation, ETL          |

---

## Config File

You need to create a config ini file at the root with the below structure. Replace `tile_dir` paths according to your system's directory layout. Also replace `DB_HOST` value with the IP address of MonkDB.

```text
[paths]
tile_dir = /home/ubuntu/geo/tiled_raster
output_csv = raster_tile_index.csv

[metadata]
layer_name = hypso_relief
export_format = csv

[database]
DB_HOST = xx.xx.xx.xxx
DB_PORT = 4200
DB_USER = testuser
DB_PASSWORD = testpassword
DB_SCHEMA = monkdb
RASTERGEO_POINTS_TABLE = raster_geo_points
RASTER_GEO_SHAPE_TABLE = raster_geo_shapes
```

## 🗂️ GDAL Usage

We used:
```bash
gdalinfo -json /path/to/HYP_HR_SR_OB_DR.tif
```
to extract:
- Bounding box
- Coordinate reference system (CRS)
- Tile name and path
- Layer metadata (e.g., 'hypso_relief')

This metadata is used to compute:
- **Polygon** for the bounding box (`GEO_SHAPE`)
- **Centroid** of the tile (`GEO_POINT`)
- **Area** using `pyproj.Geod` for Earth-accurate results

We then split the tif file into manageable tiles, e.g., 512x512 using gdal's retile utility.

```bash
gdal_retile.py \
  -ps 512 512 \
  -targetDir ./tiled_raster/ \
  -co TILED=YES -co COMPRESS=DEFLATE \
  HYP_HR_SR_OB_DR.tif
```

Each tile was then:
- Georeferenced (GeoTIFF)
- Usable independently
- Mapped cleanly to a bounding box (for ROI querying)

---

## 🐍 Core Python Script

### `index_raster_tiles.py`

- Reads the metadata from pre-tiled `.tif` files using a CSV.
- Computes and adjusts:
  - Bounding polygon (as `GEO_SHAPE`)
  - Centroid (as `GEO_POINT`)
  - Area (via geodesic area in km² using WGS84 ellipsoid)
- Stores all metadata into a `raster_geo_shapes` table in MonkDB.

> Includes safety checks (e.g. bounding limits -180/180, -90/90), duplicate handling, and invalid polygon skips.

---

## 🧮 Geospatial Calculations

| Feature     | Description |
|-------------|-------------|
| **Centroid** | Computed from polygon center using `shapely` |
| **Area (km²)** | Computed using geodesic area on ellipsoid using `pyproj.Geod.geometry_area_perimeter()` |
| **Safe Polygon** | Ensures lat/lon does not exceed [-180, 180] and [-90, 90] |
| **Geohash** | Encodes centroid into geohash regions for grouping |

---

## 📊 Geospatial Queries in MonkDB

The queries are designed for both **data integrity** and **spatial intelligence**:

### 🔎 Core Query Set

| #  | Query Name                     | SQL Snippet                                                                 | Purpose / Use Case                                                                                          |
|----|------------------------------- |-----------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| 1  | Centroids within Bounding Box  | `WHERE within(centroid, 'POLYGON ((100 -10, 120 -10, 120 10, 100 10, 100 -10))'`) | Extract tiles in Southeast Asia for regional spatial analytics.                                             |
| 2  | Tiles with Zero/Near-Zero Area | `WHERE area_km < 0.01`                                                        | Flags corrupt raster tiles or invalid polygons with bad area values.                                        |
| 3  | Top 10 Largest Tiles           | `ORDER BY area_km DESC LIMIT 10`                                              | Identify top-coverage tiles, helpful in understanding dominant geographies.                                 |
| 4  | Layer-based Filtering          | `WHERE layer = 'hypso_relief'`                                                | Isolates thematic content within specific layers (e.g., elevation, water, slope).                          |
| 5  | Centroids within Radius        | `WHERE distance(centroid, [85.0, 20.0]) < 1000000`                           | Fetch tiles within 1000km of a reference point (e.g., disaster/event zone).                                |
| 6  | Geohash Grouping               | `GROUP BY substr(geohash(centroid), 1, 3)`                                    | Spatial binning for regional heatmaps and distributed ingestion partitioning.                               |
| 7  | Southern & Eastern Hemisphere  | `WHERE latitude(centroid) < 0 AND longitude(centroid) > 0`                    | Filters tiles falling in the southeastern quadrant (e.g., Oceania, Indonesia).                             |
| 8  | Total Area Coverage            | `SELECT SUM(area_km) FROM ...`                                                | Measures total geospatial coverage (in km²) across all tiles.                                               |


### Advanced Query Result Set

| #  | Query Name                                  | Purpose                                                                                          | SQL Snippet                                                                                                                                                                                                                      |
|----|---------------------------------------------|--------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | Tiles with Multiple Layer Versions          | Detects duplicate tile_ids across layers (e.g., tile123__hypso, tile123__slope)                  | `SELECT tile_id, COUNT(*) AS layer_versions FROM schema.table GROUP BY tile_id HAVING COUNT(*) > 1;`                                                                                                                            |
| 2  | Compare Area Across Layers for Same Tile ID | Cross-checks area_km across versions of the same tile across different layers                    | `SELECT t.tile_id, t.layer, t.area_km FROM schema.table t JOIN (SELECT tile_id FROM schema.table GROUP BY tile_id HAVING COUNT(*) > 1) d ON t.tile_id = d.tile_id;`                                                             |
| 3  | Tile Count by Layer (Distribution)          | Shows how many tiles exist in each raster layer (e.g., land cover, slope, water bodies)           | `SELECT layer, COUNT(*) AS tile_count FROM schema.table GROUP BY layer ORDER BY tile_count DESC;`                                                                                                                               |
| 4  | Average Tile Area per Layer                 | Highlights granularity differences between low-res and high-res layers                           | `SELECT layer, ROUND(AVG(area_km), 2) AS avg_area_km FROM schema.table GROUP BY layer ORDER BY avg_area_km DESC;`                                                                         |
| 5  | Top 5 Tiles by Area per Layer               | Finds the largest tiles in each layer for QA or tile curation                                   | `SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY layer ORDER BY area_km DESC) AS rank FROM schema.table ) t WHERE rank <= 5;`                                                    |
| 6  | Tiles Missing Area Data (NULL area_km)      | Ensures all tiles have proper geodesic area populated                                           | `SELECT tile_id, layer FROM schema.table WHERE area_km IS NULL;`                                                                                                                          |
| 7  | Resolution Distribution by Layer            | Groups tiles by resolution level (e.g., "high", "low") within each layer                        | `SELECT layer, resolution, COUNT(*) AS count FROM schema.table GROUP BY layer, resolution ORDER BY layer, count DESC;`                                                                     |
| 8  | Average Distance to Reference Point per Layer | Measures how far on average each layer’s tiles are from a focal point (e.g., disaster zone)      | `SELECT layer, ROUND(AVG(distance(centroid, [85.0, 20.0])), 2) AS avg_dist_m FROM schema.table GROUP BY layer ORDER BY avg_dist_m;`                                                        |


> Replace `schema.table` with `{DB_SCHEMA}.{RASTER_TABLE}`.

## Spatial Insights Queries

| #  | Query/Output Name                         | Purpose                                                                                             | SQL Snippet / Method                                                                                                                                                                                                 | Benefits                                                                                         | AI Insight Use Case                                                                                             |
|----|-------------------------------------------|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| 1  | Layer Statistics Summary                  | Gives min, max, mean, stddev, and count of tile areas per layer                                     | `SELECT layer, COUNT(*), MIN(area_km), MAX(area_km), ROUND(AVG(area_km),2), ROUND(STDDEV(area_km),2) FROM schema.table GROUP BY layer;`                                        | Identifies data anomalies, standardizes expected tile sizes                                    | Detect layers with abnormal fragmentation; guide AI models to handle edge-case tiles                           |
| 2  | Layer Percentile Distribution             | Understands spread of tile area across 25th, 50th, 75th, and 95th percentile                        | `SELECT layer, percentile(area_km, 0.25), 0.5, 0.75, 0.95 FROM schema.table GROUP BY layer;`                                                                                    | Helps define thresholds for area-based clustering or pruning                                    | Feed percentile curves into spatial anomaly detection or adaptive tiling AI models                             |
| 3  | WKT-based Intersection Query              | Finds tiles intersecting a target WKT polygon (e.g., state boundary)                                | `SELECT tile_id, layer, area_km, centroid FROM schema.table WHERE intersects(area, ?) ORDER BY area_km DESC LIMIT 100;`                                                         | Quick lookup for all coverage tiles of a region                                                 | Enable AI to infer content availability, perform boundary-aware inference (e.g., floods in Telangana)          |
| 4  | Boundary Union + Bounding Box (Client)    | Computes bounding geometry of entire tile set                                                       | `unary_union` + `.bounds` in Python                                                                                                                                            | Geo-referencing all tiles as one boundary block                                                  | Train AI to operate over the entire coverage zone; use unified WKT as input for global pattern learning        |

---

## Chat-Based Solution

In this solution, we pass SQL commands to MonkDB and leverage the TinyLlama model to perform summarization, extract key insights, and more. The stack utilizes several key tools to enable seamless interaction between natural language, databases, and advanced NLP techniques.

| Tool                  | Description                                                                                           |
|-----------------------|-------------------------------------------------------------------------------------------------------|
| [mcp-monkdb server](https://pypi.org/project/mcp-monkdb/) | AI gateway that bridges MonkDB with large language models, enabling SQL command execution and response handling. |
| TinyLlama             | Lightweight LLM (1.1B parameters) fine-tuned for tasks such as text-to-SQL, summarization, and extraction of insights from database responses. Efficient for environments with limited resources, but can be swapped for larger models for improved output quality[2][4]. |
| Pandas                | Used to structure and format the responses retrieved from MonkDB collections for downstream processing and display. |
| HF transformers pipeline | Provides advanced NLP capabilities and model inference, supporting a wide range of language processing tasks. |

Below is a short demonstration of the workflow:

![Demo](./assets/geo_2.gif)

[!CAUTION]
> MonkDB's MCP server requires Python >=3.13. Run only `SELECT *` statements. Other SQL statements won't work with our MCP as it is not recommended. For more information, please go through this GitHub [page](https://github.com/monkdbofficial/monkdb-mcp).

[!NOTE]
> TinyLlama is a compact model (1.1B parameters) designed for efficiency. For higher quality outputs, consider replacing TinyLlama with larger models such as OpenAI, Anthropic, Grok, Gemini, Llama, or others as needed.

---
## 📁 Output

- Final output CSV contains:
  - `tile_id`
  - `area` (as `GEO_SHAPE`)
  - `centroid` (as `GEO_POINT`)
  - `area_km` (float)
  - `layer`
  - `path`

- Outputs have been captured in the `results/` folder for core, advanced and spatial insight queries. 
- The column names in spatial intelligence query results which were captured in the csv are:
  - `layer_statistics`
    - `layer`, `tile_count`, `min_area`, `max_area`, `mean_area`, `stddev_area`.
  - `layer_percentiles`
    - `layer`, `p25`, `median`, `p75`, `p95`
  - `wkt_intersection_results`
    - `tile_id`, `layer`, `area_km`, `centroid`.
---

## ✅ Results and Observations

- Successfully indexed and inserted 940+ raster tiles across 4 simulated layers
- Enabled multi-layer multiplexing with each `tile_id` replicated across layers (e.g., `tile_id__hypso_relief`, `tile_id__land_cover_simulated`, etc.)
- Introduced resolution tiers (`high`, `medium`, `low`, `very_low`) to simulate realistic downsampling and test hybrid query adaptability
- Geodesic area calculations are accurate using `WGS84 ellipsoid`; earlier WKT-based miscalculations were fully resolved
- Top tiles show uniform size due to identical bounding box logic across layers which is consistent with tile-based raster design
- Only ~5–10 records encountered `NULLs` or invalid geometry, all of which were safely skipped during ingestion
- Queries now reflect layer-specific insights, cross-layer comparisons, and are optimized for sub-second performance even with multiplexed data

---

## 🚀 Real-World Relevance (e.g., Telecom, Smart Infra)

- **Edge-aware tile indexing** for 5G tower planning or multi-resolution coverage heatmaps (e.g., high-res elevation + low-res land use).
- **Disaster zone geofencing** using `within()` + radius search on multi-layered raster datasets (e.g., flood risk + slope map + urban density).
- **Terrain & surface analytics** for smart cities, drone/autonomous routing—powered by layer-aware queries.
- **Resolution multiplexing** enables context-sensitive insights at different zoom levels (e.g., fine-grained slope maps vs coarse vegetation layers).
- **Dynamic data partitioning via geohash** for efficient spatial sharding and parallel ingest, scalable to national and continental datasets.

---

## 📌 Next Steps

- Enable streaming tile insertions via MonkDB’s ingestion APIs
- Layer additional metadata: vegetation, land use, satellite data
- Explore real-time raster transformations and AI model overlays

---
