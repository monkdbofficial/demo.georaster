# 🌍 Raster Tile Geospatial Indexing Demo with MonkDB

> **Author:** MonkDB Engineering  
> **Last Updated:** 2025-06-04

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

---

## 📁 Output

- Final output CSV contains:
  - `tile_id`
  - `area` (as `GEO_SHAPE`)
  - `centroid` (as `GEO_POINT`)
  - `area_km` (float)
  - `layer`
  - `path`

- Saved low hanging fruit kind of query results to this [file](./query_results.txt).
- Saved medium to advanced query value results to this [file](./advanced_query_results.txt). This also has the time it has taken to execute complex queries. 

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
