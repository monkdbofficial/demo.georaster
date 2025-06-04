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

## 🐍 Python Scripts

### 1. `index_raster_tiles.py`

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

### 🔎 Query Set

1. **Centroids within a Bounding Box**
   ```sql
   WHERE within(centroid, 'POLYGON ((100 -10, 120 -10, 120 10, 100 10, 100 -10))')
   ```
   ⟶ Extracts tiles in Southeast Asia for regional analytics.

2. **Tiles with Zero or Near-Zero Area**
   ```sql
   WHERE area_km < 0.01
   ```
   ⟶ Flags bad polygons or corrupt rasters.

3. **Top 10 Largest Tiles**
   ```sql
   ORDER BY area_km DESC LIMIT 10
   ```
   ⟶ Identifies dominant geographies by coverage.

4. **Layer-based Filtering**
   ```sql
   WHERE layer = 'hypso_relief'
   ```
   ⟶ Segments tiles by thematic content (useful in multi-layer datasets).

5. **Centroids within Radius**
   ```sql
   WHERE distance(centroid, [85.0, 20.0]) < 1000000
   ```
   ⟶ Fetches tiles within 1000km from a target (e.g., disaster zone analytics).

6. **Geohash Grouping**
   ```sql
   GROUP BY substr(geohash(centroid), 1, 3)
   ```
   ⟶ Clusters tiles spatially for heatmaps or load partitioning.

7. **Southern & Eastern Hemisphere**
   ```sql
   WHERE latitude(centroid) < 0 AND longitude(centroid) > 0
   ```
   ⟶ Filters specific hemispheric zones (e.g., Oceania).

8. **Total Area Coverage**
   ```sql
   SELECT SUM(area_km) FROM ...
   ```
   ⟶ Computes the global surface area covered by tiles.

---

## 📁 Output

- Final output CSV contains:
  - `tile_id`
  - `area` (as `GEO_SHAPE`)
  - `centroid` (as `GEO_POINT`)
  - `area_km` (float)
  - `layer`
  - `path`

- Saved query results to `query_results.txt` for external processing or audit.

---

## ✅ Results and Observations

- Successfully indexed and inserted **940+ raster tiles**
- Geodesic area computations are **accurate**, overcoming earlier WKT-based errors
- Top tiles had uniform size because of equal bounding boxes (common in tiling systems)
- Only ~5–10 tiles had NULL or invalid data

---

## 🚀 Real-World Relevance (e.g., Telecom, Smart Infra)

- **Edge-aware tile indexing** for 5G tower planning or coverage heatmaps
- **Disaster zone geofencing** using `within()` + radius search
- **Terrain analytics** in smart cities and autonomous routing
- **Dynamic data partitioning** using geohash for scalable ingestion

---

## 📌 Next Steps

- Enable streaming tile insertions via MonkDB’s ingestion APIs
- Layer additional metadata: vegetation, land use, satellite data
- Explore real-time raster transformations and AI model overlays

---
