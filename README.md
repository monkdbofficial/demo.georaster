# 🌍 Raster Tile Geospatial Indexing Demo with MonkDB

> **Author:** MonkDB Engineering  
> **Last Updated:** 2025-06-13

---

## 📦 Dataset Overview

We constructed a large-scale synthetic geospatial tile index using real metadata extracted from the **Sentinel-2 MSI L2A** product specifically from a representative granule with tile ID **30UVA** and orbit details like:

| Field                      | Value                                                                                                 |
|----------------------------|-------------------------------------------------------------------------------------------------------|
| Absolute orbit number      | 4011                                                                                                  |
| Beginning date time        | 2025-06-12T11:21:31.025000Z                                                                           |
| Cloud cover                | 97.414082                                                                                             |
| Datastrip id               | S2C_OPER_MSI_L2A_DS_2CPS_20250612T150504_S20250612T112401_N05.11                                      |
| Ending date time           | 2025-06-12T11:21:31.025000Z                                                                           |
| Granule identifier         | S2C_OPER_MSI_L2A_TL_2CPS_20250612T150504_A004011_T30UVA_N05.11                                        |
| Modification date          | 2025-06-12T16:09:26.704058Z                                                                           |
| Operational mode           | INS-NOBS                                                                                              |
| Origin                     | ESA                                                                                                   |
| Origin date                | 2025-06-12T15:57:26.000000Z                                                                           |
| Processing date            | 2025-06-12T15:05:04.000000Z                                                                           |
| Processing level           | S2MSI2A                                                                                               |
| Processor version          | 05.11                                                                                                 |
| Product group id           | GS2C_20250612T112131_004011_N05.11                                                                    |
| Product type               | S2MSI2A                                                                                               |
| Publication date           | 2025-06-12T16:09:26.704058Z                                                                           |
| Relative orbit number      | 37                                                                                                    |
| S3Path                     | /eodata/Sentinel-2/MSI/L2A/2025/06/12/S2C_MSIL2A_20250612T112131_N0511_R037_T30UVA_20250612T150504.SAFE|
| Source product             | S2C_OPER_MSI_L2A_TL_2CPS_20250612T150504_A004011_T30UVA_N05.11, S2C_OPER_MSI_L2A_DS_2CPS_20250612T150504_S20250612T112401_N05.11 |
| Source product origin date | 2025-06-12T15:57:26Z, 2025-06-12T15:55:59Z                                                            |
| Tile id                    | 30UVA                                                                                                 |

This seed tile was accompanied by associated raster .tif paths and footprint polygons.

---

### 🔁 Synthetic Amplification

Since the original dataset contained only *36 tile footprints*, we amplified it to `100,000` entries for scalability and stress testing purposes. The process involved:

- Spatial translations and perturbations of original WKT polygons.
- Systematic duplication across varying `layer`, `resolution`, and `path` values.
- Generation of realistic metadata fields such as `centroid`, `area_km`, and derived `geohash3`

Each row in the resulting dataset simulates a unique raster tile footprint suitable for spatial querying, aggregation, and indexing.

### 🔹 Why This Dataset?

- Real-world provenance (based on actual Sentinel-2 tile metadata)
- Highly scalable: 100K records mimic high-throughput satellite ingestion pipelines
- Rich geospatial structure:
  - `area`: WKT `GEO_SHAPE` (polygon)
  - `centroid`: derived `GEO_POINT`
  - `geohash3`: spatial region hashing for clustering and diversity checks
- Used for:
  - intersects, within, distance, and area benchmarks
  - Advanced queries like percentile distributions, bounding box coverage, resolution-layer analysis, and geohash diversity
  - Evaluating MonkDB's performance for Earth observation-style workloads

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
| `Python3`    | Scripting, data transformation, ETL          |

---

## Config File

You need to create a config ini file at the root with the below structure. Replace `tile_dir` paths according to your system's directory layout. Also replace `DB_HOST` value with the IP address of MonkDB.

```text
[paths]
output_csv_v3 = sentinel_v3_tile_index.csv   # this is the file name (where the indexed tiles would be written to)

[sentinel]
sentinel_data_dir_v2 = /home/ubuntu/v3_geo   # this is where the derived *.tiff are stored.

[metadata]
export_format = csv

[database]
DB_HOST = xx.xx.xxx.xxx
DB_PORT = 4200
DB_USER = testuser
DB_PASSWORD = testpassword
DB_SCHEMA = monkdb
RASTER_GEO_SHAPE_TABLE_V2 = sentinel
```

## 🗂️ GDAL Usage

The data from [Sentinel Hub](https://browser.dataspace.copernicus.eu) is open-source and typically provided as a `.SAFE.zip` archive.

Once extracted, the archive contains a structured directory hierarchy. To locate the imagery files, navigate to:

```txt
GRANULE/L<...>{TILE_ID}{TIMESTAMP}/IMG_DATA
```

Inside the `IMG_DATA` folder, you will find multiple `.jp2` (JPEG2000) files representing different spectral bands and resolutions.

### Convert `.jp2` to `.tif`

To convert `.jp2` files to `.tif` format, use the provided [`to_tiff.sh`](./to_tiff.sh) script:

#### Step 1: Make the script executable

```bash
chmod +x to_tiff.sh
```

#### Step 2: Run the conversion script

```bash
./to_tiff.sh <SOURCE_IMG_DATA_FOLDER> <DESTINATION_FOLDER_FOR_TIFFS>
```

This will recursively process all `.jp2` files in the source folder and save the converted `.tif` files in the specified destination directory.

### Validating TIF files

`tif` files need to be validated to ensure the presence of spatial reference system (CRS). To validate run the below command. 

We used:
```bash
gdalinfo -json /path/to/file_name.tif
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

> **MonkDB requires all `GEO_SHAPE` column entries to be spatially referenced.** 

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

- Migrated to **Sentinel-2 Level-2A imagery**, using metadata-rich `.SAFE` archives from [Copernicus Open Access Hub](https://browser.dataspace.copernicus.eu)
- Generated **100,000+ synthetic raster tile entries** from an original sample of 36 real Sentinel tiles, ensuring varied spatial coverage and realistic duplication for benchmarking
- Resolution values (e.g., `10m`, `20m`, `60m`) were **extracted directly from the dataset structure**, ensuring fidelity to Sentinel band characteristics
- **Bounding boxes and centroids** were computed accurately using `WGS84` ellipsoid geometry for each tile polygon, and all values were normalized to `GEO_SHAPE` and `GEO_POINT`
- Calculated `area_km` per tile with high precision; **uniformity across layers** reflects consistent tiling and reprojection logic
- Introduced **geohash3 regions** to enable proximity and clustering analysis in downstream geospatial queries
- All ingestion steps included **validation and fault tolerance** — only 5–10 entries had invalid or null geometries and were gracefully skipped
- End-to-end queries such as percentile distribution, bounding box unions, region diversity, and intersection tests were executed on **MonkDB with sub-second response times**
- Results show MonkDB’s **spatial indexing, layer-wise analytics, and vector-tile handling are production-ready**, even under synthetic scale and multiplexed conditions

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
- Explore real-time raster transformations and AI model overlays

---
