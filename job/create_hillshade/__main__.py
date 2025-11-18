import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import INFO, basicConfig, getLogger
from subprocess import check_call, check_output
from tempfile import TemporaryDirectory

# Configuration
MAX_WORKERS = os.cpu_count() or 1  # Thread workers

basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = getLogger(__name__)

bboxes = []
for x in range(-180, 180, 10):
    min_x = x
    max_x = min_x + 10
    for y in range(-90, 90, 10):
        min_y = y
        max_y = min_y + 10
        bboxes.append(
            dict(
                id=f"{x:03d}X_{y:03d}Y",
                min_x=min_x,
                min_y=min_y,
                max_x=max_x,
                max_y=max_y,
            )
        )


def get_dem(bounds: tuple[float, float, float, float], id: str) -> str:
    logger.info(f"Generating DEM {id}")

    # Processing DEM
    logger.info("Generating DEM")
    dem_info = json.loads(
        check_output(
            f"""ogrinfo \
                -spat {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} \
                -json \
                -features \
                /vsicurl/https://storage.googleapis.com/gee-ramiqcom-s4g-bucket/collection_tiles/nasadem_tiles.fgb
            """,
            shell=True,
            text=True,
        )
    )
    dem_tiles = dem_info["layers"][0]["features"]

    if len(dem_tiles) > 0:
        # Create list of dem paths
        dem_paths = [
            f""" "/vsicurl/https://storage.googleapis.com/gee-ramiqcom-s4g-bucket/nasadem/{tile["properties"]["id"]}.tif" """
            for tile in dem_tiles
        ]

        folder = TemporaryDirectory(delete=False)

        # Mosaic it
        dem = f"{folder.name}/dem.tif"
        check_call(
            f"""gdalwarp \
                -t_srs EPSG:4326 \
                -co COMPRESS=ZSTD \
                -te {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} \
                {" ".join(dem_paths)} \
                {dem}
            """,
            shell=True,
        )

        return dem
    else:
        raise Exception(f"No DEM {id}")


def create_hillshade(bounds: tuple[float, float, float, float], id: str) -> str:
    logger.info(f"Generating hillshade {id}")

    folder = TemporaryDirectory(delete=False)

    dem = get_dem(bounds, id)
    hillshade = f"{folder.name}/hillshade.tif"
    check_call(
        f"""gdaldem hillshade \
            -z 10 \
            -s 111120 \
            -multidirectional \
            {dem} \
            {hillshade}
        """,
        shell=True,
    )

    # Upload it
    check_call(
        f"gcloud storage cp {hillshade} gs://gee-ramiqcom-s4g-bucket/basemap/hillshade/NASADEM_Hillshade_{id}.tif",
        shell=True,
    )

    return hillshade


def main():
    # run with threadpool
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        jobs = []
        for dict_bounds in bboxes:
            name = dict_bounds["id"]
            bbox = (
                dict_bounds["min_x"],
                dict_bounds["min_y"],
                dict_bounds["max_x"],
                dict_bounds["max_y"],
            )
            jobs.append(executor.submit(create_hillshade, bbox, name))

        for job in as_completed(jobs):
            try:
                job.result()
            except Exception as e:
                logger.info(f"Error: {e}")


if __name__ == "__main__":
    main()
