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


def get_dem(
    bounds: tuple[float, float, float, float], id: str, folder_name: str
) -> str:
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
            f"/vsicurl/https://storage.googleapis.com/gee-ramiqcom-s4g-bucket/nasadem/{tile['properties']['id']}.tif"
            for tile in dem_tiles
        ]

        folder = TemporaryDirectory(delete=False)

        # save input
        paths_file = f"{folder_name}/paths.txt"
        with open(paths_file, "w") as file:
            file.write("\n".join(dem_paths))

        # vrt
        vrt = f"{folder_name}/vrt.vrt"
        check_call(f"gdalbuildvrt input_file_list {paths_file} {vrt}", shell=True)

        # Mosaic it
        dem = f"{folder_name}/dem.tif"
        check_call(
            f"""gdalwarp \
                -t_srs EPSG:4326 \
                -co COMPRESS=ZSTD \
                -te {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} \
                {vrt} \
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

    cog = f"{folder.name}/hillshade_cog.tif"
    check_call(
        f"""gdalwarp \
            -of COG \
            -co COMPRESS=ZSTD \
            {hillshade} \
            {cog}
        """,
        shell=True,
    )

    # Upload it
    check_call(
        f"gcloud storage cp {cog} gs://gee-ramiqcom-s4g-bucket/basemap/hillshade/NASADEM_Hillshade_{id}.tif",
        shell=True,
    )

    folder.cleanup()


def main():
    try:
        # check done
        done = check_output(
            "gcloud storage ls gs://gee-ramiqcom-s4g-bucket/basemap/hillshade",
            shell=True,
            text=True,
        ).split("\n")[:-1]
        done = ["_".join(path.split(".tif")[0].split("_")[-2:]) for path in done]
    except Exception:
        done = []

    # run with threadpool
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        jobs = []
        for dict_bounds in bboxes:
            name = dict_bounds["id"]
            if name not in done:
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
