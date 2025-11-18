import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import INFO, basicConfig, getLogger
from subprocess import check_call, check_output
from tempfile import TemporaryDirectory

import matplotlib.colors as colors

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

values = [0, 1, 100, 500, 1000, 2000]
color_list = ["lightskyblue", "lightgreen", "gold", "orange", "sienna", "white"]
color_text = []
for index in range(len(values)):
    value = values[index]
    rgba = colors.to_rgba(color_list[index])
    color = " ".join([f"{int(number * 255)}" for number in rgba])
    color_text.append(f"{value} {color}")
color_text.append("nv 0 0 0 0")
folder = TemporaryDirectory(delete=False)
color_file = f"{folder.name}/color.txt"
with open(color_file, "w") as file:
    file.write("\n".join(color_text))


def get_dem(
    bounds: tuple[float, float, float, float], id: str, folder_name: str
) -> str:
    logger.info(f"Generating DEM {id}")

    # Processing DEM
    logger.info("Generating DEM")
    dem_info = json.loads(
        check_output(
            f"""gdal vector pipeline \
                ! read /vsicurl/https://storage.googleapis.com/gee-ramiqcom-s4g-bucket/collection_tiles/nasadem_tiles.fgb \
                ! filter --bbox={bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]} \
                ! info -f json --features
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

        # save input
        paths_file = f"{folder_name}/paths.txt"
        with open(paths_file, "w") as file:
            file.write("\n".join(dem_paths))

        # DEM
        dem = f"{folder_name}/dem.tif"
        check_call(
            f"""gdal raster mosaic \
                --bbox={bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]} \
                --of=COG \
                --co="COMPRESS=ZSTD" \
                -i @{paths_file} \
                -o {dem}
            """,
            shell=True,
        )

        return dem
    else:
        raise Exception(f"No DEM {id}")


def create_color_relief(bounds: tuple[float, float, float, float], id: str) -> str:
    logger.info(f"Generating hillshade {id}")

    folder = TemporaryDirectory(delete=False)

    dem = get_dem(bounds, id, folder.name)
    colored = f"{folder.name}/colored.tif"
    check_call(
        f"""gdal raster color-map \
            --of=COG \
            --co="COMPRESS=ZSTD" \
            --color-map={color_file} \
            -i {dem} \
            -o {colored}
        """,
        shell=True,
    )

    # Upload it
    check_call(
        f"gcloud storage cp {colored} gs://gee-ramiqcom-s4g-bucket/basemap/color_relief/NASADEM_Color-Relief_{id}.tif",
        shell=True,
    )

    folder.cleanup()


def main():
    try:
        # check done
        done = check_output(
            "gcloud storage ls gs://gee-ramiqcom-s4g-bucket/basemap/color_relief",
            shell=True,
            text=True,
        ).split("\n")[:-1]
        done = ["_".join(path.split(".tif")[0].split("_")[-2:]) for path in done]
    except Exception:
        done = []

    # run with threadpool
    with ThreadPoolExecutor(8) as executor:
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
                jobs.append(executor.submit(create_color_relief, bbox, name))

        for job in as_completed(jobs):
            try:
                job.result()
            except Exception as e:
                logger.info(f"Error: {e}")


if __name__ == "__main__":
    main()
