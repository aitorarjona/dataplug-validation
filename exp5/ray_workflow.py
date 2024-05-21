import time
import math
import io
import os
import tempfile
import json
import itertools
import random
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint

import s3fs
import boto3

from dataplug import CloudObject

import numpy as np
import pdal
import laspy
import rasterio as rio
from rasterio.merge import merge

import ray


BUCKET = "point-cloud-datasets"
NUM_CHUNKS = 9

ray.init(address="ray://172.31.7.96:10001")
# ray.init()


@ray.remote(memory=1769)
def partition_las(tile_key, num_chunks):
    print(tile_key, num_chunks)
    print(f">>> partition_las - start - {time.time()} - {tile_key}")

    side_splits = math.sqrt(num_chunks)
    assert int(side_splits + 0.5) ** 2 == num_chunks  # check if perfect sqrt
    side_splits = int(side_splits)

    input_file = tempfile.mktemp() + ".laz"
    partitions = [tempfile.mktemp() + f"part{i}.laz" for i in range(num_chunks)]

    try:
        s3 = boto3.client("s3")
        s3.download_file(Bucket=BUCKET, Key=tile_key.split("/", 1)[1], Filename=input_file)

        with laspy.open(input_file) as file:
            x_max = file.header.x_max
            x_min = file.header.x_min
            y_max = file.header.y_max
            y_min = file.header.y_min

            x_size = (x_max - x_min) / side_splits
            y_size = (y_max - y_min) / side_splits

            bounds = []
            for i in range(side_splits):
                for j in range(side_splits):
                    x_min_bound = (x_size * i) + x_min
                    y_min_bound = (y_size * j) + y_min
                    x_max_bound = x_min_bound + x_size
                    y_max_bound = y_min_bound + y_size
                    bounds.append((x_min_bound, y_min_bound, x_max_bound, y_max_bound))

            writers = [
                laspy.open(partition, mode="w", header=file.header, do_compress=True) for partition in partitions
            ]

            try:
                count = 0
                for points in file.chunk_iterator(1_000_000):
                    x, y = points.x.copy(), points.y.copy()
                    points_piped = 0

                    for i, (x_min, y_min, x_max, y_max) in enumerate(bounds):
                        mask = (x >= x_min) & (x <= x_max) & (y >= y_min) & (y <= y_max)

                        if np.any(mask):
                            sub_points = points[mask]
                            writers[i].write_points(sub_points)

                        points_piped += np.sum(mask)
                        if points_piped == len(points):
                            break
                    count += len(points)
            finally:
                for writer in writers:
                    if writer is not None:
                        writer.close()

            partition_refs = []
            for partition in partitions:
                with open(partition, "rb") as partition_file:
                    partition_data = partition_file.read()
                ref = ray.put(partition_data)
                partition_refs.append(ref)

            print(f">>> partition_las - end - {time.time()} - {tile_key}")
            return tile_key, partition_refs
    finally:
        try:
            os.remove(input_file)
        except:
            pass
        for out in partitions:
            try:
                os.remove(out)
            except:
                pass


def create_dem(tile_key, las_filename, dem_filename):
    print(f">>> create_dem - start - {time.time()} - {tile_key}")

    dem_pipeline_json = {
        "pipeline": [
            {
                "type": "readers.las",
                "filename": las_filename,
                # 'spatialreference': 'EPSG:25830'
            },
            # {
            #     'type': 'filters.reprojection',
            #     'in_srs': 'EPSG:25830',
            #     'out_srs': 'EPSG:25830'
            # },
            {"type": "filters.assign", "assignment": "Classification[:]=0"},
            {"type": "filters.elm"},
            {"type": "filters.outlier", "method": "radius", "radius": 1.0, "min_k": 4},
            {
                "type": "filters.assign",
                "value": ["ReturnNumber = 1 WHERE ReturnNumber < 1", "NumberOfReturns = 1 WHERE NumberOfReturns < 1"],
            },
            {
                "type": "filters.smrf",
                "ignore": "Classification[7:7]",
                "slope": 0.2,
                "window": 16,
                "threshold": 0.45,
                "scalar": 1.2,
            },
            {
                "type": "filters.range",
                # Classification equals 2 (corresponding to ground in LAS).
                "limits": "Classification[2:2]",
            },
            {
                "type": "writers.gdal",
                "gdaldriver": "GTiff",
                "nodata": "-9999",
                "output_type": "max",
                "resolution": 1,
                "filename": dem_filename,
            },
        ]
    }

    pipeline = pdal.Pipeline(json.dumps(dem_pipeline_json))
    # pipeline.validate()
    # pipeline.loglevel = 8
    # print(f'Executing DEM pipeline for {file_path}...')
    result = pipeline.execute()
    # print(f'DEM result wrote {result} bytes')

    print(f">>> create_dem - end - {time.time()} - {tile_key}")


@ray.remote(memory=1769)
def merge_dem_partitions(tile_key, partition_refs):
    print(f">>> merge_dem_partitions - start - {time.time()} - {tile_key}")

    partition_filenames = [tempfile.mkdtemp() + ".gtiff" for _ in range(len(partition_refs))]
    tmp_file_prefix = tempfile.mktemp()
    file_dem_merged = tmp_file_prefix + "_dem-merged.gtiff"

    try:
        for partition_filename, partition in zip(partition_filenames, partition_refs):
            partition_data = ray.get(partition)
            with open(partition_filename, "wb") as partition_file:
                partition_file.write(partition_data)

        dems = [rio.open(f) for f in partition_filenames]
        mosaic_dem, output_dem = merge(dems)

        dem_output_meta = dems[0].meta.copy()
        dem_output_meta.update(
            {
                "driver": "GTiff",
                "blockxsize": 256,
                "blockysize": 256,
                "tiled": True,
                "height": mosaic_dem.shape[1],
                "width": mosaic_dem.shape[2],
                "transform": output_dem,
            }
        )

        with rio.open(file_dem_merged, "w", **dem_output_meta) as m:
            m.write(mosaic_dem)

        try:
            os.remove(file_dem_merged)
        except Exception:
            pass

        print(f">>> merge_dem_partitions - end - {time.time()} - {tile_key}")
        return tile_key
    finally:
        for partition_filename in partition_filenames:
            try:
                os.remove(partition_filename)
            except:
                pass


@ray.remote(memory=1769)
def create_dem_from_file(tile_key, partition_data):
    part_id = random.randint(0, 100000000)

    print(f">>> create_dem_from_file - start - {time.time()} - {tile_key}/{part_id}")

    tmp_prefix = tempfile.mktemp()
    laz_filename = tmp_prefix + ".laz"
    dem_filename = tmp_prefix + "_dem.gtiff"

    try:
        with open(laz_filename, "wb") as input_file:
            input_file.write(partition_data)

        print(f">>> partition_ready - x - {time.time()} - {tile_key}/{part_id}")

        create_dem(tile_key, laz_filename, dem_filename)

        with open(dem_filename, "rb") as result_file:
            result = result_file.read()

        result_ref = ray.put(result)

        print(f">>> create_dem_from_file - end - {time.time()} - {tile_key}/{part_id}")
        return tile_key, result_ref
    finally:
        try:
            os.remove(laz_filename)
        except FileNotFoundError:
            pass
        try:
            os.remove(dem_filename)
        except FileNotFoundError:
            pass


@ray.remote(memory=1769)
def create_dem_from_slice(tile_key, slice, suffix):
    part_id = random.randint(0, 100000000)

    print(f">>> create_dem_from_slice - start - {time.time()} - {tile_key}/{part_id}")
    print(slice)

    tmp_prefix = tempfile.mktemp()
    laz_filename = tmp_prefix + f".{suffix}"
    dem_filename = tmp_prefix + "_dem.gtiff"

    print(f">>> get_slice - start - {time.time()} - {tile_key}/{part_id}")
    slice.to_file(laz_filename)
    print(f">>> get_slice - end - {time.time()} - {tile_key}/{part_id}")

    print(f">>> partition_ready - x - {time.time()} - {tile_key}/{part_id}")

    try:
        create_dem(tile_key, laz_filename, dem_filename)

        with open(dem_filename, "rb") as result_file:
            result = result_file.read()

        result_ref = ray.put(result)

        # dask_client = get_client()
        # result_future = dask_client.scatter(result)

        print(f">>> create_dem_from_slice - end - {time.time()} - {tile_key}/{part_id}")
        return tile_key, result_ref
    finally:
        try:
            os.remove(laz_filename)
        except FileNotFoundError:
            pass
        try:
            os.remove(dem_filename)
        except FileNotFoundError:
            pass


def run_static_partitioning_workflow():
    s3 = s3fs.S3FileSystem()

    paths = s3.glob(f"s3://{BUCKET}/laz/CA_YosemiteNP_2019/*.laz")
    # paths = s3.glob('s3://geospatial/laz/*')
    # paths = paths[:len(paths) // 4]
    # paths = [paths[0]]
    print(len(paths))
    # print(paths)

    print(f">>> pipeline - start - {time.time()} - x")

    partition_tasks = []
    for path in paths:
        task = partition_las.remote(path, NUM_CHUNKS)
        partition_tasks.append(task)

    partitions = ray.get(partition_tasks)

    partitions_flat = []
    for key, parts in partitions:
        for p in parts:
            partitions_flat.append((key, p))

    dem_tasks = []
    for key, partition in partitions_flat:
        task = create_dem_from_file.remote(key, partition)
        dem_tasks.append(task)
    dems = ray.get(dem_tasks)

    grouped_dems = []
    for key, group in itertools.groupby(dems, lambda part: part[0]):
        partitions = [p[1] for p in group]
        grouped_dems.append((key, partitions))
    # print(grouped_dems)

    merge_tasks = []
    for key, dems in grouped_dems:
        task = merge_dem_partitions.remote(key, dems)
        merge_tasks.append(task)

    ray.get(merge_tasks)
    print(f">>> pipeline - end - {time.time()} - x")


def run_dataplug_workflow():
    from dataplug.storage import PickleableS3ClientProxy
    from dataplug.types.geospatial.copc import LiDARPointCloud, square_split_strategy

    # from dataplug.types.geospatial.laspc import LiDARPointCloud, square_split_strategy

    s3_config = {
        "aws_access_key_id": None,
        "aws_secret_access_key": None,
        "region_name": "us-east-1",
        "endpoint_url": None,
        "use_token": False,
    }

    s3 = s3fs.S3FileSystem()

    paths = s3.glob(f"s3://{BUCKET}/copc/CA_YosemiteNP_2019/*.laz")
    # paths = s3.glob(f"s3://{BUCKET}/las/CA_YosemiteNP_2019/*.las")
    # paths = s3.glob('s3://geospatial/copc/*')
    # paths = [paths[0]]
    print(len(paths))
    # print(paths)

    s3_client = PickleableS3ClientProxy(**s3_config)

    def _create_slice(key):
        co = CloudObject.from_s3(LiDARPointCloud, f"s3://{key}", s3_client=s3_client)
        s = co.partition(square_split_strategy, num_chunks=NUM_CHUNKS)
        return key, s

    print(f">>> pipeline - start - {time.time()} - x")
    with ThreadPoolExecutor(max_workers=4) as pool:
        fs = pool.map(_create_slice, paths)
        group_slices = [f for f in fs]

    print(len(group_slices))

    # slices = []
    # for path in paths:
    #     co = CloudObject.from_s3(LiDARPointCloud, f"s3://{path}", s3_config=s3_config)
    #     s = co.partition(square_split_strategy, num_chunks=NUM_CHUNKS)
    #     slices.append(s)
    # print(slices)

    dem_tasks = []
    for key, slices in group_slices:
        for slice_ in slices:
            # task = create_dem_from_slice.remote(key, slice_, "las")
            task = create_dem_from_slice.remote(key, slice_, "laz")
            dem_tasks.append(task)
    dems = ray.get(dem_tasks)

    grouped_dems = []
    for key, group in itertools.groupby(dems, lambda part: part[0]):
        partitions = [p[1] for p in group]
        grouped_dems.append((key, partitions))

    merge_tasks = []
    for key, dems in grouped_dems:
        task = merge_dem_partitions.remote(key, dems)
        merge_tasks.append(task)

    ray.get(merge_tasks)


if __name__ == "__main__":
    run_static_partitioning_workflow()
    # run_dataplug_workflow()
    # debug()
