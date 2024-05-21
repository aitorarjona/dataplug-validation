import lithops
import hashlib
import boto3
import time
import json
import gzip

from dataplug import CloudObject
from dataplug.formats.genomics.fastq import FASTQGZip, partition_reads_batches

BUCKET = "lithops-genomics"

KEYS_PREFIX = ""


def get_partition_decompressed(key, storage):
    t0 = time.perf_counter()
    data = storage.get_object(bucket=BUCKET, key=key, stream=True)
    t1 = time.perf_counter()
    hash = hashlib.md5(data)
    return t1 - t0


def get_chunk_compressed(key, storage):
    t0 = time.perf_counter()
    data = storage.get_object(bucket=BUCKET, key=key, stream=True)
    data_unzip = gzip.decompress(data)
    t1 = time.perf_counter()
    hash = hashlib.md5(data_unzip)
    return t1 - t0


def get_chunk_dataplug(slice):
    t0 = time.perf_counter()
    data = slice.get()
    t1 = time.perf_counter()
    hash = hashlib.md5(data)
    return t1 - t0


if __name__ == "__main__":
    s = lithops.Storage()
    keys = s.list_keys(bucket=BUCKET, prefix=KEYS_PREFIX)
    fexec = lithops.FunctionExecutor()

    # Static decompressed
    # fexec.map(get_partition_decompressed, keys)

    # Static compressed
    # fexec.map(get_chunk_compressed, keys)

    # Dataplug
    slices = []
    for key in keys:
        co = CloudObject.from_s3(FASTQGZip, "s3://BUCKET/{key}")
        data_slices = co.partition(
            partition_reads_batches,
            num_batches=TODO,  # Change this parameter so that chunks are of approx equal size (e.g. 1 GB file / 4 = 250 MB chunks)
        )
        slices.extend(data_slices)

    fexec.map(get_chunk_dataplug, slices)
    res = fexec.get_result()

    with open(KEYS_PREFIX.replace("/", "-") + "_results.json", "w") as out_file:
        json.dump(out_file, res)
