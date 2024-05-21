from dataplug import CloudObject
from dataplug.formats.genomics.fastq import FASTQGZip, partition_reads_batches
from dataplug.util import setup_logging

import logging
import time
from datetime import datetime

BUCKET = ""


def create_index(key):
    setup_logging(logging.DEBUG)

    co = CloudObject.from_s3(FASTQGZip, "s3://{BUCKET}/{key}", s3_config={"use_token": False})

    t0 = time.perf_counter()
    co.preprocess()
    t1 = time.perf_counter()

    t = t1 - t0
    print(f"Total exec time: {t}")
    return t


if __name__ == "__main__":
    with open("keys.txt", "r") as f:
        keys = f.readlines()

    with open("indexing_times.txt", "a") as f:
        for key in keys:
            key = key.strip()
            t = create_index(key)
            f.write(f"{key}\t{t}\n")
