import json
import tempfile
import os

import s3fs
import pdal

BUCKET = "point-cloud-datasets"
INPUT_PREFIX = "laz/CA_YosemiteNP_2019/"
REPLACE = ("laz", "las")

if __name__ == "__main__":
    s3 = s3fs.S3FileSystem()

    keys = s3.glob(f"s3://{BUCKET}/{INPUT_PREFIX}/*")

    for key in keys:
        output_key = key.replace(REPLACE[0], REPLACE[1])

        if s3.exists("s3://" + output_key):
            print(f"Key {output_key} already exists")
            continue

        print(f"Processing {key}")
        tmp_prefix = tempfile.mktemp()
        laz_file = tmp_prefix + ".laz"
        las_file = tmp_prefix + ".las"

        try:
            s3.get_file("s3://" + key, laz_file)

            pipeline_json = {"pipeline": [{"type": "readers.las", "filename": laz_file}, {"type": "writers.las", "filename": las_file}]}
            pipeline = pdal.Pipeline(json.dumps(pipeline_json))
            # # pipeline.validate()
            # # pipeline.loglevel = 8
            result = pipeline.execute()

            s3.put_file(las_file, "s3://" + output_key)
        finally:
            try:
                os.remove(laz_file)
            except FileNotFoundError:
                pass
            try:
                os.remove(las_file)
            except FileNotFoundError:
                pass
        print("Done")
