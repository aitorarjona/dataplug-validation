import pdal
import argparse
import json

INPUT_FILE = "inputfile.las"
OUTPUT_FILE = "outputfile.copc.laz"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("inputfile", help="input file path")
    parser.add_argument("outputfile", help="output file path")
    args = parser.parse_args()

    dem_pipeline_json = {"pipeline": [args.inputfile, {"type": "writers.copc", "filename": args.outputfile}]}

    pipeline = pdal.Pipeline(json.dumps(dem_pipeline_json))
    # pipeline.validate()
    # pipeline.loglevel = 8
    result = pipeline.execute()
