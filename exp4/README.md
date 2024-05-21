# Preprocessing comparison between Cloud-optimized and Dataplug for LiDAR data

The goal of this experiment is to compare the overhead of format transformation to a cloud-optimized format and pre-processing with Dataplug.

The dataset can be downloaded from [here](https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/CA_YosemiteNP_2019_D19/CA_YosemiteNP_2019/).

The files used can be found in the `keys.txt` file. You have to download the data locally and upload it to your S3 bucket.

The data is in the LAZ format. `laz2las.py` is a convenience script to convert LAZ files to LAS files directly from S3.

Then, the `copc_preprocessing.py` script is used for the conversion to the cloud-optimized format, and the `dataplug_preprocessing.py` script is used for the pre-processing with Dataplug.

Remember that these scripts are intended to be run in a AWS EC2 instance!

Good luck!