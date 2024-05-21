# Geospatial workflow with an autoscaling Ray cluster

The goal of this experiment is to compare in-cluster partitioning with on-the-fly partitioning in a geospatial workflow running on an autoscaling Ray cluster in Amazon EC2.

The cluster definition can be found in `ray_cluster.yaml`. Follow the documentation from Ray to set up the cluster on Amazon EC2.

The `ray.Dockerfile` is a Dockerfile to build a Ray image with the necessary dependencies for the worker.

The dataset can be downloaded from [here](https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/CA_YosemiteNP_2019_D19/CA_YosemiteNP_2019/).

The files used can be found in the `keys.txt` file. You have to download the data locally and upload it to your S3 bucket.

The data is in the LAZ format. `laz2las.py` from the **exp4** folder is a convenience script to convert LAZ files to LAS files directly from S3.

Good luck!