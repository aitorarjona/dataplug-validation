# Chunk retreival latency comparison between static chunks and dynamic partitioning for FASTQGZip data

For this experiment, we used [Lithops](https://github.com/lithops-cloud/lithops) to deploy serverless functions to retrieve chunks of FASTQ data.

We used three approaches:
1. Static uncompressed partitions: For this, you can use the dataset from **exp1**, uncompress the data, chunk it into the desired size (e.g. 256 MB) and upload it to S3.
2. Static compressed partitions: The same as above, but the chunk is compressed before being uploaded to S3.
3. Dynamic compressed partitions: The data is decompressed on-the-fly using Dataplug. Data must be pre-processed first using dataplug.

The three aproaches are implemented in `lithops_fastqgz_proc.py` script.

Good luck!
