# Comparison between static partitioning and pre-processing with Dataplug for FASTQGZip data

The aim of this experiment is to compare static partitioning and pre-processing with Dataplug.

We used FASTQGZip samples from here: [https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE157103](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE157103)

To set up the data, you must download files from the provided link avobe and place them in your S3 bucket.

To create larger files, we combined many files from the provided dataset. For instance, you can do like this:

```bash
cat SRR12101200_1.fastq.gz SRR12101201_1.fastq.gz SRR12101202_1.fastq.gz > combined_1.fastq.gz
```

Put the keys in a file named "keys.txt", and execute the index_partitioning.py script to pre-process them with Dataplug.

For static partitioning, we used the script `static_partitioning.sh`, with a for-loop, setting the BUCKET and KEY environment variables.

Remember that these scripts are intended to be run in a AWS EC2 instance!

Good luck!
